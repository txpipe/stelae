//! Stelae-owned policy for publishing the Dolos Cardano profile.

use std::path::{Path, PathBuf};

use dolos_core::config::{RootConfig, StorageConfig};
use dolos_snapshot::{
    export::{Plan, BLOCKS_REPOSITORY, DOLOS_REPOSITORY},
    facade::SnapshotSource,
    node,
    planning::PlanReport,
    prepare::Finalizing,
    registry::{self, Auth, Point, Preview, Published, Publishing, Registry, Repository, Tuning},
    restore::profile_repository,
    DolosProfile,
};
use stelae::{progress::Observer, Digest, SnapshotReader as _};
use stelae_blocks::BlocksProfile;
use stelae_driver::Standing;

use crate::{
    backfill,
    batch::{self, Committed, Held},
    snapshot,
};

/// Resolved repository publication settings. `repo` is the network root,
/// which holds the Dolos repository and the blocks repository beside it.
pub struct RepositoryPublish<'a> {
    pub repo: &'a Repository,
    pub auth: Auth,
    pub insecure: bool,
    pub scratch_dir: Option<&'a Path>,
    pub tuning: Tuning,
}

/// The Dolos repository under the network root `root`.
pub fn dolos_repository(root: &Repository) -> Result<Repository, dolos_snapshot::Error> {
    profile_repository(root, DOLOS_REPOSITORY)
}

/// The blocks repository under the network root `root`.
pub fn blocks_repository(root: &Repository) -> Result<Repository, dolos_snapshot::Error> {
    profile_repository(root, BLOCKS_REPOSITORY)
}

/// The policy decision made from the repository's current standing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    First,
    After { latest: u64 },
    Nothing(String),
}

impl Next {
    pub fn read(standing: Standing, sequence: u64, require_new: bool) -> Result<Self, PolicyError> {
        match standing {
            Standing::Empty => Ok(Self::First),
            Standing::Next { latest } => Ok(Self::After { latest }),
            Standing::UpToDate { latest } => {
                let message = format!(
                    "nothing to publish: this repository is at sequence {latest} and this node is at sequence {sequence}"
                );
                if require_new {
                    Err(PolicyError::NothingToPublish(message))
                } else {
                    Ok(Self::Nothing(message))
                }
            }
            Standing::Ahead { latest, distance } => Err(PolicyError::PublishWouldGap {
                latest,
                sequence,
                distance,
            }),
        }
    }
}

/// Publisher-host refusals that do not belong to the Dolos profile facade.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("{0}")]
    NothingToPublish(String),

    #[error(
        "this repository's latest stele is sequence {latest} and this node is at sequence \
         {sequence}, {distance} sequences ahead: a publish must follow the repository's latest \
         stele, and this one would leave a gap no later stele could close"
    )]
    PublishWouldGap {
        latest: u64,
        sequence: u64,
        distance: u64,
    },

    #[error(
        "the blocks repository {repository} holds no blocks snapshot at epoch-{sequence}: a Dolos \
         snapshot names the blocks snapshot of its own sequence, so that one is published first"
    )]
    BlocksAbsent { repository: String, sequence: u64 },
}

/// An opened Dolos repository plus the journal of this publisher.
///
/// It reads one sequence at a time, as a publisher reads its predecessor: its
/// manifests and inscription, never the chain behind them.
pub struct Publisher {
    registry: Registry,
    record_path: PathBuf,
}

impl Publisher {
    pub fn open(
        config: &RootConfig,
        publish: &RepositoryPublish<'_>,
    ) -> Result<Self, dolos_snapshot::Error> {
        let scratch = node::scratch_dir(&config.storage, publish.scratch_dir);
        Self::open_explicit(
            &dolos_repository(publish.repo)?,
            publish.insecure,
            publish.auth.clone(),
            scratch,
            registry::record_path_in(&config.storage.path),
            publish.tuning.clone(),
        )
    }

    pub fn open_explicit(
        repository: &Repository,
        insecure: bool,
        auth: Auth,
        scratch_dir: PathBuf,
        record_path: PathBuf,
        tuning: Tuning,
    ) -> Result<Self, dolos_snapshot::Error> {
        let registry = registry::open(repository, insecure, auth, scratch_dir, tuning)?;

        Ok(Self {
            registry,
            record_path,
        })
    }

    pub fn standing(&self, plan: &Plan) -> Result<Standing, dolos_snapshot::Error> {
        registry::standing(&self.registry, plan)
    }

    /// What the repository really holds, as a stele that follows it would name
    /// it.
    pub fn latest(&self, plan: &Plan) -> Result<Option<Committed>, dolos_snapshot::Error> {
        if self.standing(plan)? == Standing::Empty {
            return Ok(None);
        }
        Ok(Some(held(&Point::Latest.pull(&self.registry)?)?.committed))
    }

    /// The stele under `sequence`'s immutable tag, if the repository holds one.
    ///
    /// A seal tags a sequence's tip before the sequence itself, so a sequence
    /// held here has its tip too, and one tagged only at its tip is absent:
    /// sealing it again writes the same tip.
    pub fn stele(&self, sequence: u64) -> Result<Option<Held>, dolos_snapshot::Error> {
        match Point::Epoch(sequence).pull(&self.registry) {
            Ok(snapshot) => Ok(Some(held(&snapshot)?)),
            Err(dolos_snapshot::Error::Stelae(error)) if error.is_absent() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn preflight(&self) -> Result<(), dolos_snapshot::Error> {
        Ok(registry::preflight(&self.registry, &DolosProfile)?)
    }

    pub fn preview(
        &self,
        plan: &Plan,
        source: &dyn SnapshotSource,
    ) -> Result<Preview, dolos_snapshot::Error> {
        source.preview_repository(self.publishing(), plan)
    }

    pub fn publish(
        &self,
        plan: &Plan,
        source: &dyn SnapshotSource,
        observer: &Observer,
    ) -> Result<Published, dolos_snapshot::Error> {
        source.publish_repository(self.publishing(), plan, observer)
    }

    /// Publish a prepared boundary, chained to the repository's latest stele,
    /// which has to be `predecessor`.
    pub fn finalize(
        &self,
        finalizing: &Finalizing<'_>,
        predecessor: Option<&Committed>,
        source: &dyn SnapshotSource,
        observer: &Observer,
    ) -> Result<Published, dolos_snapshot::Error> {
        let predecessor = predecessor
            .map(|held| Ok::<_, stelae::Error>((held.sequence, held.identity.parse()?)))
            .transpose()?;
        let publishing = self.publishing().following(predecessor);
        source.finalize_repository(publishing, finalizing, observer)
    }

    fn publishing(&self) -> Publishing<'_> {
        Publishing::new(&self.registry).recording_in(self.record_path.clone())
    }
}

/// A sequence as the repository holds it.
fn held(snapshot: &stelae::oci::Snapshot) -> Result<Held, dolos_snapshot::Error> {
    let inscription = snapshot.read_inscription()?;
    Ok(Held {
        committed: Committed {
            sequence: inscription.sequence,
            identity: inscription.digest()?.to_string(),
        },
        inscription,
    })
}

/// The blocks repository beside a Dolos repository, which a Dolos snapshot
/// names its blocks in.
pub struct Blocks {
    registry: Registry,
}

impl Blocks {
    pub fn open(
        config: &RootConfig,
        publish: &RepositoryPublish<'_>,
    ) -> Result<Self, dolos_snapshot::Error> {
        let registry = registry::open(
            &blocks_repository(publish.repo)?,
            publish.insecure,
            publish.auth.clone(),
            node::scratch_dir(&config.storage, publish.scratch_dir),
            Tuning::default(),
        )?;
        Ok(Self { registry })
    }

    pub fn repository(&self) -> String {
        self.registry.repository().to_string()
    }

    /// The inscription digest of the blocks snapshot at `plan`'s sequence,
    /// which the Dolos snapshot there names, or `None` when the repository
    /// holds none. One of another sequence or network is refused.
    pub fn reference(
        &self,
        plan: &Plan,
    ) -> Result<Option<Digest>, Box<dyn std::error::Error + Send + Sync>> {
        let held = match self.registry.pull_sequence(&BlocksProfile, plan.sequence) {
            Ok(held) => held,
            Err(error) if error.is_absent() => return Ok(None),
            Err(error) => return Err(dolos_snapshot::Error::from(error).into()),
        };
        let inscription = held
            .read_inscription()
            .map_err(dolos_snapshot::Error::from)?;
        Ok(Some(snapshot::blocks_reference(
            &self.repository(),
            &inscription,
            plan,
        )?))
    }

    /// `plan` naming the blocks snapshot at its sequence, refused when there
    /// is none to name.
    pub fn name_in(&self, plan: Plan) -> Result<Plan, Box<dyn std::error::Error + Send + Sync>> {
        match self.reference(&plan)? {
            Some(blocks) => Ok(plan.with_blocks(blocks)),
            None => Err(PolicyError::BlocksAbsent {
                repository: self.repository(),
                sequence: plan.sequence,
            }
            .into()),
        }
    }
}

pub struct BackfillPublisher<'a> {
    pub config: &'a RootConfig,
    pub settings: RepositoryPublish<'a>,
    pub observer: &'a Observer,
}

impl backfill::Publish for BackfillPublisher<'_> {
    fn announce(&self, plan: &Plan) -> Result<(), backfill::Error> {
        let report = PlanReport::read(plan).map_err(backfill::Error::caller)?;
        tracing::info!(
            sequence = report.sequence,
            cursor = %report.cursor,
            network = %report.network,
            "publishing pending boundary"
        );
        Ok(())
    }

    fn latest(&self, plan: &Plan) -> Result<Option<Committed>, backfill::Error> {
        Publisher::open(self.config, &self.settings)
            .and_then(|publisher| publisher.latest(plan))
            .map_err(backfill::Error::caller)
    }

    fn stele(&self, sequence: u64) -> Result<Option<Held>, backfill::Error> {
        Publisher::open(self.config, &self.settings)
            .and_then(|publisher| publisher.stele(sequence))
            .map_err(backfill::Error::caller)
    }

    fn blocks(&self, plan: &Plan) -> Result<Option<Digest>, backfill::Error> {
        Blocks::open(self.config, &self.settings)
            .map_err(backfill::Error::caller)?
            .reference(plan)
            .map_err(backfill::Error::Caller)
    }

    fn finalize(
        &self,
        finalizing: &Finalizing<'_>,
        predecessor: Option<&Committed>,
        source: &dyn SnapshotSource,
    ) -> Result<Committed, backfill::Error> {
        let published = Publisher::open(self.config, &self.settings)
            .and_then(|publisher| {
                publisher.preflight()?;
                publisher.finalize(finalizing, predecessor, source, self.observer)
            })
            .map_err(|error| backfill::Error::Finalizing(Box::new(error)))?;
        tracing::info!(
            sequence = published.inscription.sequence,
            identity = %published.identity,
            built = published.layers_built,
            reused = published.layers_reused,
            "published prepared boundary"
        );
        Ok(Committed {
            sequence: published.inscription.sequence,
            identity: published.identity.to_string(),
        })
    }

    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
        let publisher =
            Publisher::open(self.config, &self.settings).map_err(backfill::Error::caller)?;
        let standing = publisher.standing(plan).map_err(backfill::Error::caller)?;
        match Next::read(standing, plan.sequence, false).map_err(backfill::Error::caller)? {
            Next::Nothing(message) => {
                tracing::info!(%message);
                return Ok(());
            }
            Next::First | Next::After { .. } => {}
        }
        publisher.preflight().map_err(backfill::Error::caller)?;
        let published = publisher
            .publish(plan, source, self.observer)
            .map_err(backfill::Error::caller)?;
        tracing::info!(
            sequence = plan.sequence,
            identity = %published.identity,
            built = published.layers_built,
            reused = published.layers_reused,
            "published pending boundary"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Destination {
    /// A network root on disk: Dolos writes the blocks snapshot into its
    /// `blocks` layout and the Dolos snapshot naming it into its `dolos` one.
    Directory {
        path: PathBuf,
        scratch_dir: Option<PathBuf>,
    },
    /// A network root: the Dolos snapshot goes to its `dolos` repository and
    /// names the one its `blocks` repository holds at the same sequence.
    Repository {
        direct_s3: Option<Box<stelae::oci::s3_upload::Config>>,
        repository: Repository,
        auth: Auth,
        insecure: bool,
        scratch_dir: Option<PathBuf>,
        concurrency: Option<std::num::NonZeroUsize>,
        require_new: bool,
    },
}

impl Destination {
    /// Where this publish spools its index runs.
    fn spool(&self, storage: &StorageConfig) -> PathBuf {
        let (Self::Directory { scratch_dir, .. } | Self::Repository { scratch_dir, .. }) = self;
        node::scratch_dir(storage, scratch_dir.as_deref())
    }
}

#[derive(Debug)]
pub enum PublishOutcome {
    DryRun {
        sequence: u64,
    },
    Nothing(String),
    Directory {
        sequence: u64,
        identity: String,
        layers: usize,
    },
    Repository {
        sequence: u64,
        identity: String,
        built: usize,
        reused: usize,
        transfer: stelae::oci::Transfer,
    },
}

/// A publish starting a history refuses to where a batch already passed. A dry
/// run publishes nothing, and a repository holding anything refuses a gap
/// itself.
fn refuse_lost_history(
    next: &Next,
    dry_run: bool,
    storage_path: &Path,
    sequence: u64,
) -> Result<(), batch::Error> {
    match (next, dry_run) {
        (Next::First, false) => batch::refuse_lost_history(storage_path, sequence),
        _ => Ok(()),
    }
}

pub fn publish_once(
    config: &RootConfig,
    genesis: &dolos_core::Genesis,
    selection: dolos_snapshot::facade::Selection,
    destination: Destination,
    dry_run: bool,
    observer: &Observer,
) -> Result<(PlanReport, PublishOutcome), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if matches!(destination, Destination::Repository { .. }) && !dry_run {
        batch::refuse_pending(&config.storage.path)?;
    }
    let workspace =
        dolos::engine::ReplayWorkspace::open(config, std::sync::Arc::new(genesis.clone()))?;
    let result = (|| {
        let snapshot = workspace.snapshot();
        let spool = destination.spool(&config.storage);
        let plan = snapshot
            .selected_plan(u64::from(genesis.network_magic()), selection)?
            .with_scratch(spool);
        let report = PlanReport::read(&plan)?;
        let outcome = match destination {
            Destination::Directory { path, .. } => {
                if dry_run {
                    PublishOutcome::DryRun {
                        sequence: plan.sequence,
                    }
                } else {
                    let inscription = snapshot.publish_directory(&path, &plan, observer)?;
                    PublishOutcome::Directory {
                        sequence: plan.sequence,
                        identity: inscription.digest()?.to_string(),
                        layers: inscription.layers().count(),
                    }
                }
            }
            Destination::Repository {
                direct_s3,
                repository,
                auth,
                insecure,
                scratch_dir,
                concurrency,
                require_new,
            } => {
                let settings = RepositoryPublish {
                    repo: &repository,
                    auth,
                    insecure,
                    scratch_dir: scratch_dir.as_deref(),
                    tuning: Tuning {
                        direct_s3,
                        concurrency,
                    },
                };
                let publisher = Publisher::open(config, &settings)?;
                let standing = publisher.standing(&plan)?;
                match Next::read(standing, plan.sequence, require_new)? {
                    Next::Nothing(message) => PublishOutcome::Nothing(message),
                    next @ (Next::First | Next::After { .. }) => {
                        refuse_lost_history(&next, dry_run, &config.storage.path, plan.sequence)?;
                        let plan = Blocks::open(config, &settings)?.name_in(plan)?;
                        publisher.preflight()?;
                        if dry_run {
                            let _preview = publisher.preview(&plan, &snapshot)?;
                            PublishOutcome::DryRun {
                                sequence: plan.sequence,
                            }
                        } else {
                            let published = publisher.publish(&plan, &snapshot, observer)?;
                            PublishOutcome::Repository {
                                sequence: plan.sequence,
                                identity: published.identity.to_string(),
                                built: published.layers_built,
                                reused: published.layers_reused,
                                transfer: published.transfer,
                            }
                        }
                    }
                }
            }
        };
        Ok::<_, Box<dyn std::error::Error + Send + Sync + 'static>>((report, outcome))
    })();
    let finish = workspace
        .finish()
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) });
    match (result, finish) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(finish)) => Err(format!(
            "publication failed ({operation}); closing stores also failed ({finish})"
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_publish_spools_under_the_scratch_directory_it_is_given() {
        let storage = StorageConfig {
            path: PathBuf::from("/data"),
            ..Default::default()
        };
        let storage = &storage;
        let repository = |scratch_dir: Option<&str>| Destination::Repository {
            direct_s3: None,
            repository: "oci://registry.invalid/cardano/preview".parse().unwrap(),
            auth: Auth::Anonymous,
            insecure: false,
            scratch_dir: scratch_dir.map(PathBuf::from),
            concurrency: None,
            require_new: false,
        };
        let directory = |scratch_dir: Option<&str>| Destination::Directory {
            path: PathBuf::from("/stele"),
            scratch_dir: scratch_dir.map(PathBuf::from),
        };
        let default = storage.path.join("scratch");
        assert_eq!(
            repository(Some("/spool")).spool(storage),
            Path::new("/spool")
        );
        assert_eq!(repository(None).spool(storage), default);
        assert_eq!(
            directory(Some("/spool")).spool(storage),
            Path::new("/spool")
        );
        assert_eq!(directory(None).spool(storage), default);
    }

    /// A node whose first batch began over an empty repository keeps the
    /// record, and later publishes to the repository it then filled must
    /// not read it.
    #[test]
    fn only_a_publish_starting_a_history_reads_the_record_of_a_batch() {
        let storage = tempfile::tempdir().unwrap();
        let source = batch::Source {
            repository: "oci://registry.invalid/cardano/preview".to_owned(),
            network_magic: 2,
            store_contract: "contract".to_owned(),
            mutable_slots: 129_600,
            dolos_revision: "revision".to_owned(),
        };
        let dir = batch::dir_in(storage.path());
        batch::Batch::open(dir, batch::Limits::ONE, source)
            .unwrap()
            .begin(storage.path(), 1, None)
            .unwrap();

        let reading = |next, dry_run| refuse_lost_history(next, dry_run, storage.path(), 4);
        assert!(matches!(
            reading(&Next::First, false),
            Err(batch::Error::HistoryLost { .. })
        ));
        assert!(reading(&Next::First, true).is_ok());
        assert!(reading(&Next::After { latest: 3 }, false).is_ok());
    }

    #[test]
    fn repository_standing_maps_to_first_next_noop_and_refusal() {
        assert_eq!(
            Next::read(Standing::Empty, 500, false).unwrap(),
            Next::First
        );
        assert_eq!(
            Next::read(Standing::Next { latest: 499 }, 500, false).unwrap(),
            Next::After { latest: 499 }
        );
        assert!(matches!(
            Next::read(Standing::UpToDate { latest: 500 }, 500, false).unwrap(),
            Next::Nothing(_)
        ));
        assert!(Next::read(Standing::UpToDate { latest: 500 }, 500, true).is_err());
        assert!(Next::read(
            Standing::Ahead {
                latest: 497,
                distance: 3,
            },
            500,
            false,
        )
        .is_err());
    }
}
