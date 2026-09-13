//! Stelae-owned policy for publishing the Dolos Cardano profile.

use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export::Plan,
    facade::SnapshotSource,
    node,
    planning::{self, PlanReport},
    registry::{self, Auth, Preview, Published, Repository, Tuning},
};
use stelae::progress::Observer;
use stelae_driver::Standing;

use crate::backfill;

/// Resolved repository publication settings.
pub struct RepositoryPublish<'a> {
    pub repo: &'a Repository,
    pub insecure: bool,
    pub scratch_dir: Option<&'a Path>,
    pub rebuild: bool,
    pub tuning: Tuning,
}

/// The policy decision made from the repository's current standing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    First,
    After { latest: u64 },
    Nothing(String),
}

impl Next {
    pub fn read(
        standing: Standing,
        sequence: u64,
        require_new: bool,
    ) -> Result<Self, dolos_snapshot::Error> {
        match standing {
            Standing::Empty => Ok(Self::First),
            Standing::Next { latest } => Ok(Self::After { latest }),
            Standing::UpToDate { latest } => {
                let message = format!(
                    "nothing to publish: this repository is at sequence {latest} and this node is at sequence {sequence}"
                );
                if require_new {
                    Err(dolos_snapshot::Error::NothingToPublish(message))
                } else {
                    Ok(Self::Nothing(message))
                }
            }
            Standing::Ahead { latest, distance } => Err(dolos_snapshot::Error::PublishWouldGap {
                latest,
                sequence,
                distance,
            }),
        }
    }
}

/// An opened repository plus the journal and reuse policy of this publisher.
pub struct Publisher {
    inner: dolos_snapshot::publisher::Publisher,
}

impl Publisher {
    pub fn open(
        config: &RootConfig,
        publish: &RepositoryPublish<'_>,
        auth: Auth,
    ) -> Result<Self, dolos_snapshot::Error> {
        let scratch = node::scratch_dir(&config.storage, publish.scratch_dir);
        Self::open_explicit(
            publish.repo,
            publish.insecure,
            auth,
            scratch,
            registry::record_path_in(&config.storage.path),
            publish.rebuild,
            publish.tuning,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_explicit(
        repository: &Repository,
        insecure: bool,
        auth: Auth,
        scratch_dir: PathBuf,
        record_path: PathBuf,
        rebuild: bool,
        tuning: Tuning,
    ) -> Result<Self, dolos_snapshot::Error> {
        let inner = dolos_snapshot::publisher::Publisher::open_explicit(
            repository,
            insecure,
            auth,
            scratch_dir,
            record_path,
            rebuild,
            tuning,
        )?;

        Ok(Self { inner })
    }

    pub fn standing(&self, plan: &Plan) -> Result<Standing, dolos_snapshot::Error> {
        self.inner.standing(plan)
    }

    pub fn preflight(&self) -> Result<(), dolos_snapshot::Error> {
        self.inner.preflight()
    }

    pub fn preview(
        &self,
        plan: &Plan,
        source: &dyn SnapshotSource,
    ) -> Result<Preview, dolos_snapshot::Error> {
        source.preview(&self.inner, plan)
    }

    pub fn publish(
        &self,
        plan: &Plan,
        source: &dyn SnapshotSource,
        observer: &Observer,
    ) -> Result<Published, dolos_snapshot::Error> {
        source.publish(&self.inner, plan, observer)
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

    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
        let auth = node::registry_auth(&self.config.stelae).map_err(backfill::Error::caller)?;
        let publisher =
            Publisher::open(self.config, &self.settings, auth).map_err(backfill::Error::caller)?;
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
    Directory(PathBuf),
    Repository {
        repository: Repository,
        insecure: bool,
        scratch_dir: Option<PathBuf>,
        rebuild: bool,
        concurrency: Option<std::num::NonZeroUsize>,
        verify_carried: bool,
        require_new: bool,
    },
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

pub fn publish_once(
    config: &RootConfig,
    genesis: &dolos_core::Genesis,
    selection: dolos_snapshot::facade::Selection,
    destination: Destination,
    dry_run: bool,
    observer: &Observer,
) -> Result<(PlanReport, PublishOutcome), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let workspace =
        dolos::engine::ReplayWorkspace::open(config, std::sync::Arc::new(genesis.clone()))?;
    let result = (|| {
        let snapshot = workspace.snapshot();
        let retained = planning::retained_epochs(config)?;
        let plan =
            snapshot.selected_plan(u64::from(genesis.network_magic()), retained, selection)?;
        let report = PlanReport::read(&plan)?;
        let outcome = match destination {
            Destination::Directory(path) => {
                if dry_run {
                    PublishOutcome::DryRun {
                        sequence: plan.sequence,
                    }
                } else {
                    let inscription = snapshot.publish_directory(&path, &plan, observer)?;
                    PublishOutcome::Directory {
                        sequence: plan.sequence,
                        identity: inscription.digest()?.to_string(),
                        layers: inscription.layers.len(),
                    }
                }
            }
            Destination::Repository {
                repository,
                insecure,
                scratch_dir,
                rebuild,
                concurrency,
                verify_carried,
                require_new,
            } => {
                let settings = RepositoryPublish {
                    repo: &repository,
                    insecure,
                    scratch_dir: scratch_dir.as_deref(),
                    rebuild,
                    tuning: Tuning {
                        concurrency,
                        verify_adopted: verify_carried,
                    },
                };
                let auth = node::registry_auth(&config.stelae)?;
                let publisher = Publisher::open(config, &settings, auth)?;
                let standing = publisher.standing(&plan)?;
                match Next::read(standing, plan.sequence, require_new)? {
                    Next::Nothing(message) => PublishOutcome::Nothing(message),
                    Next::First | Next::After { .. } => {
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
