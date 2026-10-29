//! Reading and reproducing published steles: what `digest`, `verify` and
//! `inspect` run.
//!
//! Every check is the Dolos profile's own. This module opens the stores and the
//! stele a command names and composes the profile's digest, transport and
//! reproduction entry points over them. A directory stele has no registry
//! manifest, so its layers are checked through the protocol reader the way the
//! profile checks a repository's.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::{BulkReplayError, ReplayWorkspace};
use dolos_core::{config::RootConfig, Genesis};
use dolos_snapshot::{
    export::{Document, First, Following, Plan, Predecessor as _},
    facade::{Selection, SnapshotRepository, SnapshotSource},
    node,
    registry::{Auth, Point, Tuning},
    restore::Source,
    DolosProfile, RetainedEpochs,
};
use stelae::{dir::SteleDir, frame::Limits, inscription::Inscription, SteleReader as _};

pub use dolos_snapshot::{
    boundary::Boundary,
    planning::PlanReport,
    read_position,
    registry::{Inspected, Verified},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("opening the data stores")]
    Stores(#[source] BulkReplayError),

    #[error("closing the data stores")]
    Close(#[source] BulkReplayError),

    #[error("reading {}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{what}")]
    Profile {
        what: String,
        #[source]
        source: dolos_snapshot::Error,
    },

    /// The check ran and the stele failed it.
    #[error("{what}")]
    Mismatch {
        what: String,
        #[source]
        source: dolos_snapshot::Error,
    },

    #[error("{} holds one stele, sequence {sequence}, and not {point}", path.display())]
    PointNotInDirectory {
        path: PathBuf,
        sequence: u64,
        point: Point,
    },
}

impl Error {
    /// Whether a verification found the stele wrong, rather than failing to
    /// run.
    pub fn is_mismatch(&self) -> bool {
        matches!(self, Self::Mismatch { .. })
    }

    fn profile(what: impl Into<String>) -> impl FnOnce(dolos_snapshot::Error) -> Self {
        let what = what.into();
        move |source| Self::Profile { what, source }
    }

    /// A verification failure, classified by whether it is the stele's.
    fn verifying(what: impl Into<String>) -> impl FnOnce(dolos_snapshot::Error) -> Self {
        let what = what.into();
        move |source| {
            if found_wrong(&source) {
                Self::Mismatch { what, source }
            } else {
                Self::Profile { what, source }
            }
        }
    }
}

/// The failures that are a published stele disagreeing with itself or with the
/// local stores, as opposed to one that could not be read.
fn found_wrong(error: &dolos_snapshot::Error) -> bool {
    use dolos_snapshot::Error as E;

    matches!(
        error,
        E::LayerVerification { .. }
            | E::ReproductionMismatch { .. }
            | E::Stelae(
                stelae::Error::ManifestMismatch(_) | stelae::Error::NonCanonicalInscription
            )
    )
}

/// Open the stores, plan `selection` over them and establish the boundary
/// cut, then run `then` over the plan before closing them.
///
/// One sequence for `digest` and `verify --reproduce`, because a node that
/// gave them different plans would be verifying a different document.
/// `announce` hears the plan before anything slow runs.
pub fn with_plan<T>(
    config: &RootConfig,
    genesis: &Genesis,
    retained: RetainedEpochs,
    selection: Selection,
    announce: &dyn Fn(&PlanReport, &Boundary),
    then: impl FnOnce(&dyn SnapshotSource, &Plan) -> Result<T, Error>,
) -> Result<T, Error> {
    let workspace =
        ReplayWorkspace::open(config, Arc::new(genesis.clone())).map_err(Error::Stores)?;
    let result = (|| {
        let snapshot = workspace.snapshot();
        let what = "planning the reproduction";
        let plan = snapshot
            .selected_plan(u64::from(genesis.network_magic()), retained, selection)
            .map_err(Error::profile(what))?;
        let boundary = snapshot.boundary(&plan).map_err(Error::profile(what))?;
        announce(
            &PlanReport::read(&plan).map_err(Error::profile(what))?,
            &boundary,
        );
        then(&snapshot, &plan)
    })();
    let finish = workspace.finish().map_err(Error::Close);
    match (result, finish) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

/// A reproduced inscription, and the history it was chained onto.
pub struct Digested {
    /// Entries of the predecessor's history extended, or `None` for a
    /// reproduction that starts a chain.
    pub history: Option<usize>,
    pub document: Document,
}

/// Reproduce `plan`'s canonical inscription, writing nothing.
///
/// `chain_from` is a predecessor's canonical inscription: `history` lives in
/// the document, so a reproduction is told its chain rather than inferring
/// one. It is read before the stores are walked.
pub fn digest(
    snapshot: &dyn SnapshotSource,
    plan: &Plan,
    chain_from: Option<&Path>,
) -> Result<Digested, Error> {
    let previous = chain_from
        .map(|path| {
            let raw = std::fs::read(path).map_err(|source| Error::Read {
                path: path.to_owned(),
                source,
            })?;
            Following::read(&raw, plan)
                .map_err(Error::profile(format!("chaining onto {}", path.display())))
        })
        .transpose()?;

    let document = match &previous {
        Some(following) => snapshot.digest_document(plan, following),
        None => snapshot.digest_document(plan, &First),
    }
    .map_err(Error::profile("reproducing the stele"))?;

    Ok(Digested {
        history: previous.as_ref().map(|following| following.history().len()),
        document,
    })
}

/// Rebuild every layer of `published` from the local stores and require the
/// result to be byte-identical.
pub fn reproduce(
    snapshot: &dyn SnapshotSource,
    plan: &Plan,
    published: &Inscription,
) -> Result<Inscription, Error> {
    snapshot
        .verify_reproduction(published, plan)
        .map_err(Error::verifying(
            "reproducing the published stele from the local stores",
        ))
}

/// A published stele's source, opened.
pub enum Published {
    Directory(SteleDir),
    Repository(SnapshotRepository),
}

impl Published {
    /// Open `source`. A repository stages blobs under the node's default
    /// scratch directory; a directory is read in place.
    pub fn open(
        config: &RootConfig,
        source: &Source,
        insecure: bool,
        auth: Auth,
    ) -> Result<Self, Error> {
        let what = "opening the stele";
        match source {
            Source::Dir(path) => SteleDir::open(path)
                .map(Self::Directory)
                .map_err(|error| Error::profile(what)(error.into())),
            Source::Repo(repository) => SnapshotRepository::open(
                repository,
                insecure,
                auth,
                node::scratch_dir(&config.storage, None),
                Tuning::default(),
            )
            .map(Self::Repository)
            .map_err(Error::profile(what)),
        }
    }

    /// Read the stele's documents without pulling a layer.
    pub fn inspect(&self, point: Point) -> Result<Inspected, Error> {
        let what = "reading the stele";
        match self {
            Self::Repository(repository) => repository.inspect(point).map_err(Error::profile(what)),
            Self::Directory(dir) => {
                let inspected =
                    inspect_dir(dir).map_err(|error| Error::profile(what)(error.into()))?;
                at_point(dir, &inspected.inscription, point)?;
                Ok(inspected)
            }
        }
    }

    /// Check the stele's digests: its documents against each other, then every
    /// blob against both of its digests.
    pub fn verify(&self, point: Point) -> Result<Verified, Error> {
        let what = "verifying the stele";
        match self {
            Self::Repository(repository) => {
                repository.verify(point).map_err(Error::verifying(what))
            }
            Self::Directory(dir) => {
                let verified = verify_dir(dir).map_err(Error::verifying(what))?;
                at_point(dir, &verified.inscription, point)?;
                Ok(verified)
            }
        }
    }
}

/// A directory holds one stele: `latest`, or the epoch it was published at.
fn at_point(dir: &SteleDir, inscription: &Inscription, point: Point) -> Result<(), Error> {
    match point {
        Point::Epoch(epoch) if epoch != inscription.sequence => Err(Error::PointNotInDirectory {
            path: dir.root().to_owned(),
            sequence: inscription.sequence,
            point,
        }),
        _ => Ok(()),
    }
}

fn inspect_dir(dir: &SteleDir) -> Result<Inspected, stelae::Error> {
    let inscription = dir.read_inscription()?;
    let blobs = dir.blob_index()?;
    let compressed = inscription
        .layers
        .iter()
        .map(|descriptor| dir.compressed_size(&blobs, descriptor))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Inspected {
        identity: inscription.digest()?,
        total_compressed: compressed.iter().flatten().sum(),
        inscription,
        compressed,
    })
}

/// The profile's repository verification over a directory: each layer
/// streamed under the profile's record ceiling, held against its descriptor
/// by the reader, and its bytes against the blob name that places it.
fn verify_dir(dir: &SteleDir) -> Result<Verified, dolos_snapshot::Error> {
    let inscription = dir.read_inscription()?;
    let blobs = dir.blob_index()?;
    let limits = Limits {
        max_record: dolos_snapshot::MAX_RECORD,
        ..Limits::default()
    };

    let mut compressed_bytes = 0u64;
    for descriptor in &inscription.layers {
        let check = || -> Result<u64, stelae::Error> {
            let digests = dir
                .stream_layer(&blobs, &DolosProfile, descriptor, limits)?
                .finish()?;
            if let Some(named) = blobs.blob_for(&descriptor.diff_id) {
                if digests.blob_digest != named {
                    return Err(stelae::Error::ManifestMismatch(format!(
                        "the blob index names this layer's blob {named} and its bytes hash to {}",
                        digests.blob_digest
                    )));
                }
            }
            Ok(digests.compressed_size)
        };
        compressed_bytes += check().map_err(|source| dolos_snapshot::Error::LayerVerification {
            kind: descriptor.kind.clone(),
            scope: descriptor.scope.to_string(),
            source: Box::new(source.into()),
        })?;
    }

    Ok(Verified {
        identity: inscription.digest()?,
        inscription,
        compressed_bytes,
    })
}
