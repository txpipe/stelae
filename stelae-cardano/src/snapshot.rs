//! Reading and reproducing published steles: what `digest`, `verify` and
//! `inspect` run.
//!
//! Every check is the Dolos profile's own. This module opens the stores and the
//! stele a command names and composes the profile's digest, transport and
//! reproduction entry points over them. A directory stele has no registry
//! manifest, so its inscription is held to the profile the way a registry pull
//! holds it, and its layers are checked through the protocol reader the way the
//! profile checks a repository's.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::{BulkReplayError, ReplayWorkspace};
use dolos_cardano::networks::KnownNetwork;
use dolos_core::{config::RootConfig, Genesis};
use dolos_snapshot::{
    export::{Document, First, Following, Plan, Predecessor as _},
    facade::{Selection, SnapshotRepository, SnapshotSource},
    node,
    registry::{Auth, Point, Repository, Tuning},
    restore::Source,
    DolosProfile, RetainedEpochs,
};
use stelae::{
    dir::{SteleDir, BLOB_INDEX_FILE},
    frame::Limits,
    inscription::Inscription,
    SteleReader as _,
};

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

/// Whether a check refused the published stele's own bytes or documents,
/// rather than failing to read them or to read the local stores.
///
/// A layer's failure is classified by what it wraps: a registry that keeps
/// answering 503 is not a corrupt layer.
fn found_wrong(error: &dolos_snapshot::Error) -> bool {
    use dolos_snapshot::Error as E;

    match error {
        E::LayerVerification { source, .. } => found_wrong(source),
        E::Stelae(error) => refused_stele(error),
        E::ReproductionMismatch { .. }
        | E::NetworkMismatch { .. }
        | E::MalformedInscription { .. }
        | E::MalformedRecord { .. }
        | E::OutOfOrder { .. }
        | E::ScopeMismatch { .. }
        | E::UnknownDimension(_)
        | E::UnknownNamespace(_)
        | E::UnknownExactKind(_) => true,
        _ => false,
    }
}

/// Exhaustive, so a new protocol refusal is classified where it is added.
fn refused_stele(error: &stelae::Error) -> bool {
    use stelae::Error as E;

    match error {
        E::Json(_)
        | E::NonCanonicalCbor { .. }
        | E::TruncatedCbor { .. }
        | E::CborTooDeep { .. }
        | E::RecordTooLarge { .. }
        | E::TrailingCbor { .. }
        | E::MalformedHeader(_)
        | E::Canonicalization(_)
        | E::UnsafeInteger { .. }
        | E::NonIntegerNumber { .. }
        | E::UnsupportedSchema { .. }
        | E::UnknownProfile { .. }
        | E::UnsupportedProfileVersion { .. }
        | E::HistoryInvariant(_)
        | E::MalformedRecord { .. }
        | E::InvalidDigest { .. }
        | E::InvalidMediaType { .. }
        | E::InvalidProfileName { .. }
        | E::InvalidTag { .. }
        | E::UnknownLayerKind { .. }
        | E::DigestMismatch { .. }
        | E::LayerNotFound { .. }
        | E::LayerMismatch { .. }
        | E::DecompressedTooLarge { .. }
        | E::NonCanonicalInscription
        | E::ManifestMismatch(_)
        | E::ManifestTooLarge { .. } => true,
        E::Io(_)
        | E::CborEncode(_)
        | E::HistoryBreak { .. }
        | E::BlobMissing { .. }
        | E::LayerNotWritten(_)
        | E::InvalidRepository { .. }
        | E::Scratch { .. }
        | E::Registry(_) => false,
    }
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
                let inscription = read_inscription(dir).map_err(Error::profile(what))?;
                at_point(dir, &inscription, point)?;
                inspect_dir(dir, inscription).map_err(|error| Error::profile(what)(error.into()))
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
                let inscription = read_inscription(dir).map_err(Error::verifying(what))?;
                // Before any layer streams: the directory holds one stele, and
                // a point naming another is not a reason to read this one.
                at_point(dir, &inscription, point)?;
                verify_dir(dir, inscription).map_err(Error::verifying(what))
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

/// The inscription, refused unless it is this profile's, as a registry pull
/// refuses it.
fn read_inscription(dir: &SteleDir) -> Result<Inscription, dolos_snapshot::Error> {
    let inscription = dir.read_inscription()?;
    inscription.check_profile(&DolosProfile)?;
    Ok(inscription)
}

/// Compressed sizes come from the blob index, and only from its sidecar: a
/// directory without one would have every blob read to build it.
fn inspect_dir(dir: &SteleDir, inscription: Inscription) -> Result<Inspected, stelae::Error> {
    let compressed = if dir.root().join(BLOB_INDEX_FILE).exists() {
        let blobs = dir.blob_index()?;
        inscription
            .layers
            .iter()
            .map(|descriptor| dir.compressed_size(&blobs, descriptor))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![None; inscription.layers.len()]
    };

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
fn verify_dir(dir: &SteleDir, inscription: Inscription) -> Result<Verified, dolos_snapshot::Error> {
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

/// The configured network's published read-only user, for its official
/// repository and no other, as Dolos reads it: its password is compiled in and
/// has no business reaching another registry.
pub fn official_reader(config: &RootConfig, repository: &Repository) -> Option<&'static str> {
    let network = KnownNetwork::by_name(&config.chain.network)?;
    let official: Repository = network.stelae.registry.parse().ok()?;

    (official.registry() == repository.registry() && !network.stelae.user.is_empty())
        .then_some(network.stelae.user)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(source: stelae::Error) -> dolos_snapshot::Error {
        dolos_snapshot::Error::LayerVerification {
            kind: "blocks".to_owned(),
            scope: "epoch 1".to_owned(),
            source: Box::new(source.into()),
        }
    }

    #[test]
    fn a_layer_is_wrong_by_what_failed_in_it() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::StorageFull,
            std::io::ErrorKind::PermissionDenied,
        ] {
            let error = layer(stelae::Error::Io(kind.into()));
            assert!(!found_wrong(&error), "{kind:?}");
            assert!(!Error::verifying("verifying")(error).is_mismatch());
        }

        let error = layer(stelae::Error::DigestMismatch {
            subject: "blob".to_owned(),
            expected: "a".to_owned(),
            actual: "b".to_owned(),
        });
        assert!(found_wrong(&error));
        assert!(Error::verifying("verifying")(error).is_mismatch());
    }
}
