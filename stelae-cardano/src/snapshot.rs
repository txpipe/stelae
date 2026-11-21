//! Reading and reproducing published snapshots: what `digest`, `verify` and
//! `inspect` run.
//!
//! Every check is the Dolos profile's own. This module opens the stores and the
//! snapshot a command names and composes the profile's digest, transport and
//! reproduction entry points over them. A source names a network root: its
//! `dolos` repository holds the snapshots, and its `blocks` repository the
//! blocks snapshots they name, each a registry repository or an OCI image
//! layout on disk. A layout's documents are held to the profile the way a
//! registry pull holds them, and its layers are checked through the protocol
//! reader the way the profile checks a repository's.

use std::{path::Path, sync::Arc};

use dolos::engine::{BulkReplayError, ReplayWorkspace};
use dolos_cardano::networks::KnownNetwork;
use dolos_core::{config::RootConfig, Genesis};
use dolos_snapshot::{
    export::{Document, Following, Plan, Predecessor, BLOCKS_REPOSITORY, DOLOS_REPOSITORY},
    facade::{Selection, SnapshotSource},
    node,
    registry::{self, Auth, Point, Registry, Repository, Tuning},
    restore::Source,
    DolosProfile,
};
use stelae::{
    dir::SnapshotDir,
    frame::Limits,
    inscription::{Inscription, Link},
    Digest, Profile, SnapshotReader as _,
};
use stelae_blocks::BlocksProfile;

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

    // This variant and the next box the profile's error, which is large enough
    // to trip `result_large_err` on every result carrying this one.
    #[error("{what}")]
    Profile {
        what: String,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },

    /// The check ran and the snapshot failed it.
    #[error("{what}")]
    Mismatch {
        what: String,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },

    #[error("a snapshot at sequence 0 follows nothing, so --previous names no snapshot before it")]
    NothingBefore,

    /// A network root whose blocks repository holds nothing at the plan's
    /// sequence, which a publish there refuses as well.
    #[error(
        "the blocks repository {repository} holds no blocks snapshot at epoch-{sequence}, which the \
         Dolos snapshot there names; --blocks names one in its place"
    )]
    BlocksAbsent { repository: String, sequence: u64 },

    #[error(transparent)]
    Blocks(#[from] BlocksRefusal),
}

/// A blocks snapshot the Dolos snapshot at a plan's sequence cannot name.
#[derive(Debug, thiserror::Error)]
pub enum BlocksRefusal {
    /// A tag naming another sequence's snapshot, which a copied or moved tag
    /// leaves: the protocol's pull does not hold an inscription to its tag.
    #[error(
        "the blocks repository {repository} holds the blocks snapshot of sequence {found} at \
         epoch-{sequence}, and a Dolos snapshot names the blocks snapshot of its own sequence"
    )]
    OtherSequence {
        repository: String,
        sequence: u64,
        found: u64,
    },

    #[error(
        "the blocks repository {repository} holds a blocks snapshot for network magic {found}, \
         and this node is configured for {expected}"
    )]
    OtherNetwork {
        repository: String,
        expected: u64,
        found: u64,
    },

    #[error("reading the blocks snapshot at epoch-{sequence} in {repository}")]
    Unreadable {
        repository: String,
        sequence: u64,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },
}

/// The digest the Dolos snapshot at `plan`'s sequence names `blocks` by, the
/// inscription `repository` holds at that sequence.
///
/// Refused when it is another sequence's or another network's: Dolos takes
/// the reference as given, and a Dolos snapshot naming either could not be
/// restored.
pub fn blocks_reference(
    repository: &str,
    blocks: &Inscription,
    plan: &Plan,
) -> Result<Digest, BlocksRefusal> {
    let unreadable = |source: dolos_snapshot::Error| BlocksRefusal::Unreadable {
        repository: repository.to_owned(),
        sequence: plan.sequence,
        source: Box::new(source),
    };

    if blocks.sequence != plan.sequence {
        return Err(BlocksRefusal::OtherSequence {
            repository: repository.to_owned(),
            sequence: plan.sequence,
            found: blocks.sequence,
        });
    }

    let found = stelae_blocks::read_position(&blocks.position)
        .map_err(|error| unreadable(error.into()))?
        .network_magic;
    let expected = plan.network.magic();
    if found != expected {
        return Err(BlocksRefusal::OtherNetwork {
            repository: repository.to_owned(),
            expected,
            found,
        });
    }

    blocks.digest().map_err(|error| unreadable(error.into()))
}

impl Error {
    /// Whether a verification found the snapshot wrong, rather than failing to
    /// run.
    pub fn is_mismatch(&self) -> bool {
        matches!(self, Self::Mismatch { .. })
    }

    fn profile(what: impl Into<String>) -> impl FnOnce(dolos_snapshot::Error) -> Self {
        let what = what.into();
        move |source| Self::Profile {
            what,
            source: Box::new(source),
        }
    }

    /// A verification failure, classified by whether it is the snapshot's.
    fn verifying(what: impl Into<String>) -> impl FnOnce(dolos_snapshot::Error) -> Self {
        Self::classified(what, found_wrong)
    }

    fn classified(
        what: impl Into<String>,
        wrong: fn(&dolos_snapshot::Error) -> bool,
    ) -> impl FnOnce(dolos_snapshot::Error) -> Self {
        let what = what.into();
        move |source| {
            let wrong = wrong(&source);
            let source = Box::new(source);
            if wrong {
                Self::Mismatch { what, source }
            } else {
                Self::Profile { what, source }
            }
        }
    }
}

/// Whether a check refused the published snapshot's own bytes or documents,
/// rather than failing to read them or to read the local stores.
///
/// A layer's failure is classified by what it wraps: a registry that keeps
/// answering 503 is not a corrupt layer.
fn found_wrong(error: &dolos_snapshot::Error) -> bool {
    use dolos_snapshot::Error as E;

    match error {
        E::LayerVerification { source, .. } => found_wrong(source),
        E::Stelae(error) => refused_snapshot(error),
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
pub(crate) fn refused_snapshot(error: &stelae::Error) -> bool {
    use stelae::Error as E;

    match error {
        // The digest check `pull_blob` runs, and the size ceiling beside it.
        E::Registry(_) => error.is_blob_refusal(),
        // Classified by what the last attempt hit, as a layer failure is.
        E::RetriesExhausted { source, .. } => refused_snapshot(source),
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
        | E::LinkInvariant(_)
        | E::DuplicateLayer { .. }
        | E::DuplicateInChain { .. }
        // A document the snapshot's own chain names and the source doesn't hold:
        // the snapshot can't be resolved, whatever else is published.
        | E::MissingLink { .. }
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
        | E::PredecessorMoved { .. }
        | E::LinkBreak { .. }
        // Absences, and a tip the registry no longer keeps: nothing published
        // is wrong, there is less of it.
        | E::TipGone { .. }
        | E::NoSuchTag { .. }
        | E::TagTaken { .. }
        | E::Layout(_)
        | E::LayerNotWritten(_)
        | E::InvalidRepository { .. }
        | E::Scratch { .. } => false,
    }
}

/// Whether a reproduction that ran found the snapshot differing from the local
/// stores, rather than failing to read them.
///
/// Narrower than [`found_wrong`]: a reproduction re-runs the export, and the
/// export refuses malformed or out-of-order records from the stores it reads
/// with the same errors a verification raises for a snapshot's own. The
/// published documents were held to the profile before the reproduction began,
/// so whatever else it raises came from the stores.
fn reproduction_differs(error: &dolos_snapshot::Error) -> bool {
    use dolos_snapshot::Error as E;

    matches!(
        error,
        E::ReproductionMismatch { .. } | E::NetworkMismatch { .. }
    )
}

/// Open the stores, plan `selection` over them and establish the boundary
/// cut, then run `then` over the plan before closing them.
///
/// One sequence for `digest` and `verify --reproduce`, because a node that
/// gave them different plans would be verifying a different document. Both
/// spool index runs under `scratch_dir`, or the storage path's default.
/// `announce` hears the plan before anything slow runs.
pub fn with_plan<T>(
    config: &RootConfig,
    genesis: &Genesis,
    selection: Selection,
    scratch_dir: Option<&Path>,
    announce: &dyn Fn(&PlanReport, &Boundary),
    then: impl FnOnce(&dyn SnapshotSource, &Plan) -> Result<T, Error>,
) -> Result<T, Error> {
    let workspace =
        ReplayWorkspace::open(config, Arc::new(genesis.clone())).map_err(Error::Stores)?;
    let result = (|| {
        let snapshot = workspace.snapshot();
        let what = "planning the reproduction";
        let plan = snapshot
            .selected_plan(u64::from(genesis.network_magic()), selection)
            .map_err(Error::profile(what))?
            .with_scratch(node::scratch_dir(&config.storage, scratch_dir));
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

/// What a reproduction is chained to and names, as the snapshot it reproduces
/// would be.
#[derive(Debug, Clone, Copy, Default)]
pub struct Inputs {
    /// The snapshot before it, or `None` where it starts a lineage.
    pub previous: Option<Link>,
    /// The blocks snapshot at its sequence, or `None` to name none.
    pub blocks: Option<Digest>,
}

/// The link to the snapshot `digest` names, just before `sequence`.
pub fn previous_at(sequence: u64, digest: Digest) -> Result<Link, Error> {
    Ok(Link {
        sequence: sequence.checked_sub(1).ok_or(Error::NothingBefore)?,
        inscription_digest: digest,
    })
}

/// A link held as a document would carry it: what a reproduction chains with
/// when it is given the link rather than the document.
struct Linked(Option<Link>);

impl Predecessor for Linked {
    fn previous(&self) -> Option<Link> {
        self.0
    }
}

/// A reproduced inscription, and what it was chained to and names.
pub struct Digested {
    pub inputs: Inputs,
    pub document: Document,
}

/// Reproduce `plan`'s canonical inscription with `inputs`, writing nothing.
pub fn digest(
    snapshot: &dyn SnapshotSource,
    plan: &Plan,
    inputs: Inputs,
) -> Result<Digested, Error> {
    let plan = match inputs.blocks {
        Some(blocks) => plan.clone().with_blocks(blocks),
        None => plan.clone(),
    };
    let document = snapshot
        .digest_document(&plan, &Linked(inputs.previous))
        .map_err(Error::profile("reproducing the snapshot"))?;

    Ok(Digested { inputs, document })
}

/// Rebuild every layer of `published` from the local stores and require the
/// result to be byte-identical.
///
/// The transport check's chain walk held the published link to the snapshot
/// before it. `previous`, when the caller names one, is held to that link as
/// well, so the documents compared are chained where the caller says.
pub fn reproduce(
    snapshot: &dyn SnapshotSource,
    plan: &Plan,
    published: &Inscription,
    previous: Option<Link>,
) -> Result<Inscription, Error> {
    // The one part of the published documents the reproduction reads that the
    // transport check did not.
    read_position(&published.position).map_err(Error::verifying(
        "reading the published snapshot's position",
    ))?;

    if let Some(previous) = previous.filter(|previous| published.previous != Some(*previous)) {
        let follows = match published.previous {
            Some(link) => format!("{} at sequence {}", link.inscription_digest, link.sequence),
            None => "nothing".to_owned(),
        };
        return Err(Error::Mismatch {
            what: "holding the published snapshot to --previous".to_owned(),
            source: Box::new(dolos_snapshot::Error::ReproductionMismatch {
                subject: "previous".to_owned(),
                reason: format!(
                    "the published snapshot follows {follows}, and --previous names {} at sequence \
                     {}",
                    previous.inscription_digest, previous.sequence,
                ),
            }),
        });
    }

    snapshot
        .verify_reproduction(published, plan)
        .map_err(Error::classified(
            "reproducing the published snapshot from the local stores",
            reproduction_differs,
        ))
}

/// One profile's repository under a network root: a registry repository or an
/// image layout on disk.
enum Side {
    Directory(SnapshotDir),
    Repository(Registry),
}

impl Side {
    fn open(
        config: &RootConfig,
        source: &Source,
        insecure: bool,
        auth: Auth,
    ) -> Result<Self, dolos_snapshot::Error> {
        match source {
            Source::Dir(path) => Ok(Self::Directory(SnapshotDir::open(path)?)),
            Source::Repo(repository) => Ok(Self::Repository(registry::open(
                repository,
                insecure,
                auth,
                node::scratch_dir(&config.storage, None),
                Tuning::default(),
            )?)),
        }
    }

    /// The inscription `profile` holds at `sequence` here, or `None`. Its tip
    /// is not read: a repository may no longer keep an older sequence's.
    fn inscription(
        &self,
        profile: &dyn Profile,
        sequence: u64,
    ) -> Result<Option<Inscription>, stelae::Error> {
        let pulled = match self {
            Self::Directory(dir) => dir.pull_inscription(profile, sequence),
            Self::Repository(registry) => registry.pull_inscription(profile, sequence),
        };
        match pulled {
            Ok(inscription) => Ok(Some(inscription)),
            Err(error) if error.is_absent() => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// A network root's snapshots, opened.
pub struct Published {
    root: Source,
    dolos: Side,
    insecure: bool,
    auth: Auth,
}

impl Published {
    /// Open the `dolos` repository under the network root `source`. A
    /// registry stages blobs under the node's default scratch directory; a
    /// layout is read in place.
    pub fn open(
        config: &RootConfig,
        source: &Source,
        insecure: bool,
        auth: Auth,
    ) -> Result<Self, Error> {
        let dolos = source
            .profile(DOLOS_REPOSITORY)
            .and_then(|dolos| Side::open(config, &dolos, insecure, auth.clone()))
            .map_err(Error::profile("opening the snapshot"))?;
        Ok(Self {
            root: source.clone(),
            dolos,
            insecure,
            auth,
        })
    }

    /// Read the snapshot's documents and its chain's, without pulling a layer.
    pub fn inspect(&self, point: Point) -> Result<Inspected, Error> {
        let inspected = match &self.dolos {
            Side::Repository(registry) => registry::inspect(registry, point),
            Side::Directory(dir) => inspect_dir(dir, point),
        };

        inspected.map_err(Error::profile("reading the snapshot"))
    }

    /// The inscription `latest` names, or `None` where the repository holds
    /// no `latest`. Only its own documents are read, not its chain's.
    pub fn latest_inscription(&self) -> Result<Option<Inscription>, Error> {
        let pulled = match &self.dolos {
            Side::Repository(registry) => registry
                .pull_latest(&DolosProfile)
                .and_then(|snapshot| snapshot.read_inscription()),
            Side::Directory(dir) => dir
                .pull_latest(&DolosProfile)
                .and_then(|snapshot| snapshot.read_inscription()),
        };
        match pulled {
            Ok(inscription) => Ok(Some(inscription)),
            Err(error) if error.is_absent() => Ok(None),
            Err(error) => Err(Error::profile("reading the latest snapshot")(error.into())),
        }
    }

    /// Check the snapshot's digests: its chain, its documents against each
    /// other, then every blob of its layer set against both of its digests.
    ///
    /// Both sources' failures are classified at one place, so what the
    /// directory tests hold a refusal to holds for a registry's too.
    pub fn verify(&self, point: Point) -> Result<Verified, Error> {
        let verified = match &self.dolos {
            Side::Repository(registry) => registry::verify(registry, point),
            Side::Directory(dir) => verify_dir(dir, point),
        };

        verified.map_err(Error::verifying("verifying the snapshot"))
    }

    /// The link a publish of `plan` here carries: to the snapshot before its
    /// sequence, or `None` where there is none, which starts a lineage.
    pub fn previous(&self, plan: &Plan) -> Result<Option<Link>, Error> {
        let Some(sequence) = plan.sequence.checked_sub(1) else {
            return Ok(None);
        };
        let what = format!("reading the snapshot at sequence {sequence}");
        match self.dolos.inscription(&DolosProfile, sequence) {
            Ok(Some(previous)) => Ok(Following::new(&previous, plan)
                .map_err(Error::profile(what))?
                .previous()),
            Ok(None) => Ok(None),
            Err(error) => Err(Error::profile(what)(error.into())),
        }
    }

    /// The blocks snapshot a publish of `plan` here names: the one at its
    /// sequence in the root's `blocks` repository, refused where a publish
    /// refuses it.
    pub fn blocks(&self, config: &RootConfig, plan: &Plan) -> Result<Digest, Error> {
        let what = format!("reading the blocks snapshot at sequence {}", plan.sequence);
        let source = self
            .root
            .profile(BLOCKS_REPOSITORY)
            .map_err(Error::profile(what.clone()))?;
        let held = Side::open(config, &source, self.insecure, self.auth.clone())
            .and_then(|blocks| Ok(blocks.inscription(&BlocksProfile, plan.sequence)?))
            .map_err(Error::profile(what))?;

        let repository = source.to_string();
        match held {
            Some(held) => Ok(blocks_reference(&repository, &held, plan)?),
            None => Err(Error::BlocksAbsent {
                repository,
                sequence: plan.sequence,
            }),
        }
    }
}

/// The documents a layout holds at `point`, resolved through its chain.
fn inspect_dir(dir: &SnapshotDir, point: Point) -> Result<Inspected, dolos_snapshot::Error> {
    let snapshot = resolve_dir(dir, point)?;
    let chain = chain_of(&snapshot)?;
    let blobs = snapshot.blob_index()?;
    let compressed = chain
        .descriptors()
        .map(|descriptor| snapshot.compressed_size(&blobs, descriptor))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Inspected {
        identity: chain.identity(),
        total_compressed: compressed.iter().flatten().sum(),
        inscription: snapshot.read_inscription()?,
        chain,
        compressed,
    })
}

/// The profile's repository verification over a layout: the chain resolved,
/// each layer of its set streamed under the profile's record ceiling, held
/// against its descriptor by the reader, and its bytes against the blob name
/// that places it.
fn verify_dir(dir: &SnapshotDir, point: Point) -> Result<Verified, dolos_snapshot::Error> {
    let snapshot = resolve_dir(dir, point)?;
    let chain = chain_of(&snapshot)?;
    let blobs = snapshot.blob_index()?;
    let limits = Limits {
        max_record: dolos_snapshot::MAX_RECORD,
        ..Limits::default()
    };

    let mut compressed_bytes = 0u64;
    for descriptor in chain.descriptors() {
        let check = || -> Result<u64, stelae::Error> {
            let digests = snapshot
                .stream_layer(&blobs, &DolosProfile, descriptor, limits)
                .and_then(|reader| reader.finish())?;
            if let Some(named) = blobs.blob_for(&descriptor.diff_id) {
                if digests.blob_digest != named {
                    return Err(stelae::Error::ManifestMismatch(format!(
                        "the manifest names this layer's blob {named} and its bytes hash to {}",
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
        identity: chain.identity(),
        inscription: snapshot.read_inscription()?,
        chain,
        compressed_bytes,
    })
}

fn resolve_dir(
    dir: &SnapshotDir,
    point: Point,
) -> Result<stelae::dir::Snapshot, dolos_snapshot::Error> {
    Ok(match point {
        Point::Latest => dir.resolve_latest(&DolosProfile)?,
        Point::Epoch(epoch) => dir.resolve_sequence(&DolosProfile, epoch)?,
    })
}

/// The chain a resolve carries, which a pull of one sequence does not.
fn chain_of(snapshot: &stelae::dir::Snapshot) -> Result<stelae::Chain, dolos_snapshot::Error> {
    snapshot.chain().cloned().ok_or_else(|| {
        dolos_snapshot::Error::Stelae(stelae::Error::Layout(
            "a resolved snapshot carries no chain".to_owned(),
        ))
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
    use std::path::PathBuf;

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

        // A registry or a staging directory that failed is not the snapshot.
        for source in [
            stelae::Error::Registry(std::io::Error::from(std::io::ErrorKind::StorageFull).into()),
            stelae::Error::Scratch {
                dir: PathBuf::from("/scratch"),
                source: std::io::ErrorKind::PermissionDenied.into(),
            },
        ] {
            assert!(!found_wrong(&layer(source)));
        }

        let error = layer(stelae::Error::DigestMismatch {
            subject: "blob".to_owned(),
            expected: "a".to_owned(),
            actual: "b".to_owned(),
        });
        assert!(found_wrong(&error));
        assert!(Error::verifying("verifying")(error).is_mismatch());

        // A registry that served other bytes than the digest it was asked for.
        let served = layer(stelae::Error::Registry(
            oci_client::errors::DigestError::VerificationError {
                expected: "sha256:aa".to_owned(),
                actual: "sha256:bb".to_owned(),
            }
            .into(),
        ));
        assert!(found_wrong(&served));
        assert!(Error::verifying("verifying")(served).is_mismatch());
    }

    #[test]
    fn a_reproduction_differs_only_where_it_compared() {
        let differs = dolos_snapshot::Error::ReproductionMismatch {
            subject: "sequence".to_owned(),
            reason: "another".to_owned(),
        };
        assert!(reproduction_differs(&differs));
        assert!(reproduction_differs(
            &dolos_snapshot::Error::NetworkMismatch {
                expected: 1,
                found: 2,
            }
        ));

        // What the export raises over records it read from the local stores.
        for error in [
            dolos_snapshot::Error::OutOfOrder {
                kind: "blocks",
                reason: "a slot went backwards".to_owned(),
            },
            dolos_snapshot::Error::Stelae(stelae::Error::RecordTooLarge {
                offset: 0,
                required: 2,
                limit: 1,
            }),
        ] {
            assert!(found_wrong(&error));
            assert!(!reproduction_differs(&error));
            let classified = Error::classified("reproducing", reproduction_differs)(error);
            assert!(!classified.is_mismatch());
        }
    }
}
