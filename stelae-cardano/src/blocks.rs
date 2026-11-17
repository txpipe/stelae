//! The blocks snapshot each Dolos snapshot names, published by the host.
//!
//! At the boundary that opens epoch N, the host exports epoch N−1 from its
//! immutable files with the blocks profile and seals it at sequence N in the
//! network root's `blocks` repository, before the Dolos snapshot there is
//! prepared or published. A blocks snapshot the repository already holds at N
//! is reproduced from the same files and compared by inscription digest, as a
//! Dolos snapshot is, and a difference refuses.

use std::path::PathBuf;

use dolos_snapshot::export::Plan;
use stelae::{
    inscription::{link_for, Link},
    oci::Registry,
    Digest,
};
use stelae_blocks::{
    hardano::{self, Epoch},
    BlocksProfile, EpochScope, Position,
};
use stelae_driver::{
    publish::{Chained, Origin, Publishing},
    Predecessor,
};
use tracing::{info, warn};

use crate::{
    backfill,
    snapshot::{blocks_reference, BlocksRefusal},
};

/// Where the immutable files of the epoch a blocks snapshot adds are.
#[derive(Debug, Clone)]
pub struct EpochFiles {
    pub dir: PathBuf,
    pub scope: EpochScope,
    /// Slots per immutable file, from genesis.
    pub slots_per_file: u64,
}

impl EpochFiles {
    pub fn epoch(&self) -> Epoch<'_> {
        Epoch {
            immutable_dir: &self.dir,
            scope: self.scope,
            slots_per_file: self.slots_per_file,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "the blocks repository {repository} holds a blocks snapshot at epoch-{sequence} that \
         epoch {epoch}'s immutable files do not reproduce: it holds {held}, and they give \
         {reproduced}. A Dolos snapshot names only a blocks snapshot this host reproduces, so \
         nothing was published at this sequence"
    )]
    Differs {
        repository: String,
        sequence: u64,
        epoch: u64,
        held: Digest,
        reproduced: Digest,
    },

    #[error(transparent)]
    Refused(#[from] BlocksRefusal),

    #[error(
        "the immutable files of epoch {epoch} in {dir} can't give its blocks snapshot: {source}"
    )]
    Files {
        epoch: u64,
        dir: PathBuf,
        #[source]
        source: Box<stelae_blocks::Error>,
    },

    /// The epoch's files or the repository's lineage can't give the snapshot,
    /// or its upload failed.
    #[error("building the blocks snapshot at epoch-{sequence} for {repository}: {source}")]
    Build {
        repository: String,
        sequence: u64,
        #[source]
        source: Box<stelae_blocks::Error>,
    },

    #[error("reading the blocks repository {repository} at epoch-{sequence}: {source}")]
    Repository {
        repository: String,
        sequence: u64,
        #[source]
        source: Box<stelae::Error>,
    },
}

impl Error {
    /// What trying again can't change: a held snapshot this host doesn't
    /// reproduce, and files or a lineage the snapshot can't be built from.
    pub fn is_refusal(&self) -> bool {
        match self {
            Self::Differs { .. } | Self::Refused(_) | Self::Files { .. } => true,
            Self::Build { source, .. } => !matches!(
                **source,
                stelae_blocks::Error::Stelae(_) | stelae_blocks::Error::Driver(_)
            ),
            Self::Repository { .. } => false,
        }
    }
}

/// The epoch the blocks snapshot at `plan`'s sequence adds, the one its cut
/// closes, with the bounds the plan gives it.
pub fn closed_epoch(plan: &Plan) -> Result<EpochScope, stelae_blocks::Error> {
    let window = plan
        .sequence
        .checked_sub(1)
        .and_then(|epoch| plan.epochs.get(epoch as usize))
        .filter(|window| window.epoch + 1 == plan.sequence)
        .ok_or_else(|| stelae_blocks::Error::Lineage {
            sequence: plan.sequence,
            reason: "the plan closes no epoch at its cut".into(),
        })?;

    EpochScope::new(
        plan.network.magic(),
        window.epoch,
        window.start_slot,
        window.end_slot,
    )
}

/// The link a reproduction carries, resuming nothing.
struct Follows(Option<Link>);

impl Predecessor for Follows {
    fn previous(&self) -> Option<Link> {
        self.0
    }
}

/// The inscription digest of the blocks snapshot at `plan`'s sequence in
/// `registry`, which the Dolos snapshot there names.
///
/// One the repository holds is reproduced from `files`, following the
/// repository's sequence before it, and kept only when the digests agree.
/// Otherwise one is exported from `files` and sealed after the repository's
/// latest, which has to be the sequence before.
pub fn ensure(
    registry: &Registry,
    plan: &Plan,
    files: &dyn Fn() -> Result<EpochFiles, backfill::Error>,
) -> Result<Digest, backfill::Error> {
    let repository = registry.repository().to_string();
    let sequence = plan.sequence;
    let read = |source| Error::Repository {
        repository: repository.clone(),
        sequence,
        source: Box::new(source),
    };
    let build = |source| Error::Build {
        repository: repository.clone(),
        sequence,
        source: Box::new(source),
    };

    let held = match registry.pull_inscription(&BlocksProfile, sequence) {
        Ok(held) => Some(blocks_reference(&repository, &held, plan).map_err(Error::from)?),
        Err(error) if error.is_absent() => None,
        Err(error) => return Err(read(error).into()),
    };
    let files = files()?;
    let epoch = files.epoch();

    let Some(held) = held else {
        let latest = registry.latest(&BlocksProfile).map_err(read)?;
        let magic = plan.network.magic();
        let position = stelae_blocks::position(&Position {
            network_magic: magic,
            epoch: sequence,
        });
        let origin = Origin::of(
            registry,
            magic,
            stelae_blocks::parameters(),
            stelae_blocks::compression(),
        );
        let chained = Chained::new(
            &BlocksProfile,
            Publishing::new(registry),
            latest.as_ref(),
            sequence,
            &position,
            origin,
        )
        .map_err(|error| build(error.into()))?;
        let sealed = hardano::export(registry, &chained, &epoch).map_err(build)?;
        info!(sequence, digest = %sealed.digest, %repository, "published the blocks snapshot");
        return Ok(sealed.digest);
    };

    let previous = match sequence.checked_sub(1).filter(|before| *before > 0) {
        None => None,
        Some(before) => match registry.pull_inscription(&BlocksProfile, before) {
            // A tag at `before` naming another sequence is no link to follow.
            Ok(previous) => link_for(Some(&previous), sequence).map_err(|error| {
                build(stelae_blocks::Error::Lineage {
                    sequence,
                    reason: error.to_string(),
                })
            })?,
            Err(error) if error.is_absent() => None,
            Err(error) => return Err(read(error).into()),
        },
    };
    warn!(
        sequence,
        %held,
        "the blocks repository already holds this sequence: reproducing it to compare"
    );
    let reproduced = hardano::export(&stelae::Discarding, &Follows(previous), &epoch)
        .map_err(build)?
        .digest;
    if reproduced != held {
        return Err(Error::Differs {
            repository,
            sequence,
            epoch: files.scope.epoch,
            held,
            reproduced,
        }
        .into());
    }
    Ok(held)
}
