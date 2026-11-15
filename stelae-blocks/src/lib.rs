//! # The `io.txpipe.cardano.blocks` profile
//!
//! Raw Cardano blocks, one layer per epoch, for any client: a layer carries
//! each block's slot, header hash and body as the node stores it, and nothing
//! a particular node derives from them. The profile document is
//! [`PROFILE.md`](https://github.com/txpipe/stelae/blob/main/stelae-blocks/PROFILE.md).
//!
//! ## Module map
//!
//! - [`record`] — the `blocks` record codec and its order rule.
//! - [`scope`] — the epoch scope a layer carries, in both of its encodings.
//! - [`layer`] — writing a layer from records and reading one back.
//! - `hardano` (feature `hardano`) — an epoch's blocks read out of the Haskell
//!   node's immutable files, sealed as a blocks snapshot.
//!
//! Every layer is final once its epoch has closed, so every layer goes in an
//! inscription's `diff` and a blocks snapshot has no tip.

#[cfg(feature = "hardano")]
pub mod hardano;
pub mod layer;
pub mod record;
pub mod scope;

use std::{ops::Range, path::PathBuf};

use serde_json::json;
use stelae::{inscription::Inscription, Compression, Profile};

pub use layer::{write_layer, BlocksReader, BlocksWriter};
pub use record::{BlockRecord, OrderCheck};
pub use scope::EpochScope;

/// Reverse-DNS name of this profile.
pub const PROFILE_NAME: &str = "io.txpipe.cardano.blocks";

/// Major version of the profile this crate implements.
pub const PROFILE_VERSION: u64 = 1;

/// The profile's one layer kind.
pub const BLOCKS: &str = "blocks";

/// Every kind this profile defines.
pub const KINDS: [&str; 1] = [BLOCKS];

/// Payload media type of a `blocks` layer.
pub const BLOCKS_MEDIA_TYPE: &str = "application/vnd.txpipe.stelae.blocks.v1+zstd";

/// Compression algorithm the profile publishes under. Identity never depends
/// on it: a layer is named by its uncompressed digest.
pub const COMPRESSION_ALGO: &str = "zstd";

/// zstd level the profile publishes at, so publishers converge on one blob.
pub const COMPRESSION_LEVEL: i32 = 9;

/// Errors raised by the profile.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Stelae(#[from] stelae::Error),

    #[error(transparent)]
    Driver(#[from] stelae_driver::Error),

    #[error("malformed blocks record: {0}")]
    MalformedRecord(String),

    #[error("blocks are out of order: {0}")]
    OutOfOrder(String),

    /// A block whose slot the layer's epoch does not cover.
    #[error("slot {slot} is outside epoch {epoch}, which spans slots {start_slot}..={end_slot}")]
    OutsideEpoch {
        slot: u64,
        epoch: u64,
        start_slot: u64,
        end_slot: u64,
    },

    #[error("malformed blocks scope: {0}")]
    MalformedScope(String),

    #[error("the inscription's {field} is not the shape this profile writes: {reason}")]
    MalformedInscription { field: String, reason: String },

    /// A snapshot whose link does not fit where it stands in its lineage.
    #[error("cannot seal the blocks snapshot at sequence {sequence}: {reason}")]
    Lineage { sequence: u64, reason: String },

    /// An immutable file the epoch covers is absent, whole or in part.
    #[error("immutable file {file:05} is missing: {path} does not exist")]
    MissingFile { file: u64, path: PathBuf },

    /// The directory skips file numbers inside the epoch.
    #[error("immutable files are not contiguous: {after:05} is followed by {next:05}")]
    FileGap { after: u64, next: u64 },

    /// The epoch's last file is the directory's newest, which a node may
    /// still be appending to.
    #[error("immutable file {file:05} may still be open: no later file follows it")]
    OpenFile { file: u64 },

    #[error("cannot read immutable file {file:05}: {reason}")]
    Immutable { file: u64, reason: String },

    #[error("malformed block in immutable file {file:05}: {reason}")]
    MalformedBlock { file: u64, reason: String },
}

impl Error {
    fn inscription(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::MalformedInscription {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

/// The blocks profile. Stateless: it names things and holds no chain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlocksProfile;

impl Profile for BlocksProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        PROFILE_VERSION
    }

    fn kinds(&self) -> &[&str] {
        &KINDS
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
        if kind != BLOCKS {
            return Err(stelae::Error::UnknownLayerKind {
                profile: PROFILE_NAME.to_owned(),
                kind: kind.to_owned(),
            });
        }

        Ok(BLOCKS_MEDIA_TYPE.to_owned())
    }

    /// Sequence N is the cut at the boundary that opens epoch N, tagged
    /// without zero padding.
    fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
        Ok(format!("epoch-{sequence}"))
    }
}

/// What the lifecycle asks: one dense kind, no tip, and every layer in `diff`,
/// because a closed epoch's blocks never change.
impl stelae_driver::DriverProfile for BlocksProfile {
    fn epoch_kinds(&self) -> &[&str] {
        &KINDS
    }

    fn dense_epoch_kinds(&self) -> &[&str] {
        &KINDS
    }

    fn is_state_kind(&self, _kind: &str) -> bool {
        false
    }

    fn role(
        &self,
        kind: &str,
        _scope: &serde_json::Value,
        _sequence: u64,
    ) -> Result<stelae_driver::Role, stelae_driver::Error> {
        match kind {
            BLOCKS => Ok(stelae_driver::Role::Diff),
            other => Err(stelae_driver::Error::Unplaced {
                kind: other.to_owned(),
                reason: format!("{PROFILE_NAME} has no such kind"),
            }),
        }
    }

    /// Two snapshots describe one dataset when their positions name the same
    /// network magic.
    fn check_same_dataset(
        &self,
        previous: &Inscription,
        position: &serde_json::Value,
    ) -> Result<(), stelae_driver::Error> {
        let magic = |value: &serde_json::Value| {
            read_position(value)
                .map(|p| p.network_magic)
                .map_err(|e| match e {
                    Error::MalformedInscription { field, reason } => {
                        stelae_driver::Error::MalformedInscription { field, reason }
                    }
                    other => stelae_driver::Error::MalformedInscription {
                        field: "position".to_owned(),
                        reason: other.to_string(),
                    },
                })
        };

        let found = magic(&previous.position)?;
        let expected = magic(position)?;

        if found != expected {
            return Err(stelae_driver::Error::DatasetMismatch { expected, found });
        }

        Ok(())
    }
}

/// Where a blocks snapshot stands: the network and the epoch its cut opens.
///
/// `epoch` equals the inscription's `sequence`; the snapshot holds the blocks
/// of every epoch before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub network_magic: u64,
    pub epoch: u64,
}

/// `position`: `{"networkMagic": m, "epoch": e}`.
pub fn position(position: &Position) -> serde_json::Value {
    json!({"networkMagic": position.network_magic, "epoch": position.epoch})
}

/// Read a `position` back, refusing any other shape.
pub fn read_position(value: &serde_json::Value) -> Result<Position, Error> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::inscription("position", "not an object"))?;

    if let Some(extra) = object
        .keys()
        .find(|key| !["networkMagic", "epoch"].contains(&key.as_str()))
    {
        return Err(Error::inscription(
            format!("position.{extra}"),
            "not a field of this profile",
        ));
    }

    let uint = |name: &str| {
        object
            .get(name)
            .ok_or_else(|| Error::inscription(format!("position.{name}"), "missing"))?
            .as_u64()
            .ok_or_else(|| Error::inscription(format!("position.{name}"), "not a u64"))
    };

    Ok(Position {
        network_magic: uint("networkMagic")?,
        epoch: uint("epoch")?,
    })
}

/// `parameters`: empty. The profile has nothing to configure.
pub fn parameters() -> serde_json::Value {
    json!({})
}

/// The compression this profile publishes under.
pub fn compression() -> Compression {
    Compression {
        algo: COMPRESSION_ALGO.to_owned(),
        level: COMPRESSION_LEVEL as i64,
    }
}

/// The epochs whose layers the inscription at `sequence` lists in `diff`.
///
/// A snapshot that extends a lineage adds the one epoch that closed at its
/// cut, `sequence - 1`. The first snapshot of a lineage lists every epoch
/// before its cut. Sequence 0 holds nothing and is not a snapshot: `None`.
pub fn diff_epochs(sequence: u64, starts_lineage: bool) -> Option<Range<u64>> {
    let last = sequence.checked_sub(1)?;

    Some(if starts_lineage {
        0..sequence
    } else {
        last..sequence
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use stelae_driver::DriverProfile;

    #[test]
    fn a_lineage_adds_one_epoch_per_snapshot() {
        assert_eq!(diff_epochs(0, true), None);
        assert_eq!(diff_epochs(0, false), None);
        assert_eq!(diff_epochs(1, true), Some(0..1));
        assert_eq!(diff_epochs(667, false), Some(666..667));
        assert_eq!(diff_epochs(5, true), Some(0..5));
    }

    #[test]
    fn position_round_trips_and_refuses_other_shapes() {
        let at = Position {
            network_magic: 764824073,
            epoch: 667,
        };

        assert_eq!(read_position(&position(&at)).unwrap(), at);

        for bad in [
            json!([]),
            json!({"epoch": 1}),
            json!({"networkMagic": 2, "epoch": "1"}),
            json!({"networkMagic": 2, "epoch": 1, "network": "preview"}),
        ] {
            let err = read_position(&bad).unwrap_err();
            assert!(
                matches!(err, Error::MalformedInscription { .. }),
                "{bad}: {err:?}"
            );
        }
    }

    #[test]
    fn a_predecessor_on_another_network_is_refused() {
        let previous = Inscription::new(
            &BlocksProfile,
            1,
            position(&Position {
                network_magic: 1,
                epoch: 1,
            }),
            parameters(),
            compression(),
        );

        let same = position(&Position {
            network_magic: 1,
            epoch: 2,
        });
        BlocksProfile.check_same_dataset(&previous, &same).unwrap();

        let other = position(&Position {
            network_magic: 2,
            epoch: 2,
        });
        let err = BlocksProfile
            .check_same_dataset(&previous, &other)
            .unwrap_err();
        assert!(
            matches!(
                err,
                stelae_driver::Error::DatasetMismatch {
                    expected: 2,
                    found: 1
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn every_layer_is_diff_and_none_is_tip() {
        let scope = json!({"epoch": 0, "startSlot": 0, "endSlot": 1});

        assert_eq!(
            BlocksProfile.role(BLOCKS, &scope, 1).unwrap(),
            stelae_driver::Role::Diff
        );
        assert!(matches!(
            BlocksProfile.role("state", &scope, 1),
            Err(stelae_driver::Error::Unplaced { .. })
        ));
        assert!(!BlocksProfile.is_state_kind(BLOCKS));
        assert_eq!(BlocksProfile.dense_epoch_kinds(), &[BLOCKS]);
    }
}
