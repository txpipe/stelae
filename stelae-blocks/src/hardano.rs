//! A blocks snapshot built from the Haskell node's immutable files.
//!
//! An epoch is a whole number of immutable files: file `n` holds the slots
//! `n * width .. (n + 1) * width`, the width coming from the network's
//! genesis. The epoch's files are read in order, each block decoded only as
//! far as its header's slot and hash, and its body kept as the file's bytes.
//! Concatenated, one file's bodies are that file's `.chunk`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::RangeInclusive,
    path::Path,
};

use minicbor::Decoder;
use pallas_hardano::storage::immutable::chunk;
use pallas_traverse::MultiEraHeader;
use stelae::{inscription::Inscription, Digest, SnapshotWriter};
use stelae_driver::{place_layers, Predecessor};

use crate::{
    compression, parameters, position, BlockRecord, BlocksProfile, BlocksWriter, EpochScope, Error,
    Position, BLOCKS, COMPRESSION_LEVEL,
};

/// The three files the node writes per immutable file number.
const EXTENSIONS: [&str; 3] = ["chunk", "primary", "secondary"];

/// One closed epoch, and where its immutable files are.
#[derive(Debug, Clone, Copy)]
pub struct Epoch<'a> {
    pub immutable_dir: &'a Path,
    /// The network, the epoch and its inclusive slot bounds.
    pub scope: EpochScope,
    /// Slots per immutable file, from genesis (`10 * k`).
    pub slots_per_file: u64,
}

/// A sealed blocks snapshot.
#[derive(Debug, Clone)]
pub struct Sealed {
    pub inscription: Inscription,
    /// The snapshot's identity: the digest of its canonical inscription.
    pub digest: Digest,
}

impl Epoch<'_> {
    /// The numbers of the immutable files the epoch covers, refused when its
    /// bounds do not fall on file boundaries.
    pub fn files(&self) -> Result<RangeInclusive<u64>, Error> {
        let EpochScope {
            epoch,
            start_slot,
            end_slot,
            ..
        } = self.scope;
        let width = self.slots_per_file;
        let malformed = |why: String| Error::MalformedScope(format!("epoch {epoch}: {why}"));

        if width == 0 {
            return Err(malformed("an immutable file covers no slots".into()));
        }

        let end = end_slot
            .checked_add(1)
            .ok_or_else(|| malformed(format!("end slot {end_slot} has no successor")))?;

        if start_slot % width != 0 || end % width != 0 {
            return Err(malformed(format!(
                "slots {start_slot}..={end_slot} are not a whole number of {width}-slot \
                 immutable files"
            )));
        }

        Ok(start_slot / width..=end_slot / width)
    }

    /// Refuse a directory that lacks a file of the epoch, skips a number
    /// inside it, or holds nothing after its last file: the newest file is
    /// the one a node may still be appending to.
    pub fn check_files(&self) -> Result<(), Error> {
        let files = self.files()?;
        let present = numbered_files(self.immutable_dir)?;
        let numbers: BTreeSet<u64> = present.keys().copied().collect();

        for file in files.clone() {
            let Some(extensions) = present.get(&file) else {
                let after = numbers.range(..file).next_back();
                let next = numbers.range(file..).next();

                return Err(match (after, next) {
                    (Some(&after), Some(&next)) => Error::FileGap { after, next },
                    _ => Error::MissingFile {
                        file,
                        path: file_path(self.immutable_dir, file, EXTENSIONS[0]),
                    },
                });
            };

            if let Some(extension) = EXTENSIONS.iter().find(|ext| !extensions.contains(**ext)) {
                return Err(Error::MissingFile {
                    file,
                    path: file_path(self.immutable_dir, file, extension),
                });
            }
        }

        let last = *files.end();

        if numbers.range(last + 1..).next().is_none() {
            return Err(Error::OpenFile { file: last });
        }

        Ok(())
    }

    /// The epoch's blocks in chain order, after [`Epoch::check_files`].
    pub fn blocks(&self) -> Result<impl Iterator<Item = Result<BlockRecord, Error>> + '_, Error> {
        self.check_files()?;

        Ok(self.files()?.flat_map(move |file| {
            let opened = chunk::read_blocks(self.immutable_dir, &file_name(file))
                .map_err(|e| immutable(file, e));

            let blocks: Box<dyn Iterator<Item = _>> = match opened {
                Ok(blocks) => Box::new(blocks.map(move |body| {
                    body.map_err(|e| immutable(file, e))
                        .and_then(|body| block_record(file, body))
                })),
                Err(e) => Box::new(std::iter::once(Err(e))),
            };

            blocks
        }))
    }
}

/// Write the epoch's `blocks` layer and seal the snapshot at the sequence
/// whose cut it closes, epoch + 1, linked as `predecessor` says.
///
/// The snapshot at sequence 1 has no link. A lineage that starts later would
/// have to list every earlier epoch, which one epoch's files cannot give, so
/// a later snapshot must follow the one before it.
pub fn export<W: SnapshotWriter>(
    writer: &W,
    predecessor: &dyn Predecessor,
    epoch: &Epoch<'_>,
) -> Result<Sealed, Error> {
    let scope = epoch.scope;
    let sequence = scope.epoch.checked_add(1).ok_or_else(|| Error::Lineage {
        sequence: scope.epoch,
        reason: "the epoch has no successor".into(),
    })?;
    let previous = predecessor.previous();

    let lineage = |reason: String| Error::Lineage { sequence, reason };

    match previous {
        None if sequence > 1 => {
            return Err(lineage(
                "it starts no lineage, so it must follow the snapshot at the sequence before it"
                    .into(),
            ))
        }
        Some(link) if link.sequence + 1 != sequence => {
            return Err(lineage(format!(
                "it would follow sequence {}, not the one before it",
                link.sequence
            )))
        }
        _ => {}
    }

    let spec = scope.layer_spec()?;

    let descriptor = match predecessor.resume(BLOCKS, &spec.scope)? {
        Some(resumed) => resumed,
        None => {
            let mut layer = BlocksWriter::open(writer, scope, COMPRESSION_LEVEL)?;

            for block in epoch.blocks()? {
                layer.push(&block?)?;
            }

            layer.finish()?.descriptor
        }
    };

    predecessor.landed(&descriptor)?;

    let mut inscription = Inscription::new(
        &BlocksProfile,
        sequence,
        position(&Position {
            network_magic: scope.network_magic,
            epoch: sequence,
        }),
        parameters(),
        compression(),
    );
    inscription.previous = previous;
    place_layers(&BlocksProfile, &mut inscription, [descriptor])?;
    inscription.validate()?;

    let digest = writer.seal(&BlocksProfile, &inscription)?;

    Ok(Sealed {
        inscription,
        digest,
    })
}

/// A block's record: the slot and hash its header gives, and the bytes the
/// file stores for it.
///
/// A stored block is `[era, [header, ..]]`. Only the header is decoded; the
/// rest of the block is never parsed.
pub fn block_record(file: u64, body: Vec<u8>) -> Result<BlockRecord, Error> {
    let malformed = |reason: String| Error::MalformedBlock { file, reason };
    let cbor = |e: minicbor::decode::Error| malformed(e.to_string());

    let mut decoder = Decoder::new(&body);

    if decoder.array().map_err(cbor)? != Some(2) {
        return Err(malformed("not an [era, block] pair".into()));
    }

    let era = decoder.u16().map_err(cbor)?;

    // The header decoder's numbering: Byron with a subtag telling a boundary
    // block (0) from a main block (1), then each later era one below the
    // block's own tag.
    let (tag, subtag) = match era {
        0 => (0, Some(0)),
        1 => (0, Some(1)),
        2..=7 => (era as u8 - 1, None),
        other => return Err(malformed(format!("unknown era tag {other}"))),
    };

    decoder.array().map_err(cbor)?;
    let start = decoder.position();
    decoder.skip().map_err(cbor)?;
    let header = &body[start..decoder.position()];

    let header =
        MultiEraHeader::decode(tag, subtag, header).map_err(|e| malformed(e.to_string()))?;
    let (slot, hash) = (header.slot(), *header.hash());

    Ok(BlockRecord::new(slot, hash, body))
}

/// Every numbered file in `dir`, by number, with the extensions present.
fn numbered_files(dir: &Path) -> Result<BTreeMap<u64, BTreeSet<String>>, Error> {
    let unreadable = |e: std::io::Error| {
        Error::Stelae(stelae::Error::Io(std::io::Error::new(
            e.kind(),
            format!("listing {}: {e}", dir.display()),
        )))
    };

    let mut files: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();

    for entry in fs::read_dir(dir).map_err(unreadable)? {
        let name = entry.map_err(unreadable)?.file_name();
        let Some((stem, extension)) = name.to_str().and_then(|name| name.split_once('.')) else {
            continue;
        };

        if !EXTENSIONS.contains(&extension) || !stem.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }

        if let Ok(number) = stem.parse() {
            files
                .entry(number)
                .or_default()
                .insert(extension.to_owned());
        }
    }

    Ok(files)
}

fn file_name(file: u64) -> String {
    format!("{file:05}")
}

fn file_path(dir: &Path, file: u64, extension: &str) -> std::path::PathBuf {
    dir.join(format!("{}.{extension}", file_name(file)))
}

fn immutable(file: u64, error: chunk::Error) -> Error {
    Error::Immutable {
        file,
        reason: error.to_string(),
    }
}
