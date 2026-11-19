//! The blocks lineage of an old Dolos lineage, re-encoded from its own
//! `blocks` layers and compared with Mithril (decision 0080, point 19).
//!
//! An old lineage's per-epoch `blocks` layers hold the records the blocks
//! profile carries, under a header naming the Dolos profile. [`reencode`]
//! streams one, writes its header again under the blocks profile and copies
//! every record byte, so only the blob changes. On the way past it hashes the
//! bodies of each immutable file and holds them to the `.chunk` digest of
//! Mithril's published list. [`run`] does this for every closed epoch of the
//! newest `epoch-N`, sealing epoch E at sequence E + 1 and writing a journal
//! line per sequence.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    ops::RangeInclusive,
    path::PathBuf,
};

use dolos_snapshot::DolosProfile;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use stelae::{
    frame::Limits,
    manifest::Descriptor,
    oci::Registry,
    transport::{Discarding, SnapshotWriter, WrittenLayer},
    CanonicalCbor, Digest, LayerDescriptor, LayerReader, Link, Profile as _, RecordSink as _,
    SnapshotReader as _,
};
use stelae_blocks::{record, BlocksProfile, EpochScope, OrderCheck, COMPRESSION_LEVEL};

use super::{read_journal, sequences, OldSequence};
use crate::Repository;
use crate::{
    backfill::slots_per_immutable_file, config::without_userinfo, publisher::blocks_repository,
    Auth, Genesis,
};

/// The aggregator route serving the digest list of the latest Cardano
/// database snapshot.
pub const DIGESTS_ROUTE: &str = "artifact/cardano-database/digests";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Old(#[from] super::Error),

    #[error(
        "epoch-{sequence} lists blocks layers for epochs {found:?}, not each epoch before it once"
    )]
    Epochs { sequence: u64, found: Vec<u64> },

    #[error("re-encoding the blocks layer of epoch {epoch}")]
    Layer {
        epoch: u64,
        #[source]
        source: Box<stelae_blocks::Error>,
    },

    #[error(
        "refused epoch {epoch}: its blocks do not match Mithril's digests: {}",
        .files.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
    )]
    Mithril {
        epoch: u64,
        files: Vec<FileMismatch>,
    },

    #[error("Mithril's digest list {0}")]
    Digests(String),

    #[error("the blocks journal {0}")]
    Journal(String),

    #[error("{what} {repository}")]
    Target {
        repository: String,
        what: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("writing the blocks journal")]
    Write(#[source] std::io::Error),
}

/// An immutable file whose blocks Mithril does not certify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileMismatch {
    Differs {
        file: u64,
        mithril: Digest,
        found: Digest,
    },
    Uncertified {
        file: u64,
    },
}

impl std::fmt::Display for FileMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Differs {
                file,
                mithril,
                found,
            } => write!(
                f,
                "immutable file {file:05}: Mithril certifies {mithril}, its blocks hash to {found}"
            ),
            Self::Uncertified { file } => {
                write!(
                    f,
                    "immutable file {file:05}: Mithril's list has no digest for it"
                )
            }
        }
    }
}

/// The `.chunk` digests of Mithril's digest list, by immutable file number.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChunkDigests(BTreeMap<u64, Digest>);

impl ChunkDigests {
    /// Read the aggregator's list: `[{"immutable_file_name", "digest"}]`,
    /// the digest in hex. Index-file entries are skipped.
    pub fn parse(json: &[u8]) -> Result<Self, Error> {
        #[derive(Deserialize)]
        struct Item {
            immutable_file_name: String,
            digest: String,
        }

        let items: Vec<Item> = serde_json::from_slice(json)
            .map_err(|error| Error::Digests(format!("is not a list of digests: {error}")))?;
        let mut digests = BTreeMap::new();

        for item in items {
            let Some(stem) = item.immutable_file_name.strip_suffix(".chunk") else {
                continue;
            };
            let refused = || Error::Digests(format!("names {:?}", item.immutable_file_name));
            let file: u64 = match stem.bytes().all(|b| b.is_ascii_digit()) {
                true => stem.parse().map_err(|_| refused())?,
                false => return Err(refused()),
            };
            let digest: Digest = format!("sha256:{}", item.digest)
                .parse()
                .map_err(|_| Error::Digests(format!("gives {:?} for {stem}", item.digest)))?;

            if digests
                .insert(file, digest)
                .is_some_and(|other| other != digest)
            {
                return Err(Error::Digests(format!("gives two digests for {stem}")));
            }
        }

        Ok(Self(digests))
    }

    pub fn get(&self, file: u64) -> Option<&Digest> {
        self.0.get(&file)
    }
}

/// Fetch the digest list from `aggregator`'s [`DIGESTS_ROUTE`].
pub fn fetch(aggregator: &str) -> Result<ChunkDigests, Error> {
    let url = format!("{}/{DIGESTS_ROUTE}", aggregator.trim_end_matches('/'));
    let shown = without_userinfo(&url);
    let failed =
        |error: reqwest::Error| Error::Digests(format!("at {shown}: {}", error.without_url()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Digests(format!("needs a runtime: {error}")))?;
    let body = runtime
        .block_on(async { reqwest::get(&url).await?.error_for_status()?.bytes().await })
        .map_err(failed)?;

    ChunkDigests::parse(&body)
}

/// One epoch's layer under the blocks profile.
#[derive(Debug, Clone)]
pub struct Reencoded {
    pub layer: WrittenLayer,
    /// Immutable files compared with Mithril.
    pub files: u64,
}

/// Re-encode the old `blocks` layer `old`, its blob in `source`, onto
/// `writer`, refusing it when a file's bodies are not Mithril's `.chunk`.
///
/// The old layer is proven against its descriptor before the new one is
/// finished, so a refused or corrupt layer leaves nothing on `writer`.
pub fn reencode<R: Read, W: SnapshotWriter>(
    source: R,
    old: &LayerDescriptor,
    network_magic: u64,
    slots_per_file: u64,
    digests: &ChunkDigests,
    writer: &W,
) -> Result<Reencoded, Error> {
    let epoch = old
        .scope
        .get("epoch")
        .and_then(|v| v.as_u64())
        .unwrap_or_default();
    let refused = |source: stelae_blocks::Error| Error::Layer {
        epoch,
        source: Box::new(source),
    };
    let stelae = |source: stelae::Error| refused(source.into());

    let limits = Limits {
        max_record: DolosProfile.max_record(),
        ..Limits::default()
    };
    let mut reader = LayerReader::new(source, &DolosProfile, old, limits).map_err(stelae)?;
    let scope = EpochScope::read_header(reader.header().scope.as_bytes()).map_err(refused)?;
    let described = EpochScope::read_descriptor(network_magic, &old.scope).map_err(refused)?;
    if scope != described {
        return Err(refused(stelae_blocks::Error::MalformedScope(format!(
            "the header scope {scope:?} is not the descriptor's on network {network_magic}"
        ))));
    }

    let mut chunks = Chunks::new(scope.files(slots_per_file).map_err(refused)?, digests);
    let spec = scope.layer_spec().map_err(refused)?;
    let mut sink = writer
        .layer_sink(&BlocksProfile, &spec, COMPRESSION_LEVEL)
        .map_err(stelae)?;
    let mut order = OrderCheck::default();

    while let Some(bytes) = reader.next_record() {
        let bytes = bytes.map_err(stelae)?;
        let block = record::decode(bytes).map_err(refused)?;

        if !scope.covers(block.slot) {
            return Err(refused(stelae_blocks::Error::OutsideEpoch {
                slot: block.slot,
                epoch: scope.epoch,
                start_slot: scope.start_slot,
                end_slot: scope.end_slot,
            }));
        }
        order.check(block.slot).map_err(refused)?;
        chunks.push(block.slot / slots_per_file, &block.body);

        let record = CanonicalCbor::new(bytes.to_vec()).map_err(stelae)?;
        sink.write_record(&record).map_err(stelae)?;
    }

    reader.finish().map_err(stelae)?;

    let (files, mismatches) = chunks.finish();
    if !mismatches.is_empty() {
        return Err(Error::Mithril {
            epoch: scope.epoch,
            files: mismatches,
        });
    }

    Ok(Reencoded {
        layer: sink.finish().map_err(stelae)?,
        files,
    })
}

/// Each immutable file's bodies, hashed in turn and held to Mithril's list.
struct Chunks<'a> {
    files: RangeInclusive<u64>,
    next: u64,
    hasher: Sha256,
    digests: &'a ChunkDigests,
    mismatches: Vec<FileMismatch>,
}

impl<'a> Chunks<'a> {
    fn new(files: RangeInclusive<u64>, digests: &'a ChunkDigests) -> Self {
        Self {
            next: *files.start(),
            files,
            hasher: Sha256::new(),
            digests,
            mismatches: Vec::new(),
        }
    }

    /// A body of `file`. Slots never go backwards, so neither do files.
    fn push(&mut self, file: u64, body: &[u8]) {
        while self.next < file {
            self.close();
        }
        self.hasher.update(body);
    }

    fn close(&mut self) {
        let file = self.next;
        let found = Digest::from_bytes(std::mem::take(&mut self.hasher).finalize().into());

        match self.digests.get(file) {
            Some(mithril) if *mithril == found => {}
            Some(&mithril) => self.mismatches.push(FileMismatch::Differs {
                file,
                mithril,
                found,
            }),
            None => self.mismatches.push(FileMismatch::Uncertified { file }),
        }
        self.next += 1;
    }

    /// Close every file left, an empty one included, and count them.
    fn finish(mut self) -> (u64, Vec<FileMismatch>) {
        while self.next <= *self.files.end() {
            self.close();
        }
        (self.files.end() - self.files.start() + 1, self.mismatches)
    }
}

/// The repository whose blocks lineage is written, given upload mode.
#[derive(Debug, Clone)]
pub struct Upload {
    /// The network root; the lineage goes to its `blocks` repository.
    pub root: Repository,
    pub auth: Auth,
}

#[derive(Debug, Clone)]
pub struct Options {
    /// The old lineage's repository.
    pub source: Repository,
    pub auth: Auth,
    pub insecure: bool,
    /// Without it, nothing is written to a registry.
    pub upload: Option<Upload>,
    pub scratch_dir: Option<PathBuf>,
    /// The largest layer, in bytes, uploaded as one request; the transport's
    /// default when `None`.
    pub monolithic_max: Option<u64>,
}

/// What a run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    /// The sequences the old lineage gives, `1..=last`.
    pub last: u64,
    /// Sequences the journal already held, not re-encoded.
    pub resumed: u64,
    pub epochs: u64,
    /// Immutable files compared with Mithril, every one of them a match.
    pub files: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

/// Re-encode and seal every closed epoch of the old lineage in
/// `options.source` after those `journal` already holds, appending a line
/// per sequence to `append`.
pub fn run(
    options: &Options,
    genesis: &Genesis,
    digests: &ChunkDigests,
    journal: &str,
    append: &mut dyn Write,
) -> Result<Summary, Error> {
    let registry_options = |auth: &Auth| stelae::oci::Options {
        insecure: options.insecure,
        auth: auth.clone(),
        scratch_dir: options.scratch_dir.clone(),
        monolithic_max: options
            .monolithic_max
            .unwrap_or(stelae::oci::DEFAULT_MONOLITHIC_MAX),
        ..Default::default()
    };
    let repository = options.source.to_string();
    let read = |what: String, source: stelae::Error| super::Error::Read {
        repository: repository.clone(),
        what,
        source: Box::new(source),
    };

    let source = Registry::open(&options.source, registry_options(&options.auth))
        .map_err(|e| read("the repository".into(), e))?;
    let tags = source.tags().map_err(|e| read("its tags".into(), e))?;
    let last = *sequences(&repository, tags)?
        .last()
        .expect("refused when empty");
    let tag = format!("epoch-{last}");
    let document = source.pull_raw(&tag).map_err(|e| read(tag, e))?;
    let old = OldSequence::read(last, &document)?;
    let layers = closed_epochs(&old, &document.manifest.layers)?;

    let resumed = resumed(journal, last)?;
    let context = Context {
        source: &source,
        network_magic: u64::from(genesis.network_magic()),
        slots_per_file: slots_per_immutable_file(genesis),
        digests,
        layers: &layers,
    };

    match &options.upload {
        None => context.lineage(&Discarding, resumed, append),
        Some(upload) => {
            let target = blocks_repository(&upload.root)
                .map_err(|e| target_error(&upload.root, "naming the blocks repository of", e))?;
            let registry = Registry::open(&target, registry_options(&upload.auth))
                .map_err(|e| target_error(&target, "opening", e))?;
            check_target(&target, &registry, resumed)?;
            context.lineage(&registry, resumed, append)
        }
    }
}

/// The closed epochs' `blocks` layers of `old`, with the manifest layers
/// naming their blobs, by epoch: each epoch before its sequence once.
fn closed_epochs(
    old: &OldSequence,
    manifest: &[Descriptor],
) -> Result<Vec<(LayerDescriptor, Descriptor)>, Error> {
    let sequence = old.inscription.sequence;
    let mut layers: Vec<(u64, &LayerDescriptor)> = old
        .inscription
        .layers
        .iter()
        .filter(|layer| layer.kind == dolos_snapshot::BLOCKS)
        .filter_map(|layer| Some((layer.scope.get("epoch")?.as_u64()?, layer)))
        .filter(|(epoch, _)| *epoch < sequence)
        .collect();
    layers.sort_by_key(|(epoch, _)| *epoch);

    let epochs: Vec<u64> = layers.iter().map(|(epoch, _)| *epoch).collect();
    if !epochs.iter().copied().eq(0..sequence) {
        return Err(Error::Epochs {
            sequence,
            found: epochs,
        });
    }

    layers
        .into_iter()
        .map(|(epoch, layer)| {
            let blob = old
                .blobs
                .get(&layer.diff_id)
                .map(|blob| blob.digest.to_string());
            let named = manifest.iter().find(|d| Some(&d.digest) == blob.as_ref());
            named
                .map(|named| (layer.clone(), named.clone()))
                .ok_or_else(|| {
                    super::Error::Malformed {
                        sequence,
                        reason: format!(
                            "its manifest names no blob for the blocks of epoch {epoch}"
                        ),
                    }
                    .into()
                })
        })
        .collect()
}

/// The link the journal ends at, refusing one that is not `1..=S` with
/// `S` at most `last`.
fn resumed(journal: &str, last: u64) -> Result<Option<Link>, Error> {
    let lines = read_journal(journal)?;

    if !lines.keys().copied().eq(1..=lines.len() as u64) || lines.len() as u64 > last {
        return Err(Error::Journal(format!(
            "holds sequences {:?}, not 1 to at most {last} in turn",
            lines.keys().collect::<Vec<_>>()
        )));
    }

    Ok(lines
        .into_iter()
        .next_back()
        .map(|(sequence, digest)| Link {
            sequence,
            inscription_digest: digest,
        }))
}

/// Refuse a target whose newest snapshot is not where the journal ends, or
/// the one after it linked to where the journal ends: a seal whose journal
/// line was lost is sealed again with the same bytes.
fn check_target(
    target: &Repository,
    registry: &Registry,
    resumed: Option<Link>,
) -> Result<(), Error> {
    let newest = || -> Result<_, stelae::Error> {
        let Some(snapshot) = registry.latest(&BlocksProfile)? else {
            return Ok(None);
        };
        let inscription = snapshot.read_inscription()?;
        Ok(Some((
            inscription.sequence,
            inscription.digest()?,
            inscription.previous,
        )))
    };
    let newest = newest().map_err(|e| target_error(target, "reading the newest snapshot of", e))?;
    let at = resumed.map_or(0, |link| link.sequence);

    match (newest, resumed) {
        (None, None) => Ok(()),
        (Some((sequence, _, previous)), _) if sequence == at + 1 && previous == resumed => Ok(()),
        (Some((sequence, digest, _)), Some(link))
            if sequence == at && digest == link.inscription_digest =>
        {
            Ok(())
        }
        (newest, _) => Err(Error::Journal(format!(
            "ends at sequence {at}, and {target} holds {}",
            match newest {
                Some((sequence, digest, _)) => format!("epoch-{sequence} as {digest}"),
                None => "nothing".into(),
            }
        ))),
    }
}

fn target_error(
    target: &Repository,
    what: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::Target {
        repository: target.to_string(),
        what: what.to_owned(),
        source: Box::new(source),
    }
}

struct Context<'a> {
    source: &'a Registry,
    network_magic: u64,
    slots_per_file: u64,
    digests: &'a ChunkDigests,
    layers: &'a [(LayerDescriptor, Descriptor)],
}

impl Context<'_> {
    /// Seal each epoch after `resumed` on `writer`, one layer at a time.
    fn lineage<W: SnapshotWriter>(
        &self,
        writer: &W,
        resumed: Option<Link>,
        append: &mut dyn Write,
    ) -> Result<Summary, Error> {
        let mut summary = Summary {
            last: self.layers.len() as u64,
            resumed: resumed.map_or(0, |link| link.sequence),
            ..Default::default()
        };
        let mut previous = resumed;

        for (old, blob) in &self.layers[summary.resumed as usize..] {
            let epoch = old.scope["epoch"].as_u64().expect("read as a u64");
            let sequence = epoch + 1;
            let staged = self
                .source
                .pull_raw_blob(blob)
                .map_err(|e| super::Error::Read {
                    repository: self.source.repository().to_string(),
                    what: format!("the blocks blob of epoch {epoch}"),
                    source: Box::new(e),
                })?;
            let reencoded = reencode(
                staged,
                old,
                self.network_magic,
                self.slots_per_file,
                self.digests,
                writer,
            )?;
            let digests = reencoded.layer.digests;
            let sealed = stelae_blocks::seal(
                writer,
                self.network_magic,
                sequence,
                previous,
                reencoded.layer.descriptor,
            )
            .map_err(|source| Error::Layer {
                epoch,
                source: Box::new(source),
            })?;

            writeln!(append, "{sequence} {}", sealed.digest)
                .and_then(|()| append.flush())
                .map_err(Error::Write)?;
            tracing::info!(
                sequence,
                files = reencoded.files,
                diff_id = %digests.diff_id,
                inscription = %sealed.digest,
                "sealed the blocks snapshot"
            );

            previous = Some(Link {
                sequence,
                inscription_digest: sealed.digest,
            });
            summary.epochs += 1;
            summary.files += reencoded.files;
            summary.uncompressed_bytes += digests.uncompressed_size;
            summary.compressed_bytes += digests.compressed_size;
        }

        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn hex(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    #[test]
    fn the_digest_list_gives_each_chunk_once_and_skips_index_files() {
        let list = json!([
            {"immutable_file_name": "00000.chunk", "digest": hex(1)},
            {"immutable_file_name": "00000.primary", "digest": hex(2)},
            {"immutable_file_name": "00000.secondary", "digest": hex(3)},
            {"immutable_file_name": "08391.chunk", "digest": hex(4)},
            {"immutable_file_name": "08391.chunk", "digest": hex(4)},
        ]);
        let digests = ChunkDigests::parse(&serde_json::to_vec(&list).unwrap()).unwrap();

        assert_eq!(digests.0.len(), 2);
        assert_eq!(digests.get(0).unwrap().to_hex(), hex(1));
        assert_eq!(digests.get(8391).unwrap().to_hex(), hex(4));

        for bad in [
            json!([{"immutable_file_name": "x.chunk", "digest": hex(1)}]),
            json!([{"immutable_file_name": "00001.chunk", "digest": "01"}]),
            json!([
                {"immutable_file_name": "00001.chunk", "digest": hex(1)},
                {"immutable_file_name": "00001.chunk", "digest": hex(2)},
            ]),
            json!({"digests": []}),
        ] {
            let parsed = ChunkDigests::parse(&serde_json::to_vec(&bad).unwrap());
            assert!(matches!(parsed, Err(Error::Digests(_))), "{bad}");
        }
    }

    #[test]
    fn a_file_with_no_block_is_compared_as_an_empty_chunk() {
        let empty = Digest::compute(b"");
        let body = Digest::compute(b"block");
        let digests = ChunkDigests([(4, body), (5, empty)].into());

        let mut chunks = Chunks::new(4..=6, &digests);
        chunks.push(4, b"bl");
        chunks.push(4, b"ock");

        assert_eq!(
            chunks.finish(),
            (3, vec![FileMismatch::Uncertified { file: 6 }])
        );
    }

    #[test]
    fn the_journal_resumes_only_from_a_run_of_sequences_from_one() {
        let one = Digest::compute(b"1");
        let two = Digest::compute(b"2");

        assert_eq!(resumed("", 5).unwrap(), None);
        assert_eq!(
            resumed(&format!("1 {one}\n2 {two}\n"), 5).unwrap(),
            Some(Link {
                sequence: 2,
                inscription_digest: two,
            })
        );
        // Not from 1, a gap, and past the old lineage's last sequence.
        for (refused, last) in [
            (format!("2 {two}\n"), 5),
            (format!("1 {one}\n3 {two}\n"), 5),
            (format!("1 {one}\n2 {two}\n"), 1),
        ] {
            assert!(resumed(&refused, last).is_err(), "{refused:?} under {last}");
        }
    }
}
