//! Old `blocks` layers written from preprod's first immutable files
//! (`stelae-blocks/tests/data/preprod-immutable`): Byron epochs 0, 1 and 2,
//! one 21,600-slot file each, file 0 opening with the chain's epoch boundary
//! block. Each old layer holds the same records `stelae-blocks` exports, under
//! a header naming the Dolos profile, as the Dolos profile wrote them.

use std::{
    fs,
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use dolos_snapshot::{DolosProfile, PROFILE_NAME, PROFILE_VERSION};
use serde_json::json;
use stelae::{
    dir::BlobStore,
    inscription::Link,
    manifest::DIFF_ID_ANNOTATION,
    transport::{Discarding, RecordSink as _, SnapshotWriter as _, WrittenLayer},
    Compression, Digest, LayerSpec, ProfileRef, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE,
    SCHEMA_VERSION,
};
use stelae_blocks::{
    hardano::{export, Epoch},
    record, BlockRecord, EpochScope, BLOCKS, COMPRESSION_LEVEL,
};
use stelae_driver::Predecessor;

use crate::rebuild::OldInscription;

pub const PREPROD: u64 = 1;
pub const WIDTH: u64 = 21_600;
/// The closed epochs the fixture holds, so the old lineage's last sequence.
pub const EPOCHS: u64 = 3;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../stelae-blocks/tests/data/preprod-immutable")
}

/// The fixture's files with the empty file a node opens after them, which
/// closes the last.
pub fn immutable() -> tempfile::TempDir {
    let fixture = fixture();
    let dir = tempfile::tempdir().unwrap();
    for number in 0..=EPOCHS {
        for extension in ["chunk", "primary", "secondary"] {
            let name = format!("{number:05}.{extension}");
            match number < EPOCHS {
                true => fs::copy(fixture.join(&name), dir.path().join(&name)).map(|_| ()),
                false => fs::write(dir.path().join(&name), b""),
            }
            .unwrap();
        }
    }
    dir
}

/// Files `files` as one epoch numbered `number`.
pub fn epoch(dir: &Path, number: u64, files: RangeInclusive<u64>) -> Epoch<'_> {
    let scope = EpochScope::new(
        PREPROD,
        number,
        files.start() * WIDTH,
        (files.end() + 1) * WIDTH - 1,
    )
    .unwrap();
    Epoch {
        immutable_dir: dir,
        scope,
        slots_per_file: WIDTH,
    }
}

pub fn blocks_of(epoch: &Epoch<'_>) -> Vec<BlockRecord> {
    epoch.blocks().unwrap().map(Result::unwrap).collect()
}

/// The layer the Dolos profile wrote for `scope`: `blocks` records under its
/// own header.
pub fn old_layer(store: &BlobStore, scope: EpochScope, blocks: &[BlockRecord]) -> WrittenLayer {
    let spec = LayerSpec::new(BLOCKS, scope.header().unwrap(), scope.descriptor());
    let mut sink = store
        .layer_sink(&DolosProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();
    for block in blocks {
        sink.write_record(&record::encode(block).unwrap()).unwrap();
    }
    sink.finish().unwrap()
}

/// Mithril's digest list for the fixture's files, with each file's index
/// files beside its `.chunk`, as the aggregator lists them.
pub fn digest_list(dir: &Path, files: impl IntoIterator<Item = u64>) -> Vec<u8> {
    let items: Vec<_> = files
        .into_iter()
        .flat_map(|file| {
            ["chunk", "primary", "secondary"].map(|extension| {
                let name = format!("{file:05}.{extension}");
                let digest = Digest::compute(fs::read(dir.join(&name)).unwrap());
                json!({"immutable_file_name": name, "digest": digest.to_hex()})
            })
        })
        .collect();
    serde_json::to_vec(&items).unwrap()
}

struct Follows(Option<Link>);

impl Predecessor for Follows {
    fn previous(&self) -> Option<Link> {
        self.0
    }
}

/// The digests of the blocks lineage `stelae-blocks` exports from the files.
pub fn exported_lineage(dir: &Path) -> Vec<Digest> {
    let mut previous = None;
    (0..EPOCHS)
        .map(|number| {
            let sealed = export(
                &Discarding,
                &Follows(previous),
                &epoch(dir, number, number..=number),
            )
            .unwrap();
            previous = Some(Link {
                sequence: sealed.inscription.sequence,
                inscription_digest: sealed.digest,
            });
            sealed.digest
        })
        .collect()
}

/// Lay under `root`, as the loopback repository `name`, an old lineage
/// `epoch-1` to `epoch-3` of the files in `immutable`: each inscription lists
/// the blocks layers of every closed epoch, and the block layer of its own
/// cut, which the re-encode leaves out.
pub fn lay_old_blocks_lineage(root: &Path, name: &str, immutable: &Path) {
    let repository = root.join(name);
    let store = BlobStore::create(&repository).unwrap();
    let tags = repository.join("manifests/tags");
    fs::create_dir_all(&tags).unwrap();

    let closed: Vec<WrittenLayer> = (0..EPOCHS)
        .map(|number| {
            let epoch = epoch(immutable, number, number..=number);
            old_layer(&store, epoch.scope, &blocks_of(&epoch))
        })
        .collect();

    for sequence in 1..=EPOCHS {
        let cut = EpochScope::new(
            PREPROD,
            sequence,
            sequence * WIDTH,
            (sequence + 1) * WIDTH - 1,
        );
        let mut layers: Vec<WrittenLayer> = closed[..sequence as usize].to_vec();
        layers.push(old_layer(&store, cut.unwrap(), &[]));

        let inscription = OldInscription {
            schema: SCHEMA_VERSION,
            profile: ProfileRef {
                name: PROFILE_NAME.to_owned(),
                version: PROFILE_VERSION,
            },
            sequence,
            position: json!({"networkMagic": PREPROD, "epoch": sequence}),
            parameters: json!({}),
            compression: Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
            history: Vec::new(),
            layers: layers
                .iter()
                .map(|layer| layer.descriptor.clone())
                .collect(),
        };
        let config = stelae::canonical_json(&serde_json::to_value(&inscription).unwrap()).unwrap();
        let config_digest = store.put(&config).unwrap();
        let manifest = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": ARTIFACT_TYPE,
            "config": {
                "mediaType": INSCRIPTION_MEDIA_TYPE,
                "digest": config_digest.to_string(),
                "size": config.len(),
            },
            "layers": layers.iter().map(|layer| json!({
                "mediaType": layer.descriptor.media_type,
                "digest": layer.digests.blob_digest.to_string(),
                "size": layer.digests.compressed_size,
                "annotations": {DIFF_ID_ANNOTATION: layer.descriptor.diff_id.to_string()},
            })).collect::<Vec<_>>(),
        });
        fs::write(
            tags.join(format!("epoch-{sequence}")),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
}
