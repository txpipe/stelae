//! `rebuild blocks` over old `blocks` layers written from preprod's first
//! immutable files (`stelae-blocks/tests/data/preprod-immutable`): Byron
//! epochs 0, 1 and 2, one 21,600-slot file each, file 0 opening with the
//! chain's epoch boundary block.
//!
//! Each old layer is what the Dolos profile wrote: the same records under a
//! header naming the Dolos profile. A re-encoded layer is held to the layer
//! `stelae-blocks` exports from the same files, and the lineage to the one
//! its export seals.

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Once,
    },
};

use dolos_snapshot::{DolosProfile, PROFILE_NAME, PROFILE_VERSION};
use serde_json::json;
use stelae::{
    dir::BlobStore,
    inscription::Link,
    manifest::DIFF_ID_ANNOTATION,
    oci::{Options as RegistryOptions, Registry},
    transport::{Discarding, RecordSink as _, SnapshotWriter as _, WrittenLayer},
    Compression, Digest, LayerSpec, ProfileRef, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE,
    SCHEMA_VERSION,
};
use stelae_blocks::{
    hardano::{export, Epoch},
    record, BlockRecord, BlocksProfile, BlocksReader, EpochScope, BLOCKS, COMPRESSION_LEVEL,
};
use stelae_cardano::{
    rebuild::{
        blocks::{self, ChunkDigests, Error, FileMismatch, Options, Summary, Upload},
        OldInscription,
    },
    testing::loopback::{Loopback, Verdict},
    Auth, Genesis, Repository,
};
use stelae_driver::Predecessor;

const PREPROD: u64 = 1;
const WIDTH: u64 = 21_600;
/// The closed epochs the fixture holds, so the old lineage's last sequence.
const EPOCHS: u64 = 3;
const OLD: &str = "cardano/old";
const ROOT: &str = "cardano/new";

fn provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn genesis() -> Genesis {
    dolos_cardano::include::preprod::load()
}

/// The fixture's files with the empty file a node opens after them, which
/// closes the last.
fn immutable() -> tempfile::TempDir {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../stelae-blocks/tests/data/preprod-immutable");
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
fn epoch(dir: &Path, number: u64, files: std::ops::RangeInclusive<u64>) -> Epoch<'_> {
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

fn blocks_of(epoch: &Epoch<'_>) -> Vec<BlockRecord> {
    epoch.blocks().unwrap().map(Result::unwrap).collect()
}

/// The layer the Dolos profile wrote for `scope`: `blocks` records under its
/// own header.
fn old_layer(store: &BlobStore, scope: EpochScope, blocks: &[BlockRecord]) -> WrittenLayer {
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
fn digest_list(dir: &Path, files: impl IntoIterator<Item = u64>) -> Vec<u8> {
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

fn chunk_digests(dir: &Path, files: impl IntoIterator<Item = u64>) -> ChunkDigests {
    ChunkDigests::parse(&digest_list(dir, files)).unwrap()
}

struct Follows(Option<Link>);

impl Predecessor for Follows {
    fn previous(&self) -> Option<Link> {
        self.0
    }
}

/// The digests of the blocks lineage `stelae-blocks` exports from the files.
fn exported_lineage(dir: &Path) -> Vec<Digest> {
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

#[test]
fn a_reencoded_layer_is_the_layer_exported_from_the_same_immutable_files() {
    let dir = immutable();
    let old = tempfile::tempdir().unwrap();
    let store = BlobStore::create(old.path()).unwrap();
    let digests = chunk_digests(dir.path(), 0..EPOCHS);

    for number in 0..EPOCHS {
        let epoch = epoch(dir.path(), number, number..=number);
        let blocks = blocks_of(&epoch);
        let layer = old_layer(&store, epoch.scope, &blocks);
        let source = File::open(store.blob_path(&layer.digests.blob_digest)).unwrap();

        let reencoded = blocks::reencode(
            source,
            &layer.descriptor,
            PREPROD,
            WIDTH,
            &digests,
            &Discarding,
        )
        .unwrap();
        let exported =
            stelae_blocks::write_layer(&Discarding, epoch.scope, COMPRESSION_LEVEL, blocks)
                .unwrap();

        assert_eq!(reencoded.files, 1);
        assert_eq!(
            reencoded.layer.descriptor, exported.descriptor,
            "epoch {number}"
        );
        assert_eq!(reencoded.layer.digests, exported.digests, "epoch {number}");
        // Only the header changed: same records, another blob.
        assert_eq!(layer.descriptor.records, exported.descriptor.records);
        assert_ne!(layer.descriptor.diff_id, exported.descriptor.diff_id);
    }
}

#[test]
fn a_reencoded_layer_reads_back_under_the_blocks_profile_with_the_same_bodies() {
    let dir = immutable();
    let (old, new) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (store, written) = (
        BlobStore::create(old.path()).unwrap(),
        BlobStore::create(new.path()).unwrap(),
    );
    // Files 0 and 1 as one epoch: two files compared, the boundary block first.
    let epoch = epoch(dir.path(), 0, 0..=1);
    let blocks = blocks_of(&epoch);
    let layer = old_layer(&store, epoch.scope, &blocks);
    let source = File::open(store.blob_path(&layer.digests.blob_digest)).unwrap();

    let reencoded = blocks::reencode(
        source,
        &layer.descriptor,
        PREPROD,
        WIDTH,
        &chunk_digests(dir.path(), 0..2),
        &written,
    )
    .unwrap();
    assert_eq!(reencoded.files, 2);

    let blob = File::open(written.blob_path(&reencoded.layer.digests.blob_digest)).unwrap();
    let mut reader = BlocksReader::open(blob, &reencoded.layer.descriptor).unwrap();
    let read: Vec<BlockRecord> = reader.by_ref().map(Result::unwrap).collect();
    reader.finish().unwrap();

    assert_eq!(reader_scope(&reencoded.layer), epoch.scope);
    assert_eq!(read, blocks);
    assert_eq!(
        read[0].slot, 0,
        "the boundary block opens the epoch, in file 0"
    );
}

fn reader_scope(layer: &WrittenLayer) -> EpochScope {
    EpochScope::read_descriptor(PREPROD, &layer.descriptor.scope).unwrap()
}

#[test]
fn a_file_mithril_does_not_certify_refuses_the_epoch_and_is_named() {
    let dir = immutable();
    let (old, new) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (store, written) = (
        BlobStore::create(old.path()).unwrap(),
        BlobStore::create(new.path()).unwrap(),
    );
    let epoch = epoch(dir.path(), 0, 0..=1);
    let layer = old_layer(&store, epoch.scope, &blocks_of(&epoch));
    let real = Digest::compute(fs::read(dir.path().join("00001.chunk")).unwrap());
    let other = Digest::compute(b"not file 1");
    let reencode = |list: serde_json::Value| {
        let source = File::open(store.blob_path(&layer.digests.blob_digest)).unwrap();
        let digests = ChunkDigests::parse(&serde_json::to_vec(&list).unwrap()).unwrap();
        blocks::reencode(
            source,
            &layer.descriptor,
            PREPROD,
            WIDTH,
            &digests,
            &written,
        )
    };
    let file_0 = json!({
        "immutable_file_name": "00000.chunk",
        "digest": Digest::compute(fs::read(dir.path().join("00000.chunk")).unwrap()).to_hex(),
    });

    let differs = reencode(json!([
        file_0,
        {"immutable_file_name": "00001.chunk", "digest": other.to_hex()},
    ]))
    .unwrap_err();
    match &differs {
        Error::Mithril { epoch: 0, files } => assert_eq!(
            files,
            &[FileMismatch::Differs {
                file: 1,
                mithril: other,
                found: real,
            }]
        ),
        other => panic!("{other:?}"),
    }
    assert!(
        differs.to_string().contains("immutable file 00001"),
        "{differs}"
    );

    let missing = reencode(json!([file_0])).unwrap_err();
    assert!(
        matches!(&missing, Error::Mithril { epoch: 0, files } if files == &[FileMismatch::Uncertified { file: 1 }]),
        "{missing:?}"
    );

    // A refused layer leaves nothing on the writer.
    assert_eq!(
        fs::read_dir(new.path().join("blobs/sha256"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn a_layer_of_another_network_is_refused() {
    let dir = immutable();
    let old = tempfile::tempdir().unwrap();
    let store = BlobStore::create(old.path()).unwrap();
    let epoch = epoch(dir.path(), 0, 0..=0);
    let layer = old_layer(&store, epoch.scope, &blocks_of(&epoch));
    let source = File::open(store.blob_path(&layer.digests.blob_digest)).unwrap();

    let error = blocks::reencode(
        source,
        &layer.descriptor,
        2,
        WIDTH,
        &chunk_digests(dir.path(), 0..1),
        &Discarding,
    )
    .unwrap_err();
    assert!(matches!(error, Error::Layer { epoch: 0, .. }), "{error:?}");
}

/// An old lineage, `epoch-1` to `epoch-3`, served by a loopback: each
/// inscription lists the blocks layers of every closed epoch, and the block
/// layer of its own cut, which the re-encode leaves out.
struct Served {
    _root: tempfile::TempDir,
    _immutable: tempfile::TempDir,
    loopback: Loopback,
    exported: Vec<Digest>,
    immutable: PathBuf,
}

fn serve() -> Served {
    let immutable = immutable();
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join(OLD);
    let store = BlobStore::create(&repository).unwrap();
    let tags = repository.join("manifests/tags");
    fs::create_dir_all(&tags).unwrap();

    let closed: Vec<WrittenLayer> = (0..EPOCHS)
        .map(|number| {
            let epoch = epoch(immutable.path(), number, number..=number);
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

    provider();
    let loopback = Loopback::serve(root.path()).unwrap();
    Served {
        exported: exported_lineage(immutable.path()),
        immutable: immutable.path().to_owned(),
        _root: root,
        _immutable: immutable,
        loopback,
    }
}

impl Served {
    fn options(&self, upload: bool) -> Options {
        Options {
            source: self.loopback.repository(OLD).parse().unwrap(),
            auth: Auth::Anonymous,
            insecure: true,
            upload: upload.then(|| Upload {
                root: self.root(),
                auth: Auth::Anonymous,
            }),
            scratch_dir: None,
        }
    }

    fn root(&self) -> Repository {
        self.loopback.repository(ROOT).parse().unwrap()
    }

    fn journal(&self, through: u64) -> String {
        (1..=through)
            .map(|sequence| format!("{sequence} {}\n", self.exported[sequence as usize - 1]))
            .collect()
    }
}

#[test]
fn a_dry_run_journals_the_exported_lineage_and_writes_nothing_to_a_registry() {
    let served = serve();
    let writes = Arc::new(AtomicU64::new(0));
    let counted = writes.clone();
    served.loopback.rule(move |seen| match seen.method {
        "GET" | "HEAD" => Verdict::Proceed,
        _ => {
            counted.fetch_add(1, Ordering::SeqCst);
            Verdict::Refuse(405)
        }
    });
    served
        .loopback
        .offer_digests(&digest_list(&served.immutable, 0..EPOCHS))
        .unwrap();
    let digests = blocks::fetch(&served.loopback.aggregator()).unwrap();

    let mut journal = Vec::new();
    let summary = blocks::run(
        &served.options(false),
        &genesis(),
        &digests,
        "",
        &mut journal,
    )
    .unwrap();

    assert_eq!(String::from_utf8(journal).unwrap(), served.journal(EPOCHS));
    assert_eq!(
        summary,
        Summary {
            last: EPOCHS,
            resumed: 0,
            epochs: EPOCHS,
            files: EPOCHS,
            uncompressed_bytes: summary.uncompressed_bytes,
            compressed_bytes: summary.compressed_bytes,
        }
    );
    assert!(summary.uncompressed_bytes > 0 && summary.compressed_bytes > 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert!(served
        .loopback
        .manifests(&format!("{ROOT}/blocks"))
        .is_empty());
}

#[test]
fn an_upload_refused_at_an_epoch_resumes_after_its_journal() {
    let served = serve();
    let genesis = genesis();
    let target = format!("{ROOT}/blocks");

    // File 2 is not certified yet: epochs 0 and 1 are sealed, 2 refused.
    let early = chunk_digests(&served.immutable, 0..2);
    let mut journal = Vec::new();
    let refused =
        blocks::run(&served.options(true), &genesis, &early, "", &mut journal).unwrap_err();
    assert!(
        matches!(&refused, Error::Mithril { epoch: 2, files } if files == &[FileMismatch::Uncertified { file: 2 }]),
        "{refused:?}"
    );
    let journal = String::from_utf8(journal).unwrap();
    assert_eq!(journal, served.journal(2));
    assert!(served.loopback.manifest(&target, "epoch-2").is_some());
    assert!(served.loopback.manifest(&target, "epoch-3").is_none());

    let digests = chunk_digests(&served.immutable, 0..EPOCHS);
    let mut appended = Vec::new();
    let summary = blocks::run(
        &served.options(true),
        &genesis,
        &digests,
        &journal,
        &mut appended,
    )
    .unwrap();
    assert_eq!((summary.resumed, summary.epochs, summary.files), (2, 1, 1));
    assert_eq!(
        journal + &String::from_utf8(appended).unwrap(),
        served.journal(EPOCHS)
    );

    let registry = Registry::open(
        &served.loopback.repository(&target).parse().unwrap(),
        RegistryOptions {
            insecure: true,
            ..Default::default()
        },
    )
    .unwrap();
    for sequence in 1..=EPOCHS {
        let inscription = registry.pull_inscription(&BlocksProfile, sequence).unwrap();
        assert_eq!(
            inscription.digest().unwrap(),
            served.exported[sequence as usize - 1]
        );
    }

    // A journal behind the repository is refused before a layer is re-encoded.
    let behind = blocks::run(
        &served.options(true),
        &genesis,
        &digests,
        &served.journal(1),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(behind, Error::Journal(_)), "{behind:?}");
}

#[test]
fn an_upload_seals_again_after_a_lost_journal_line_and_refuses_a_journal_the_target_does_not_follow(
) {
    let served = serve();
    let genesis = genesis();
    let target = format!("{ROOT}/blocks");
    let digests = chunk_digests(&served.immutable, 0..EPOCHS);
    let foreign = Digest::compute(b"another lineage");
    let refuses = |journal: &str| {
        let refused = blocks::run(
            &served.options(true),
            &genesis,
            &digests,
            journal,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(
            matches!(refused, Error::Journal(_)),
            "{journal}: {refused:?}"
        );
    };

    // An empty target follows no journal.
    refuses(&served.journal(2));
    assert!(served.loopback.manifests(&target).is_empty());

    // Epochs 0 and 1 are sealed, 2 refused: file 2 is not certified yet.
    let early = chunk_digests(&served.immutable, 0..2);
    blocks::run(&served.options(true), &genesis, &early, "", &mut Vec::new()).unwrap_err();
    let sealed = served.loopback.manifests(&target);
    assert!(sealed.contains_key("epoch-2") && !sealed.contains_key("epoch-3"));

    // The target's newest under another digest, and a snapshot one past the
    // journal that links elsewhere.
    refuses(&format!("1 {}\n2 {foreign}\n", served.exported[0]));
    refuses(&format!("1 {foreign}\n"));
    assert_eq!(served.loopback.manifests(&target), sealed);

    // The journal lost the line of the target's newest seal: it is sealed
    // again with the same bytes and the lineage goes on.
    let mut appended = Vec::new();
    let summary = blocks::run(
        &served.options(true),
        &genesis,
        &digests,
        &served.journal(1),
        &mut appended,
    )
    .unwrap();
    assert_eq!((summary.resumed, summary.epochs), (1, 2));
    assert_eq!(
        served.journal(1) + &String::from_utf8(appended).unwrap(),
        served.journal(EPOCHS)
    );
    assert_eq!(
        served.loopback.manifest(&target, "epoch-2"),
        sealed.get("epoch-2").cloned()
    );

    let registry = Registry::open(
        &served.loopback.repository(&target).parse().unwrap(),
        RegistryOptions {
            insecure: true,
            ..Default::default()
        },
    )
    .unwrap();
    for sequence in 1..=EPOCHS {
        let inscription = registry.pull_inscription(&BlocksProfile, sequence).unwrap();
        assert_eq!(
            inscription.digest().unwrap(),
            served.exported[sequence as usize - 1]
        );
    }
}

#[test]
fn a_failed_digest_fetch_does_not_report_the_aggregator_credentials() {
    provider();
    let root = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(root.path()).unwrap();
    let aggregator = loopback
        .aggregator()
        .replace("http://", "http://reader:secret@");

    // Nothing is offered, so the route answers 404.
    let error = blocks::fetch(&aggregator).unwrap_err().to_string();
    assert!(error.contains("127.0.0.1"), "{error}");
    assert!(
        !error.contains("secret") && !error.contains("reader"),
        "{error}"
    );
}
