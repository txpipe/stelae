//! The chained publish, driven the way a profile's export drives it.
//!
//! Each test publishes a toy profile into an in-process registry through
//! [`Chained`]: ask the predecessor whether a layer can be resumed, build it
//! where it cannot, note it as landed, then place the layers by their role and
//! seal. What the tests check is what the registry received.

#![cfg(feature = "oci")]

use std::{
    collections::BTreeSet,
    path::Path,
    time::{Duration, Instant},
};

use serde_json::json;
use stelae::{
    frame::{encode, CanonicalCbor},
    oci::{Options, Registry},
    Compression, Inscription, LayerDescriptor, LayerSpec, Link, SnapshotWriter,
};
use stelae_driver::{
    place_layers,
    publish::{Chained, Origin, PublishRecord, Publishing},
    DriverProfile, Predecessor, Role,
};

#[allow(dead_code)]
#[path = "../../stelae/tests/fault_registry/mod.rs"]
mod fault_registry;

use fault_registry::FaultRegistry;

const LEVEL: i32 = 9;

/// A profile with one closed chapter of notes per sequence, listed in `diff`,
/// and an index rebuilt whole each time, listed in `tip`.
struct Shelf;

impl stelae::Profile for Shelf {
    fn name(&self) -> &str {
        "dev.example.shelf"
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
        Ok(format!("application/vnd.example.stelae.{kind}.v1+zstd"))
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
        Ok(format!("chapter-{sequence}"))
    }
}

impl DriverProfile for Shelf {
    fn epoch_kinds(&self) -> &[&str] {
        &["notes"]
    }

    fn dense_epoch_kinds(&self) -> &[&str] {
        &["notes"]
    }

    fn is_state_kind(&self, kind: &str) -> bool {
        kind == "index"
    }

    fn role(
        &self,
        kind: &str,
        _scope: &serde_json::Value,
        _sequence: u64,
    ) -> Result<Role, stelae_driver::Error> {
        match kind {
            "notes" => Ok(Role::Diff),
            _ => Ok(Role::Tip),
        }
    }

    fn check_same_dataset(
        &self,
        _previous: &Inscription,
        _position: &serde_json::Value,
    ) -> Result<(), stelae_driver::Error> {
        Ok(())
    }
}

fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("nothing else installed a provider first");
    });
}

fn open(fault: &FaultRegistry) -> Registry {
    install_crypto_provider();

    Registry::open(
        &fault.url("example/shelf").parse().unwrap(),
        Options {
            insecure: true,
            ..Default::default()
        },
    )
    .unwrap()
}

fn compression() -> Compression {
    Compression {
        algo: "zstd".to_owned(),
        level: LEVEL as i64,
    }
}

fn record(n: u64) -> CanonicalCbor {
    encode(|e| {
        e.array(2)?.u64(n)?.str("a note")?;
        Ok(())
    })
    .unwrap()
}

fn header(kind: &str, sequence: u64) -> CanonicalCbor {
    encode(|e| {
        e.array(2)?.u64(sequence)?.str(kind)?;
        Ok(())
    })
    .unwrap()
}

/// The layers of `sequence`, each with the records it holds.
fn layers(sequence: u64) -> Vec<(LayerSpec, Vec<CanonicalCbor>)> {
    vec![
        (
            LayerSpec::new(
                "notes",
                header("notes", sequence),
                json!({"chapter": sequence}),
            ),
            (1..=3).map(|n| record(sequence * 10 + n)).collect(),
        ),
        (
            LayerSpec::new(
                "index",
                header("index", sequence),
                json!({"chapter": sequence}),
            ),
            (1..=sequence).map(record).collect(),
        ),
    ]
}

/// The publish of `sequence` that follows whatever `publishing`'s repository
/// holds.
fn chained_in(
    publishing: Publishing<'_>,
    sequence: u64,
) -> Result<Chained<'_>, stelae_driver::Error> {
    let registry = publishing.registry;
    let latest = registry.latest(&Shelf).unwrap();

    Chained::new(
        &Shelf,
        publishing,
        latest.as_ref(),
        sequence,
        &json!({"chapter": sequence}),
        Origin::of(registry, 1, json!({}), compression()),
    )
}

/// The publish of `sequence` that follows whatever `registry` holds, recording
/// at `record`.
fn chained<'a>(registry: &'a Registry, record: &Path, sequence: u64) -> Chained<'a> {
    chained_in(Publishing::new(registry).recording_in(record), sequence).unwrap()
}

/// What an export does per layer: resume it, or build it, and note it.
fn build(chained: &Chained<'_>, registry: &Registry, sequence: u64) -> Vec<LayerDescriptor> {
    layers(sequence)
        .into_iter()
        .map(|(spec, records)| {
            let descriptor = match chained.resume(&spec.kind, &spec.scope).unwrap() {
                Some(resumed) => resumed,
                None => {
                    registry
                        .write_layer(&Shelf, &spec, LEVEL, &records)
                        .unwrap()
                        .descriptor
                }
            };

            chained.landed(&descriptor).unwrap();

            descriptor
        })
        .collect()
}

fn seal(
    chained: &Chained<'_>,
    registry: &Registry,
    sequence: u64,
    layers: Vec<LayerDescriptor>,
) -> Inscription {
    let mut inscription = Inscription::new(
        &Shelf,
        sequence,
        json!({"chapter": sequence}),
        json!({}),
        compression(),
    );

    inscription.previous = chained.previous();
    place_layers(&Shelf, &mut inscription, layers).unwrap();

    registry.seal(&Shelf, &inscription).unwrap();
    chained.forget_record().unwrap();

    inscription
}

fn publish(registry: &Registry, record: &Path, sequence: u64) -> Inscription {
    let chained = chained(registry, record, sequence);
    let layers = build(&chained, registry, sequence);

    seal(&chained, registry, sequence, layers)
}

fn eventually(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);

    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// An attempt at `sequence` that lands all its layers, waits until they are
/// up, and stops before the seal, leaving its record behind.
fn interrupt(fault: &FaultRegistry, record: &Path, sequence: u64) {
    let interrupted = open(fault);
    let chained = chained(&interrupted, record, sequence);

    for layer in build(&chained, &interrupted, sequence) {
        let blob = interrupted
            .carried(&layer.diff_id)
            .unwrap()
            .digests
            .blob_digest
            .to_string();

        eventually("an interrupted layer's upload", || {
            fault.pushed().contains(&blob)
        });
    }
}

/// The publish at N+1 links to N, and uploads and lists only its own layers.
#[test]
fn a_chained_publish_uploads_and_lists_only_its_own_layers() {
    let fault = FaultRegistry::spawn();
    let registry = open(&fault);
    let storage = tempfile::tempdir().unwrap();
    let record = storage.path().join("publish.json");

    let first = publish(&registry, &record, 1);
    let first_blobs = fault.layers();

    assert_eq!(first.previous, None);

    registry.take_transfer();
    let before = fault.pushed().len();

    // Asked under any key of the first snapshot, the publish at 2 offers
    // nothing of it.
    let chained = chained(&registry, &record, 2);

    for layer in first.layers() {
        assert!(
            chained.resume(&layer.kind, &layer.scope).unwrap().is_none(),
            "the publish at 2 offered the first snapshot's {} layer",
            layer.kind
        );
    }

    let layers = build(&chained, &registry, 2);
    let second = seal(&chained, &registry, 2, layers);
    let transfer = registry.take_transfer();

    assert_eq!(
        second.previous,
        Some(Link {
            sequence: 1,
            inscription_digest: first.digest().unwrap(),
        })
    );

    // Listed: one notes layer in `diff` and one index in `tip`, neither of
    // them the first snapshot's, and a manifest naming no blob of the first.
    let earlier: BTreeSet<_> = first.layers().map(|layer| layer.diff_id).collect();

    assert_eq!(second.diff.len(), 1);
    assert_eq!(second.diff[0].scope, json!({"chapter": 2}));
    assert_eq!(second.tip.len(), 1);
    assert!(
        second
            .layers()
            .all(|layer| !earlier.contains(&layer.diff_id)),
        "the second inscription lists a layer of the first: {second:?}"
    );

    let listed = fault.layers();

    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|blob| !first_blobs.contains(blob)));

    // Uploaded: those two blobs and the inscription, and nothing else.
    let pushed: BTreeSet<_> = fault.pushed()[before..].iter().cloned().collect();
    let expected: BTreeSet<_> = listed.iter().cloned().chain([fault.config()]).collect();

    assert_eq!(pushed, expected);
    assert_eq!(transfer.layers_uploaded, 2);
    assert_eq!(transfer.layers_skipped, 0);
    assert_eq!(transfer.layers_reused, 0);
    assert!(!record.exists(), "the record outlived the seal");
}

/// A publish interrupted after its layers landed resumes its `diff` layer
/// from the record and builds its tip again, and the snapshot is the one an
/// uninterrupted publish would have sealed.
#[test]
fn an_interrupted_publish_resumes_its_diff_layers_and_rebuilds_its_tip() {
    let fault = FaultRegistry::spawn();
    let storage = tempfile::tempdir().unwrap();
    let record = storage.path().join("publish.json");

    publish(&open(&fault), &record, 1);
    interrupt(&fault, &record, 2);

    let recorded = PublishRecord::load(&record).unwrap().unwrap();
    let kinds: Vec<_> = recorded
        .layers
        .iter()
        .map(|layer| layer.descriptor.kind.as_str())
        .collect();

    assert_eq!(kinds, ["notes"], "a tip layer was recorded");

    let registry = open(&fault);
    let before = fault.pushed().len();

    let chained = chained(&registry, &record, 2);
    let layers = build(&chained, &registry, 2);

    assert_eq!(chained.resumed(), 1);

    let resumed = seal(&chained, &registry, 2, layers);
    let transfer = registry.take_transfer();

    assert_eq!(transfer.layers_reused, 1, "the notes come from the record");
    assert_eq!(
        transfer.layers_skipped, 1,
        "the index is built again and found up"
    );
    assert_eq!(transfer.layers_uploaded, 0);
    assert_eq!(fault.pushed()[before..], [fault.config()]);
    assert!(!record.exists(), "the record outlived the seal");

    let clean = FaultRegistry::spawn();
    let elsewhere = open(&clean);
    let clean_record = storage.path().join("clean.json");

    publish(&elsewhere, &clean_record, 1);

    assert_eq!(resumed, publish(&elsewhere, &clean_record, 2));
}

/// A recorded layer whose blob the repository no longer holds is built again
/// rather than named, and the snapshot is the one an uninterrupted publish
/// would have sealed.
#[test]
fn a_recorded_layer_whose_blob_is_gone_is_built_again() {
    let fault = FaultRegistry::spawn();
    let storage = tempfile::tempdir().unwrap();
    let record = storage.path().join("publish.json");

    publish(&open(&fault), &record, 1);
    interrupt(&fault, &record, 2);

    // Every blob `HEAD` now answers 404, so the recorded notes are gone as far
    // as the restart can tell.
    fault.fail_heads(404);

    let registry = open(&fault);
    let chained = chained(&registry, &record, 2);
    let layers = build(&chained, &registry, 2);

    assert_eq!(
        chained.resumed(),
        0,
        "a layer whose blob is gone was resumed"
    );

    let rebuilt = seal(&chained, &registry, 2, layers);
    let transfer = registry.take_transfer();

    assert_eq!(transfer.layers_reused, 0);
    assert!(!record.exists(), "the record outlived the seal");

    let clean = FaultRegistry::spawn();
    let elsewhere = open(&clean);
    let clean_record = storage.path().join("clean.json");

    publish(&elsewhere, &clean_record, 1);

    assert_eq!(rebuilt, publish(&elsewhere, &clean_record, 2));
}

/// The operator's `--rebuild` builds every layer and resumes none, whatever
/// the record says.
#[test]
fn a_rebuild_resumes_nothing_from_the_record() {
    let fault = FaultRegistry::spawn();
    let storage = tempfile::tempdir().unwrap();
    let record = storage.path().join("publish.json");

    publish(&open(&fault), &record, 1);
    interrupt(&fault, &record, 2);

    let registry = open(&fault);
    let chained = chained_in(
        Publishing::new(&registry)
            .recording_in(record.as_path())
            .rebuilding(true),
        2,
    )
    .unwrap();
    let layers = build(&chained, &registry, 2);

    assert_eq!(chained.resumed(), 0, "a rebuild resumed a recorded layer");

    seal(&chained, &registry, 2, layers);
    let transfer = registry.take_transfer();

    assert_eq!(transfer.layers_reused, 0, "a layer came from the record");
    assert_eq!(
        transfer.layers_skipped, 2,
        "both layers are built again and found up"
    );
}

/// A publish that holds the repository to a predecessor it no longer shows is
/// refused, and one that holds it to the right one chains.
#[test]
fn a_publish_is_refused_when_its_predecessor_moved() {
    let fault = FaultRegistry::spawn();
    let registry = open(&fault);
    let storage = tempfile::tempdir().unwrap();
    let record = storage.path().join("publish.json");

    let first = publish(&registry, &record, 1);
    let expected = (1, first.digest().unwrap());

    // Expecting an empty repository, or another writer's snapshot at the same
    // sequence, the publish finds the first snapshot.
    let elsewhere = (1, stelae::Digest::compute("another writer's inscription"));

    for (follows, what) in [
        (None, "an empty repository"),
        (Some(elsewhere), "another snapshot at the same sequence"),
    ] {
        let err = chained_in(
            Publishing::new(&registry)
                .recording_in(record.as_path())
                .following(follows),
            2,
        )
        .err()
        .unwrap_or_else(|| panic!("a publish expecting {what} chained to the first snapshot"));

        assert!(
            matches!(
                err,
                stelae_driver::Error::Stelae(stelae::Error::PredecessorMoved { .. })
            ),
            "{err:?}"
        );
    }

    let chained = chained_in(
        Publishing::new(&registry)
            .recording_in(record.as_path())
            .following(Some(expected)),
        2,
    )
    .unwrap();

    assert_eq!(chained.predecessor(), Some(expected));
}
