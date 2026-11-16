//! The boundary proof: a snapshot of a profile the protocol knows nothing
//! about.
//!
//! `dev.example.toy` publishes chapters of notes. It has no chain, no epochs,
//! no blocks, no ledger — nothing Cardano-shaped and nothing Dolos-shaped. If
//! the protocol had absorbed an assumption from its first real profile, this
//! file would not compile or would not pass, which is why it is the only
//! profile in the tree while the core is being written.
//!
//! What it demonstrates, end to end:
//!
//! 1. A snapshot is written to a directory and read back, records intact, and a
//!    three-sequence chain resolves out of one.
//! 2. Two independent write runs produce the same inscription digest.
//! 3. Every vendor-owned string in the artifact came from the profile; the core
//!    composed none of them.
//! 4. `position`, `parameters` and `scope` survive as arbitrary shapes — the
//!    core canonicalizes and hashes them without ever typing them.
//! 5. A snapshot of a *different* profile, or a profile major version above the
//!    one implemented, is refused cleanly.
//! 6. Both write paths — a layer handed over whole, and a layer streamed into a
//!    sink — produce the same artifact, and this profile publishes one of each.
//! 7. A snapshot carrying a layer kind this build does not define is *readable*
//!    — the layer is reported and skipped — and not publishable on top of.

use std::{collections::BTreeSet, io::Write as _, path::Path};

use serde_json::json;

use stelae::{
    digest::read_blob,
    dir::{BlobIndex, BlobStore, LayerSpec, Snapshot, SnapshotDir, WrittenLayer},
    frame::{encode, CanonicalCbor, Limits},
    inscription::link_for,
    Compression, Digest, Discarding, Error, Inscription, LayerDescriptor, LayerReader, LayerWriter,
    Profile, RecordSink, SnapshotReader, SnapshotWriter,
};

const PROFILE_NAME: &str = "dev.example.toy";
const NOTES_MEDIA_TYPE: &str = "application/vnd.example.stelae.notes.v1+zstd";
const INDEX_MEDIA_TYPE: &str = "application/vnd.example.stelae.index.v1+zstd";
const COVERS_MEDIA_TYPE: &str = "application/vnd.example.stelae.covers.v1+zstd";
const COMPRESSION_LEVEL: i32 = 9;

/// A vendor's profile. Everything here is the vendor's business; none of it is
/// known to `stelae`.
struct ToyProfile;

impl Profile for ToyProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        match kind {
            "notes" => Ok(NOTES_MEDIA_TYPE.to_owned()),
            "index" => Ok(INDEX_MEDIA_TYPE.to_owned()),
            other => Err(Error::UnknownLayerKind {
                profile: PROFILE_NAME.to_owned(),
                kind: other.to_owned(),
            }),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        Ok(format!("chapter-{sequence}"))
    }
}

/// The profile's own record shapes. The protocol never sees inside these.
struct Note {
    id: u64,
    title: &'static str,
    body: &'static [u8],
}

const NOTES: &[Note] = &[
    Note {
        id: 1,
        title: "on stelae",
        body: b"a standing inscribed slab",
    },
    Note {
        id: 2,
        title: "on determinism",
        body: b"two publishers, one digest",
    },
    Note {
        id: 3,
        title: "on profiles",
        body: b"the vendor owns the payload",
    },
];

fn note_record(note: &Note) -> CanonicalCbor {
    encode(|e| {
        e.array(3)?
            .u64(note.id)?
            .str(note.title)?
            .bytes(note.body)?;
        Ok(())
    })
    .unwrap()
}

fn index_record(note: &Note) -> CanonicalCbor {
    encode(|e| {
        e.array(2)?.str(note.title)?.u64(note.id)?;
        Ok(())
    })
    .unwrap()
}

/// The profile's scope for the notes layer: a CBOR array in the header record,
/// the same idea as a JSON object in the inscription. Two encodings of one
/// vendor-owned concept; the protocol carries both without reading either.
fn notes_scopes() -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.array(3)?.u64(3)?.u64(1)?.u64(3)?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": 3, "firstId": 1, "lastId": 3}))
}

fn index_scopes() -> (CanonicalCbor, serde_json::Value) {
    let header = encode(|e| {
        e.map(1)?.str("chapter")?.u64(3)?;
        Ok(())
    })
    .unwrap();

    (header, json!({"chapter": 3}))
}

/// Read a layer both ways and insist the two paths agree.
///
/// Everything a consumer can observe is compared: the header, the records, the
/// digests, and — where they fail — the failure. The two readers exist because
/// one of them can afford to hold a layer and the other cannot; nothing else
/// about them is allowed to differ, because a layer that restores through one
/// and not the other is a determinism bug in the format itself.
fn read_both_ways(
    snapshot: &Snapshot,
    index: &BlobIndex,
    descriptor: &LayerDescriptor,
) -> Result<Vec<Vec<u8>>, Error> {
    let buffered = snapshot.read_layer(index, &ToyProfile, descriptor);

    // A window far below one record, so the streaming path is exercised at its
    // refill boundaries rather than swallowing the layer in one read.
    let limits = Limits {
        window: 8,
        ..Limits::default()
    };

    let streamed = (|| {
        let mut reader = snapshot.stream_layer(index, &ToyProfile, descriptor, limits)?;

        let mut records = Vec::new();
        while let Some(record) = reader.next_record() {
            records.push(record?.to_vec());
        }

        let digests = reader.finish()?;

        Ok::<_, Error>((records, digests))
    })();

    match (buffered, streamed) {
        (Ok(layer), Ok((records, digests))) => {
            assert_eq!(layer.digests(), &digests, "digests");

            let buffered_records: Vec<Vec<u8>> = layer
                .records()
                .map(|r| r.unwrap().to_vec())
                .collect::<Vec<_>>();

            assert_eq!(buffered_records, records, "records");

            Ok(records)
        }
        (Err(buffered), Err(streamed)) => {
            assert_eq!(
                std::mem::discriminant(&buffered),
                std::mem::discriminant(&streamed),
                "both paths must refuse for the same reason: \
                 buffered {buffered:?}, streaming {streamed:?}"
            );

            Err(buffered)
        }
        (Ok(_), Err(streamed)) => panic!("the streaming path alone refused it: {streamed:?}"),
        (Err(buffered), Ok(_)) => panic!("the buffered path alone refused it: {buffered:?}"),
    }
}

/// Write a complete snapshot of the toy profile into `root`.
///
/// This is the publisher side of the protocol in miniature: frame the profile's
/// records into layers, collect the descriptors the layers yield, and put them
/// in an inscription whose digest is the snapshot's identity.
fn write_snapshot(root: &Path) -> (Inscription, stelae::Digest) {
    let snapshot = SnapshotDir::create(root).unwrap();
    let (notes, index) = write_layers(&snapshot, &ToyProfile);
    let inscription = chapter_three(notes.descriptor, index.descriptor);

    let digest = snapshot.seal(&ToyProfile, &inscription).unwrap();

    (inscription, digest)
}

/// The snapshot [`write_snapshot`] sealed in `root`, read back as one
/// sequence. Its link names a sequence the directory does not hold, so it is
/// pulled, not resolved.
fn pulled(root: &Path) -> Snapshot {
    SnapshotDir::open(root)
        .unwrap()
        .pull_latest(&ToyProfile)
        .unwrap()
}

/// The two layers of chapter 3, written into `writer`.
fn write_layers(
    writer: &impl SnapshotWriter,
    profile: &dyn Profile,
) -> (WrittenLayer, WrittenLayer) {
    let (notes_header_scope, notes_scope) = notes_scopes();
    let (index_header_scope, index_scope) = index_scopes();

    let notes: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let written_notes = writer
        .write_layer(
            profile,
            &LayerSpec::new("notes", notes_header_scope, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    // The index layer is streamed into a sink instead, so this profile — which
    // the protocol knows nothing about — exercises both write paths in the same
    // snapshot. The records are never collected: a profile publishing at Dolos
    // sizes cannot hold a layer, and the shape it needs has to work for a
    // three-note chapter too. That the goldens below do not move is the proof
    // that the two paths produce one artifact.
    let mut sorted: Vec<&Note> = NOTES.iter().collect();
    sorted.sort_by_key(|n| n.title);

    let mut index_sink = writer
        .layer_sink(
            profile,
            &LayerSpec::new("index", index_header_scope, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    for note in sorted {
        index_sink.write_record(&index_record(note)).unwrap();
    }

    (written_notes, index_sink.finish().unwrap())
}

/// Chapter 3's inscription over its notes and its index.
fn chapter_three(notes: LayerDescriptor, index: LayerDescriptor) -> Inscription {
    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east", "curator": {"name": "example", "since": 1998}}),
        json!({"noteWidth": 40, "titleOrder": "byte"}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.previous = Some(stelae::Link {
        sequence: 2,
        inscription_digest: stelae::Digest::from_bytes([0x22; 32]),
    });

    // The notes are a closed chapter every later snapshot inherits; the index
    // is rebuilt whole each time, so it belongs to this snapshot alone.
    inscription.diff = vec![notes];
    inscription.tip = vec![index];

    inscription
}

#[test]
fn writes_a_snapshot_and_reads_it_back() {
    let temp = tempfile::tempdir().unwrap();
    let (written, digest) = write_snapshot(temp.path());

    // On-disk shape: an image layout, and nothing beside it. The blobs are
    // the two layers, the inscription, and the diff and tip manifests.
    assert!(temp.path().join("oci-layout").is_file());
    assert!(temp.path().join("index.json").is_file());
    assert!(!temp.path().join("inscription.json").exists());
    assert!(!temp.path().join("blobs.json").exists());
    let blobs = temp.path().join("blobs").join("sha256");
    assert_eq!(std::fs::read_dir(&blobs).unwrap().count(), 5);

    let snapshot = pulled(temp.path());
    let read = snapshot.read_inscription().unwrap();

    assert_eq!(read, written);
    assert_eq!(read.digest().unwrap(), digest);
    read.check_profile(&ToyProfile).unwrap();

    // The layers come back through the same identity the inscription pins.
    let index = snapshot.blob_index().unwrap();
    assert_eq!(index.len(), 2);

    let notes_descriptor = read.layers_of_kind("notes").next().unwrap();
    let layer = snapshot
        .read_layer(&index, &ToyProfile, notes_descriptor)
        .unwrap();

    assert_eq!(layer.header().profile, PROFILE_NAME);
    assert_eq!(layer.header().kind, "notes");
    assert_eq!(layer.header().scope, notes_scopes().0);
    assert_eq!(layer.digests().diff_id, notes_descriptor.diff_id);

    let records: Vec<&[u8]> = layer.records().collect::<Result<_, _>>().unwrap();
    assert_eq!(records.len(), NOTES.len());

    for (record, note) in records.iter().zip(NOTES) {
        assert_eq!(*record, note_record(note).as_bytes());
    }

    // And the profile can decode its own records, which the protocol never does.
    let mut decoder = minicbor::Decoder::new(records[1]);
    assert_eq!(decoder.array().unwrap(), Some(3));
    assert_eq!(decoder.u64().unwrap(), 2);
    assert_eq!(decoder.str().unwrap(), "on determinism");
    assert_eq!(decoder.bytes().unwrap(), b"two publishers, one digest");

    // The second layer reads back the same way.
    let index_descriptor = read.layers_of_kind("index").next().unwrap();
    let index_layer = snapshot
        .read_layer(&index, &ToyProfile, index_descriptor)
        .unwrap();
    assert_eq!(index_layer.header().kind, "index");
    assert_eq!(index_layer.records().count(), NOTES.len());

    // And both layers read back identically without ever being held.
    for descriptor in read.layers() {
        read_both_ways(&snapshot, &index, descriptor).unwrap();
    }
}

/// The streaming reader is the one a restore uses, so the profile's records
/// have to survive it unchanged — through a window smaller than any of them,
/// which is the case that has to work for a 400 MB layer to be readable at all.
#[test]
fn a_layer_streams_back_record_for_record() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();

    let notes_descriptor = inscription.layers_of_kind("notes").next().unwrap();

    let mut reader = snapshot
        .stream_layer(
            &index,
            &ToyProfile,
            notes_descriptor,
            Limits {
                window: 4,
                ..Limits::default()
            },
        )
        .unwrap();

    assert_eq!(reader.header().profile, PROFILE_NAME);
    assert_eq!(reader.header().kind, "notes");
    assert_eq!(reader.header().scope, notes_scopes().0);

    let mut notes = NOTES.iter();
    while let Some(record) = reader.next_record() {
        let expected = notes.next().expect("no more records were written");
        assert_eq!(record.unwrap(), note_record(expected).as_bytes());
    }
    assert!(notes.next().is_none(), "every record came back");

    // Only now is the layer proven. Everything above was read on the strength
    // of the descriptor, which is the contract this reader makes explicit.
    let digests = reader.finish().unwrap();
    assert_eq!(digests.diff_id, notes_descriptor.diff_id);
    assert_eq!(
        digests.uncompressed_size,
        notes_descriptor.uncompressed_size
    );
}

/// A reader with nothing to fetch ahead streams the layers `stream_layer`
/// reads, in the order the caller names them and whatever the depth, and
/// stops at the first layer it cannot read.
#[test]
fn a_directory_streams_layers_as_stream_layer_reads_them() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();

    let read = |mut reader: LayerReader<std::fs::File>| {
        let header = reader.header().clone();
        let mut records = Vec::new();
        while let Some(record) = reader.next_record() {
            records.push(record.unwrap().to_vec());
        }

        (header, records, reader.finish().unwrap())
    };

    // Back to front, so the order is the caller's and not the inscription's.
    let mut named: Vec<LayerDescriptor> = inscription.layers().cloned().collect();
    named.reverse();

    let looped: Vec<_> = named
        .iter()
        .map(|descriptor| {
            read(
                snapshot
                    .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
                    .unwrap(),
            )
        })
        .collect();

    for ahead in [0, 1, 8] {
        let streamed: Vec<_> = snapshot
            .stream_layers(&index, &ToyProfile, named.clone(), Limits::default(), ahead)
            .map(|reader| read(reader.unwrap()))
            .collect();

        assert_eq!(streamed, looped, "depth {ahead}");
    }

    let mut absent = inscription.diff[0].clone();
    absent.diff_id = stelae::Digest::from_bytes([0xab; 32]);

    let mut stream = snapshot.stream_layers(
        &index,
        &ToyProfile,
        vec![
            inscription.diff[0].clone(),
            absent,
            inscription.tip[0].clone(),
        ],
        Limits::default(),
        8,
    );

    assert!(stream.next().unwrap().is_ok());
    assert!(matches!(
        stream.next(),
        Some(Err(Error::LayerNotFound { .. }))
    ));
    assert!(
        stream.next().is_none(),
        "a layer was handed over after an error"
    );
}

/// `finish` is the confirmation, and it does not depend on the caller having
/// been diligent: a reader dropped after one record proves nothing, and a
/// reader finished without consuming anything still reads the whole layer,
/// because the identity digest covers every byte either way.
#[test]
fn finish_confirms_the_layer_whether_or_not_the_records_were_read() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();
    let descriptor = &inscription.diff[0];

    // Not a single content record consumed.
    let untouched = snapshot
        .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
        .unwrap()
        .finish()
        .unwrap();

    assert_eq!(untouched.diff_id, descriptor.diff_id);

    // One record consumed, then finished. Same verdict, same digests.
    let mut reader = snapshot
        .stream_layer(&index, &ToyProfile, descriptor, Limits::default())
        .unwrap();
    reader.next_record().unwrap().unwrap();

    assert_eq!(reader.finish().unwrap(), untouched);
}

/// Done criterion 2, and the property the whole protocol rests on: the same
/// source data written twice, independently, yields the same identity.
#[test]
fn two_independent_writes_produce_the_same_inscription_digest() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let (left, left_digest) = write_snapshot(first.path());
    let (right, right_digest) = write_snapshot(second.path());

    assert_eq!(left, right);
    assert_eq!(left_digest, right_digest);
    assert_eq!(left.canonicalize().unwrap(), right.canonicalize().unwrap());

    // Byte-for-byte on disk: the index, and every blob under the same name.
    assert_eq!(
        std::fs::read(first.path().join("index.json")).unwrap(),
        std::fs::read(second.path().join("index.json")).unwrap(),
    );

    let blobs = |root: &Path| -> Vec<(String, Vec<u8>)> {
        let mut blobs: Vec<_> = std::fs::read_dir(root.join("blobs").join("sha256"))
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let name = entry.file_name().to_string_lossy().into_owned();
                (name, std::fs::read(entry.path()).unwrap())
            })
            .collect();
        blobs.sort();
        blobs
    };

    assert_eq!(blobs(first.path()), blobs(second.path()));

    for descriptor in left.layers() {
        let left_blob = pulled(first.path())
            .blob_index()
            .unwrap()
            .blob_for(&descriptor.diff_id)
            .unwrap();
        let right_blob = pulled(second.path())
            .blob_index()
            .unwrap()
            .blob_for(&descriptor.diff_id)
            .unwrap();

        assert_eq!(left_blob, right_blob, "layer {:?}", descriptor.kind);
    }
}

/// Layer bytes survive a write → read → write round trip unchanged, so a
/// republished layer keeps its identity.
#[test]
fn layers_round_trip_byte_identically() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();

    for descriptor in inscription.layers() {
        let layer = snapshot
            .read_layer(&index, &ToyProfile, descriptor)
            .unwrap();

        // Re-frame the records that came back and compare the whole sequence.
        // The header is re-encoded from its parsed form, so a field that did not
        // survive parsing would show up as a byte difference here.
        let mut rewritten = Vec::new();
        let mut writer = stelae::SeqWriter::new(&mut rewritten);

        let reencoded_header = layer.header().encode().unwrap();
        assert_eq!(reencoded_header.as_bytes(), layer.header_bytes());
        writer.write_record(&reencoded_header).unwrap();

        for record in layer.records() {
            writer
                .write_record(&CanonicalCbor::new(record.unwrap().to_vec()).unwrap())
                .unwrap();
        }

        assert_eq!(rewritten, layer.as_bytes(), "layer {:?}", descriptor.kind);
        assert_eq!(
            stelae::Digest::compute(&rewritten),
            descriptor.diff_id,
            "layer {:?} identity",
            descriptor.kind
        );
    }
}

/// The two write paths are one write path.
///
/// The same records, handed over whole and streamed a record at a time, yield
/// the same identity digest, the same blob digest, the same descriptor and the
/// same bytes on disk. `write_layer` is a wrapper over `layer_sink`, so what
/// this pins is that the wrapper stayed thin: staging, digest-naming, the
/// rename and the descriptor have one implementation, and a change that made
/// the buffered path special would have to move a digest here to land.
#[test]
fn both_write_paths_produce_the_same_layer() {
    let buffered_dir = tempfile::tempdir().unwrap();
    let streamed_dir = tempfile::tempdir().unwrap();

    let buffered_snapshot = SnapshotDir::create(buffered_dir.path()).unwrap();
    let streamed_snapshot = SnapshotDir::create(streamed_dir.path()).unwrap();

    let (header_scope, scope) = notes_scopes();
    let spec = LayerSpec::new("notes", header_scope, scope);
    let records: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let buffered = buffered_snapshot
        .write_layer(&ToyProfile, &spec, COMPRESSION_LEVEL, &records)
        .unwrap();

    let mut sink = streamed_snapshot
        .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    // A sink is a layer already in progress: the protocol's header record is
    // written before the handle is returned, so a producer only ever adds its
    // own records and the count is never off by one.
    assert_eq!(sink.records(), 1);

    for record in &records {
        sink.write_record(record).unwrap();
    }

    assert_eq!(sink.records(), 1 + NOTES.len() as u64);

    let streamed = sink.finish().unwrap();

    assert_eq!(buffered.descriptor, streamed.descriptor);
    assert_eq!(buffered.digests, streamed.digests);

    // Same blob digest means the same file name; the bytes under it are the
    // same too, which is what an OCI registry would be asked to deduplicate.
    let blob = |snapshot: &SnapshotDir, written: &WrittenLayer| {
        std::fs::read(snapshot.blob_path(&written.digests.blob_digest)).unwrap()
    };

    assert_eq!(
        blob(&buffered_snapshot, &buffered),
        blob(&streamed_snapshot, &streamed)
    );

    // And one layer in each snapshot, with no staging file beside it.
    for root in [buffered_dir.path(), streamed_dir.path()] {
        let blobs = root.join("blobs");
        assert_eq!(
            std::fs::read_dir(&blobs).unwrap().count(),
            1,
            "only sha256/"
        );
        assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 1);
    }
}

/// A layer whose blob is already on disk is deduplicated, not rewritten.
///
/// The same records under the same header scope hash to the same blob digest,
/// which is the same file name — so the second write finds its destination
/// occupied by the bytes it was about to write. Publishing it anyway would
/// rewrite hundreds of megabytes with their own contents, and on Windows would
/// collide with any reader holding the blob open. The second write is therefore
/// expected to keep the file that is there and drop its own staging copy, while
/// handing back the very same [`WrittenLayer`] the first write produced.
///
/// The proof that no bytes moved is a doctored modification time. A rename
/// replaces the file behind the name, and the timestamp belongs to the file, so
/// a mark set on the first write's blob survives only if the second write left
/// it alone — evidence that neither an equality assertion on the descriptor nor
/// a count of the directory could give on its own.
///
/// The first write goes through the layout's bare blob store and the second
/// through the layout, so the seal lists the layer once.
#[test]
fn a_blob_that_already_exists_is_deduplicated() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let (header_scope, scope) = notes_scopes();
    let spec = LayerSpec::new("notes", header_scope, scope);
    let records: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    // Byte-identical writes: same records, same header scope, same snapshot. The
    // header scope matters — it is inside the layer, so two shards of one kind
    // that differ only there are different blobs and never meet here.
    let write = |writer: &dyn Fn() -> Result<stelae::dir::LayerSink, Error>| {
        let mut sink = writer().unwrap();

        for record in &records {
            sink.write_record(record).unwrap();
        }

        sink.finish().unwrap()
    };

    let first = write(&|| {
        snapshot
            .blobs()
            .layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL)
    });
    let blob = snapshot.blob_path(&first.digests.blob_digest);
    assert!(blob.is_file());

    let mark = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000_000);
    std::fs::File::options()
        .write(true)
        .open(&blob)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(mark))
        .unwrap();

    let second = write(&|| snapshot.layer_sink(&ToyProfile, &spec, COMPRESSION_LEVEL));

    // Whatever the writer observes is what it would have observed first: the
    // descriptor and the digests come from the record stream, not from the
    // rename that did not happen.
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(first.digests, second.digests);

    // One blob, and it is the first write's file: the mark is still on it, so
    // nothing was written over it.
    assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 1);
    assert_eq!(
        std::fs::metadata(&blob).unwrap().modified().unwrap(),
        mark,
        "the blob was rewritten"
    );

    // And the duplicate staging file went with the sink that made it.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );

    // The layer the second writer describes reads back, through both readers,
    // out of the blob the first writer published.
    let snapshot = sealed(&snapshot, vec![second.descriptor.clone()]);
    let index = snapshot.blob_index().unwrap();
    assert_eq!(index.len(), 1);

    let expected: Vec<Vec<u8>> = records.iter().map(|r| r.as_bytes().to_vec()).collect();
    assert_eq!(
        read_both_ways(&snapshot, &index, &second.descriptor).unwrap(),
        expected
    );
}

/// Sixteen sinks open at once, which is the case the sink exists for.
///
/// The Dolos profile shards its state into sixteen layers and cannot walk the
/// store sixteen times, so it walks once and routes each record to the shard it
/// belongs in. That works only if a sink is an ordinary independent value: no
/// borrow of the snapshot it will land in, no shared staging name, no ordering
/// between them. Here the records interleave across all sixteen and every layer
/// still reads back — through both readers — exactly what was routed to it.
#[test]
fn sixteen_sinks_are_written_in_one_pass() {
    const SHARDS: u64 = 16;
    const RECORDS: u64 = 8 * SHARDS;

    let temp = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let mut sinks: Vec<_> = (0..SHARDS)
        .map(|shard| {
            let header_scope = encode(|e| {
                e.array(1)?.u64(shard)?;
                Ok(())
            })
            .unwrap();

            snapshot
                .layer_sink(
                    &ToyProfile,
                    &LayerSpec::new("notes", header_scope, json!({"shard": shard})),
                    COMPRESSION_LEVEL,
                )
                .unwrap()
        })
        .collect();

    let mut routed: Vec<Vec<Vec<u8>>> = vec![Vec::new(); SHARDS as usize];

    for id in 0..RECORDS {
        let shard = (id % SHARDS) as usize;
        let record = encode(|e| {
            e.array(2)?.u64(id)?.str("routed")?;
            Ok(())
        })
        .unwrap();

        sinks[shard].write_record(&record).unwrap();
        routed[shard].push(record.as_bytes().to_vec());
    }

    // Sixteen staging files beside `sha256/`, and not one layer yet: nothing is
    // published until its digest is known, which is not until `finish`.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count() as u64,
        SHARDS + 1
    );
    assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 0);

    let written: Vec<WrittenLayer> = sinks
        .into_iter()
        .map(|sink| sink.finish().unwrap())
        .collect();

    // Sixteen distinct layers, sixteen distinct blobs, nothing staged left.
    let diff_ids: BTreeSet<_> = written.iter().map(|w| w.descriptor.diff_id).collect();
    let blob_digests: BTreeSet<_> = written.iter().map(|w| w.digests.blob_digest).collect();
    assert_eq!(diff_ids.len() as u64, SHARDS);
    assert_eq!(blob_digests.len() as u64, SHARDS);
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );

    let snapshot = sealed(
        &snapshot,
        written.iter().map(|w| w.descriptor.clone()).collect(),
    );
    let index = snapshot.blob_index().unwrap();
    assert_eq!(index.len() as u64, SHARDS);

    for (shard, written) in written.iter().enumerate() {
        let records = read_both_ways(&snapshot, &index, &written.descriptor).unwrap();
        assert_eq!(records, routed[shard], "shard {shard}");
        assert_eq!(
            written.descriptor.records,
            routed[shard].len() as u64 + 1,
            "shard {shard} record count, header included"
        );
    }
}

/// A sink that is never finished leaves nothing behind.
///
/// The case is a mainnet export that fails partway: sixteen state shards open,
/// hundreds of megabytes written, and then a store iterator returns an error.
/// Nothing would ever *read* what is left — staging files sit beside `sha256/`,
/// and only a manifest names a blob — but leaving them on the disk is its own
/// incident, so the sink removes its file on the way out.
#[test]
fn a_sink_dropped_without_finishing_leaves_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let (header_scope, scope) = notes_scopes();

    {
        let mut sink = snapshot
            .layer_sink(
                &ToyProfile,
                &LayerSpec::new("notes", header_scope, scope),
                COMPRESSION_LEVEL,
            )
            .unwrap();

        for note in NOTES {
            sink.write_record(&note_record(note)).unwrap();
        }

        // Open: one staging file, which is not a blob and never was.
        let staged: Vec<_> = std::fs::read_dir(&blobs)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .collect();

        assert_eq!(staged.len(), 1, "{staged:?}");
        assert!(staged[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(stelae::dir::STAGING_PREFIX));
    }

    // Dropped: nothing staged, nothing published.
    assert_eq!(
        std::fs::read_dir(&blobs).unwrap().count(),
        1,
        "only sha256/"
    );
    assert_eq!(std::fs::read_dir(blobs.join("sha256")).unwrap().count(), 0);
}

/// Unknown #4: nothing vendor-owned in the artifact was composed by the core.
///
/// Every media type and tag in the snapshot is character-for-character what
/// `ToyProfile` returned. There is no fallback, no default and no template in
/// `stelae` that could have produced them — remove the profile and the strings
/// have no other source.
#[test]
fn the_core_composes_no_vendor_owned_string() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    assert_eq!(inscription.profile.name, ToyProfile.name());

    for descriptor in inscription.layers() {
        let from_profile = ToyProfile.layer_media_type(&descriptor.kind).unwrap();
        assert_eq!(descriptor.media_type, from_profile);
    }

    assert_eq!(
        stelae::profile::checked_tag_for_sequence(&ToyProfile, inscription.sequence).unwrap(),
        "chapter-3"
    );
    assert_eq!(ToyProfile.moving_tag(), "latest");

    // The canonical document mentions the vendor's names and never the
    // protocol's reserved one as a payload type.
    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();
    assert!(canonical.contains(NOTES_MEDIA_TYPE));
    assert!(canonical.contains(INDEX_MEDIA_TYPE));
    assert!(!canonical.contains("vnd.stelae."));
    assert!(!canonical.contains("dolos"));
    assert!(!canonical.contains("cardano"));
}

/// The three opaque fields keep whatever the profile put in them, however alien
/// — the core canonicalizes and hashes, and never types them.
#[test]
fn opaque_fields_are_carried_not_interpreted() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(temp.path()).unwrap();

    let alien = json!({
        "shelf": ["east", "west"],
        "curator": {"name": "example", "since": 1998, "active": true},
        "tags": [],
        "retired": null,
    });

    let header_scope = encode(|e| {
        e.array(2)?.str("anything")?.bool(true)?;
        Ok(())
    })
    .unwrap();

    let written = snapshot
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", header_scope.clone(), alien.clone()),
            COMPRESSION_LEVEL,
            &[note_record(&NOTES[0])],
        )
        .unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        0,
        alien.clone(),
        alien.clone(),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.diff = vec![written.descriptor];
    snapshot.seal(&ToyProfile, &inscription).unwrap();

    let snapshot = pulled(temp.path());
    let read = snapshot.read_inscription().unwrap();

    assert_eq!(read.position, alien);
    assert_eq!(read.parameters, alien);
    assert_eq!(read.diff[0].scope, alien);

    let index = snapshot.blob_index().unwrap();
    let layer = snapshot
        .read_layer(&index, &ToyProfile, &read.diff[0])
        .unwrap();
    assert_eq!(layer.header().scope, header_scope);
}

/// A client fails closed on a snapshot it cannot read, rather than restoring
/// part of it.
#[test]
fn a_foreign_profile_is_refused() {
    struct Other;

    impl Profile for Other {
        fn name(&self) -> &str {
            "com.acme.receipts"
        }
        fn version(&self) -> u64 {
            1
        }
        fn kinds(&self) -> &[&str] {
            &["receipts"]
        }
        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.acme.stelae.{kind}.v1+zstd"))
        }
        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("r-{sequence}"))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let err = inscription.check_profile(&Other).unwrap_err();
    assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");

    // A profile major version above the implemented one is refused too.
    let mut future = inscription.clone();
    future.profile.version = 2;
    let err = future.check_profile(&ToyProfile).unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedProfileVersion { .. }),
        "{err:?}"
    );

    // A layer kind the profile does not define is *not* one of these: it is
    // skippable at read and refused only on the publish side — see
    // `an_unknown_layer_kind_is_skippable_at_read_and_refused_at_publish`.
    let mut unknown_kind = inscription.clone();
    unknown_kind.diff[0].kind = "receipts".to_owned();
    unknown_kind.check_profile(&ToyProfile).unwrap();
    let err = unknown_kind.check_profile_strict(&ToyProfile).unwrap_err();
    assert!(matches!(err, Error::UnknownLayerKind { .. }), "{err:?}");
}

/// A snapshot written with the retired `.stele.` infix is refused, on both the
/// read and the publish side, with an error naming `.stelae.`.
#[test]
fn a_retired_infix_layer_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (mut inscription, _) = write_snapshot(temp.path());

    for layer in inscription.diff.iter_mut().chain(&mut inscription.tip) {
        layer.media_type = layer.media_type.replace(".stelae.", ".stele.");
    }

    let names_stelae = |err: &Error| matches!(err, Error::InvalidMediaType { reason, .. } if reason.contains("`.stelae.`"));

    let err = inscription.validate().unwrap_err();
    assert!(names_stelae(&err), "{err:?}");
    let err = inscription.check_profile(&ToyProfile).unwrap_err();
    assert!(names_stelae(&err), "{err:?}");
    let err = inscription.check_profile_strict(&ToyProfile).unwrap_err();
    assert!(names_stelae(&err), "{err:?}");
}

/// Tampering is caught on both halves of a snapshot: the document, whose bytes
/// are its digest, and the blobs, whose names are their digests.
#[test]
fn tampering_is_caught_on_read() {
    // Re-indenting the inscription leaves the content intact but changes the
    // bytes a verifier would hash. Stored under its own digest and named by a
    // manifest, it is still refused.
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());
    let layout = SnapshotDir::open(temp.path()).unwrap();

    let pretty = serde_json::to_vec_pretty(&inscription).unwrap();
    let pretty_digest = layout.blobs().put(&pretty).unwrap();

    let mut manifest = pulled(temp.path()).manifest().clone();
    manifest.config.digest = pretty_digest.to_string();
    manifest.config.size = pretty.len() as i64;
    retag(
        temp.path(),
        "latest",
        &serde_json::to_vec(&manifest).unwrap(),
    );

    let err = layout.pull_latest(&ToyProfile).unwrap_err();
    assert!(matches!(err, Error::NonCanonicalInscription), "{err:?}");

    // A manifest blob whose bytes no longer match its name is refused before
    // it is parsed.
    let temp = tempfile::tempdir().unwrap();
    write_snapshot(temp.path());
    let layout = SnapshotDir::open(temp.path()).unwrap();
    let diff = &layout.tags().unwrap()["chapter-3"];
    let path = layout.blob_path(&diff.digest.parse().unwrap());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.push(b' ');
    std::fs::write(&path, &bytes).unwrap();

    let err = layout.pull_sequence(&ToyProfile, 3).unwrap_err();
    assert!(matches!(err, Error::DigestMismatch { .. }), "{err:?}");

    // A layer blob is held to the identity the inscription pins when it is
    // read.
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());
    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();
    let blob = index.blob_for(&inscription.diff[0].diff_id).unwrap();

    let path = SnapshotDir::open(temp.path()).unwrap().blob_path(&blob);
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    assert!(snapshot
        .read_layer(&index, &ToyProfile, &inscription.diff[0])
        .is_err());
}

/// A layer whose bytes do not decompress is refused as that layer, on both
/// read paths, and a blob that could not be read keeps the reader's own error:
/// the one is the snapshot's defect, the other a disk's.
#[test]
fn a_blob_that_does_not_decompress_is_refused_as_its_layer() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();
    let descriptor = &inscription.diff[0];
    let path = SnapshotDir::open(temp.path())
        .unwrap()
        .blob_path(&index.blob_for(&descriptor.diff_id).unwrap());
    let bytes = std::fs::read(&path).unwrap();

    let read = |source: &mut dyn std::io::Read| {
        LayerReader::new(source, &ToyProfile, descriptor, Limits::default())
            .and_then(LayerReader::finish)
    };

    let refused_as_its_layer = |err: &Error| {
        matches!(err, Error::LayerMismatch { kind, reason }
            if *kind == descriptor.kind && reason.contains("does not decompress"))
    };

    let mut flipped = bytes.clone();
    flipped[0] ^= 0xff;
    let altered = [
        b"not a zstd frame".to_vec(),
        flipped,
        bytes[..bytes.len() - 1].to_vec(),
    ];

    for blob in &altered {
        let err = read(&mut blob.as_slice()).unwrap_err();
        assert!(refused_as_its_layer(&err), "{err:?}");

        // In place, where the buffered reader sees the same bytes.
        std::fs::write(&path, blob).unwrap();
        let err = read_both_ways(&snapshot, &index, descriptor).unwrap_err();
        assert!(refused_as_its_layer(&err), "{err:?}");
    }

    std::fs::write(&path, &bytes).unwrap();

    /// Fails after the first few bytes, as a disk does, with `error`.
    struct Failing<'a>(&'a [u8], fn() -> std::io::Error);

    impl std::io::Read for Failing<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.0.is_empty() {
                return Err((self.1)());
            }
            let n = buf.len().min(self.0.len()).min(8);
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    let err = read(&mut Failing(&bytes[..16], || {
        std::io::Error::from_raw_os_error(5)
    }))
    .unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.raw_os_error() == Some(5)),
        "{err:?}"
    );

    // The kinds zstd raises for bytes it cannot decode, raised by the disk
    // instead: still the disk's.
    for fault in [
        (|| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read on the device",
            )
        }) as fn() -> std::io::Error,
        || std::io::Error::new(std::io::ErrorKind::InvalidData, "bad sector"),
        || std::io::Error::other("Unknown frame descriptor"),
    ] {
        let expected = fault();
        let err = read(&mut Failing(&bytes[..16], fault)).unwrap_err();
        assert!(
            matches!(&err, Error::Io(io)
                if io.kind() == expected.kind() && io.to_string() == expected.to_string()),
            "{expected:?}: {err:?}"
        );
    }

    /// Interrupted `burst` times before every read, as a source under a storm
    /// of signals is.
    struct Interrupting<'a> {
        bytes: &'a [u8],
        burst: usize,
        pending: usize,
    }

    impl std::io::Read for Interrupting<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pending > 0 {
                self.pending -= 1;
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            self.pending = self.burst;
            let n = buf.len().min(self.bytes.len()).min(8);
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            Ok(n)
        }
    }

    // Past libzstd's limit of 16 calls in a row without input.
    for burst in [15, 16, 17, 64] {
        let digests = read(&mut Interrupting {
            bytes: &bytes,
            burst,
            pending: burst,
        })
        .unwrap();
        assert_eq!(digests.diff_id, descriptor.diff_id, "burst {burst}");
    }
}

/// The buffered reader keeps a disk's error as the disk's: a blob that opens
/// but cannot be read is not refused as its layer.
#[test]
fn a_blob_that_cannot_be_read_keeps_the_readers_error() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();
    let descriptor = &inscription.diff[0];
    let path = SnapshotDir::open(temp.path())
        .unwrap()
        .blob_path(&index.blob_for(&descriptor.diff_id).unwrap());

    // A directory opens, and its first read fails.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();

    let err = snapshot
        .read_layer(&index, &ToyProfile, descriptor)
        .unwrap_err();
    assert!(
        matches!(&err, Error::Io(io) if io.raw_os_error().is_some()),
        "{err:?}"
    );
}

/// A descriptor that lies about its layer is refused even when the blob itself
/// is intact — and refused identically whether the layer is held or streamed.
#[test]
fn a_descriptor_that_disagrees_with_its_layer_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();

    let mut wrong_size = inscription.diff[0].clone();
    wrong_size.uncompressed_size += 1;
    let err = read_both_ways(&snapshot, &index, &wrong_size).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_count = inscription.diff[0].clone();
    wrong_count.records += 1;
    let err = read_both_ways(&snapshot, &index, &wrong_count).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut wrong_kind = inscription.diff[0].clone();
    wrong_kind.kind = "index".to_owned();
    let err = read_both_ways(&snapshot, &index, &wrong_kind).unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");

    let mut absent = inscription.diff[0].clone();
    absent.diff_id = stelae::Digest::from_bytes([0xab; 32]);
    let err = read_both_ways(&snapshot, &index, &absent).unwrap_err();
    assert!(matches!(err, Error::LayerNotFound { .. }), "{err:?}");

    // A descriptor claiming *less* than the layer holds is refused during
    // decompression rather than after it, on both paths: the claim is the
    // ceiling, and a blob that expands past its own descriptor is not read to
    // the end just to be told so.
    let mut too_small = inscription.diff[0].clone();
    too_small.uncompressed_size -= 1;
    let err = read_both_ways(&snapshot, &index, &too_small).unwrap_err();
    assert!(matches!(err, Error::DecompressedTooLarge { .. }), "{err:?}");
}

/// A malformed record is not a record.
///
/// Both readers report the first bad one and then end, so *counting* their
/// items tallies the failure itself as a record and discards the error with it.
/// A publisher who sets `records` to match that inflated number would hand back
/// a corrupt layer as `Ok`, in the one place whose documented job is to check
/// everything the descriptor claims. This is the guarantee a refill loop is
/// most likely to lose, which is why it is checked on both paths.
#[test]
fn a_malformed_record_is_reported_not_counted() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let layout = SnapshotDir::open(temp.path()).unwrap();
    let snapshot = pulled(temp.path());
    let descriptor = inscription.diff[0].clone();

    // Take the layer's bytes back out and append the head of a CBOR text string
    // that never arrives — a byte the framing must refuse.
    let mut index = snapshot.blob_index().unwrap();
    let blob_digest = index.blob_for(&descriptor.diff_id).unwrap();

    let (mut content, _) = read_blob(
        std::fs::File::open(layout.blob_path(&blob_digest)).unwrap(),
        descriptor.uncompressed_size,
    )
    .unwrap();

    content.push(0x62);

    // Store it as a blob in its own right, under a descriptor claiming exactly
    // what `count()` would have said: the real records, plus the failure.
    let mut writer = LayerWriter::new(Vec::new(), COMPRESSION_LEVEL).unwrap();
    writer.write_all(&content).unwrap();
    let (blob, digests) = writer.finish().unwrap();
    std::fs::write(layout.blob_path(&digests.blob_digest), &blob).unwrap();

    let corrupt = LayerDescriptor {
        diff_id: digests.diff_id,
        uncompressed_size: digests.uncompressed_size,
        records: descriptor.records + 1,
        ..descriptor
    };

    // Placed by hand and named by no manifest, so the map is given it by hand.
    index.insert(corrupt.diff_id, digests.blob_digest);
    let err = read_both_ways(&snapshot, &index, &corrupt).unwrap_err();

    assert!(matches!(err, Error::TruncatedCbor { .. }), "{err:?}");

    // And a caller that ignores the bad record does not get a confirmation out
    // of `finish` instead. This layer is the awkward case: the malformed byte
    // is the *last* one, so every byte still reached the hasher and the digest
    // and size the descriptor claims both hold. Only the record count and the
    // reader's own memory of having failed stand between a corrupt layer and an
    // `Ok`.
    let mut reader = snapshot
        .stream_layer(&index, &ToyProfile, &corrupt, Limits::default())
        .unwrap();

    while let Some(record) = reader.next_record() {
        if record.is_err() {
            break;
        }
    }

    let err = reader.finish().unwrap_err();
    assert!(matches!(err, Error::LayerMismatch { .. }), "{err:?}");
}

/// A descriptor's media type has to be the one *this* profile defines for that
/// kind. `validate_structure` can only establish that the name is well formed
/// and does not squat the reserved vendor; whether it belongs to the profile
/// the inscription claims needs the profile in hand, which is `check_profile`.
#[test]
fn a_layer_media_type_that_is_not_the_profiles_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_snapshot(temp.path());

    let with_media_type = |media_type: &str| {
        let mut tampered = inscription.clone();
        tampered.diff[0].media_type = media_type.to_owned();
        tampered
    };

    // Another vendor's name, and this vendor's name for a different kind. Both
    // are well formed, so structural validation passes them.
    for media_type in [
        "application/vnd.other.stelae.notes.v1+zstd",
        INDEX_MEDIA_TYPE,
    ] {
        let tampered = with_media_type(media_type);
        tampered.validate().unwrap();

        let err = tampered.check_profile(&ToyProfile).unwrap_err();
        assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
    }

    // Version and codec are transport detail the profile may move within one
    // major, so they are not frozen here — only the vendor and the kind are.
    for media_type in [
        "application/vnd.example.stelae.notes.v2+zstd",
        "application/vnd.example.stelae.notes.v1+cbor",
    ] {
        with_media_type(media_type)
            .check_profile(&ToyProfile)
            .unwrap();
    }
}

/// A profile that hands back a name it does not own is stopped at the boundary,
/// before anything is written.
#[test]
fn a_profile_cannot_claim_the_protocols_namespace() {
    struct Squatter;

    impl Profile for Squatter {
        fn name(&self) -> &str {
            "dev.example.squatter"
        }
        fn version(&self) -> u64 {
            1
        }
        fn kinds(&self) -> &[&str] {
            &["notes"]
        }
        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.stelae.stelae.{kind}.v1+zstd"))
        }
        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("c-{sequence}"))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(temp.path()).unwrap();

    let (header_scope, scope) = notes_scopes();
    let err = snapshot
        .write_layer(
            &Squatter,
            &LayerSpec::new("notes", header_scope, scope),
            COMPRESSION_LEVEL,
            &[note_record(&NOTES[0])],
        )
        .unwrap_err();

    assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");

    // Nothing was written.
    assert_eq!(
        std::fs::read_dir(temp.path().join("blobs").join("sha256"))
            .unwrap()
            .count(),
        0
    );
}

/// Golden digests.
///
/// Every value below is a sha256 over bytes the spec fully determines — the
/// canonical JSON of the inscription, and the deterministic CBOR sequence of
/// each layer. Nothing platform-, timing- or compression-dependent enters them,
/// so they are stable across machines and across zstd versions.
///
/// That makes this the drift alarm for the whole encoding stack. If a change to
/// the CBOR framing, the JCS canonicalization, the schema's field names or the
/// header record's shape alters a single byte, these values move — and because
/// they *are* published identity, moving one silently is the failure the
/// protocol exists to prevent. A deliberate format change updates them in the
/// same commit that changes the spec; an accidental one shows up here first.
#[test]
fn golden_digests_pin_the_encoding() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, digest) = write_snapshot(temp.path());

    assert_eq!(
        digest.to_string(),
        "sha256:9d3014295b43fe4c6dbc4a0bf39531d8790f3b851eadb31428115c1ff365f157",
        "inscription digest drifted"
    );

    let expected_layers = [
        (
            "notes",
            "sha256:e4f2187aa877f927788b5b4d59241fa2c92de3077eae30022731b4cfba0614f8",
            4u64,
            155u64,
        ),
        (
            "index",
            "sha256:c00d73e03ccfa604f1c7ed5294f2986a4987d3400286ebda8c233503b350e502",
            4,
            77,
        ),
    ];

    for (descriptor, (kind, diff_id, records, size)) in inscription.layers().zip(expected_layers) {
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.diff_id.to_string(), diff_id, "{kind} diffId");
        assert_eq!(descriptor.records, records, "{kind} record count");
        assert_eq!(descriptor.uncompressed_size, size, "{kind} size");
    }

    // The canonical document itself, so a change to key naming or ordering is
    // visible in the diff rather than only as a moved hash.
    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();
    assert_eq!(canonical, golden_document(Some(GOLDEN_LINK)));
}

/// The same snapshot as the first inscription of its lineage: no `previous`
/// key at all, and every other byte as above.
#[test]
fn golden_digest_of_a_first_inscription() {
    let temp = tempfile::tempdir().unwrap();
    let (mut first, _) = write_snapshot(temp.path());
    first.previous = None;

    assert_eq!(
        first.digest().unwrap().to_string(),
        "sha256:b19af455ade1a795148c462c4a43a2125a57430f3a3d0cde336a3f85c83816d5",
        "first inscription digest drifted"
    );

    let canonical = String::from_utf8(first.canonicalize().unwrap()).unwrap();
    assert_eq!(canonical, golden_document(None));
}

/// The link `write_snapshot` gives its inscription, as canonical JSON.
const GOLDEN_LINK: &str = concat!(
    r#""previous":{"inscriptionDigest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","#,
    r#""sequence":2},"#,
);

/// The canonical document `write_snapshot` seals, with `link` in its sorted
/// place or no link at all.
fn golden_document(link: Option<&str>) -> String {
    [
        r#"{"compression":{"algo":"zstd","level":9},"#,
        r#""diff":[{"diffId":"sha256:e4f2187aa877f927788b5b4d59241fa2c92de3077eae30022731b4cfba0614f8","kind":"notes","#,
        r#""mediaType":"application/vnd.example.stelae.notes.v1+zstd","records":4,"#,
        r#""scope":{"chapter":3,"firstId":1,"lastId":3},"uncompressedSize":155}],"#,
        r#""parameters":{"noteWidth":40,"titleOrder":"byte"},"#,
        r#""position":{"chapter":3,"curator":{"name":"example","since":1998},"shelf":"east"},"#,
        link.unwrap_or_default(),
        r#""profile":{"name":"dev.example.toy","version":1},"schema":1,"sequence":3,"#,
        r#""tip":[{"diffId":"sha256:c00d73e03ccfa604f1c7ed5294f2986a4987d3400286ebda8c233503b350e502","kind":"index","#,
        r#""mediaType":"application/vnd.example.stelae.index.v1+zstd","records":4,"#,
        r#""scope":{"chapter":3},"uncompressedSize":77}]}"#,
    ]
    .concat()
}

/// The discarding writer is faithful, not merely fast.
///
/// The same records through both write halves: one into a directory, one into
/// nothing. Every field of the descriptor and every one of the four digests and
/// sizes has to agree — including the *blob* digest and the compressed size,
/// which only exist if zstd actually ran. That is the assertion this test is
/// for: a discarding writer that skipped compression would still reproduce
/// `diffId`, `records` and `uncompressedSize`, and would be exactly as wrong as
/// one that never ran at all.
///
/// The seal is compared too, since a reproduction reports an identity: a
/// directory's comes from the inscription blob it stored, and this one from
/// the document in hand.
#[test]
fn a_discarding_writer_reproduces_what_a_directory_stores() {
    let temp = tempfile::tempdir().unwrap();
    let (stored, stored_digest) = write_snapshot(temp.path());

    let (notes_header_scope, notes_scope) = notes_scopes();
    let (index_header_scope, index_scope) = index_scopes();

    let notes: Vec<CanonicalCbor> = NOTES.iter().map(note_record).collect();

    let reproduced_notes = Discarding
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", notes_header_scope, notes_scope),
            COMPRESSION_LEVEL,
            &notes,
        )
        .unwrap();

    let mut sorted: Vec<&Note> = NOTES.iter().collect();
    sorted.sort_by_key(|n| n.title);

    let mut index_sink = Discarding
        .layer_sink(
            &ToyProfile,
            &LayerSpec::new("index", index_header_scope, index_scope),
            COMPRESSION_LEVEL,
        )
        .unwrap();

    for note in sorted {
        index_sink.write_record(&index_record(note)).unwrap();
    }

    let reproduced_index = index_sink.finish().unwrap();

    // The stored snapshot's own blob digests, recovered the way a directory has
    // to: by hashing the files it holds. Nothing in an inscription carries
    // them, which is the point — they are transport, and a reproduction that
    // agreed on identity while disagreeing on the compressed bytes would still
    // publish a different blob.
    let stored_blobs: BTreeSet<String> =
        std::fs::read_dir(temp.path().join("blobs").join(stelae::Digest::ALGORITHM))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

    for (stored, reproduced) in stored.layers().zip([&reproduced_notes, &reproduced_index]) {
        assert_eq!(
            *stored, reproduced.descriptor,
            "{}: the descriptor a publish would have written",
            stored.kind,
        );

        assert!(
            stored_blobs.contains(&reproduced.digests.blob_digest.to_hex()),
            "{}: the reproduction named a blob the directory does not hold ({})",
            stored.kind,
            reproduced.digests.blob_digest,
        );

        let on_disk = std::fs::metadata(
            temp.path()
                .join("blobs")
                .join(stelae::Digest::ALGORITHM)
                .join(reproduced.digests.blob_digest.to_hex()),
        )
        .unwrap()
        .len();

        assert_eq!(
            reproduced.digests.compressed_size, on_disk,
            "{}: the compressed size only exists if zstd ran",
            stored.kind,
        );
    }

    // And the identity, over a document assembled exactly as `write_snapshot`
    // assembles it.
    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east", "curator": {"name": "example", "since": 1998}}),
        json!({"noteWidth": 40, "titleOrder": "byte"}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.previous = stored.previous;
    inscription.diff = vec![reproduced_notes.descriptor.clone()];
    inscription.tip = vec![reproduced_index.descriptor.clone()];

    assert_eq!(
        Discarding.seal(&ToyProfile, &inscription).unwrap(),
        stored_digest,
    );

    // Nothing was written anywhere on the way: the only snapshot on disk is the
    // one the directory wrote, with its two layers, its inscription and its two
    // manifests.
    assert_eq!(stored_blobs.len(), 5);
}

/// The same vendor, one kind ahead: `dev.example.toy` after it started
/// publishing cover art. Same profile name and same major version, because an
/// *additive* kind is exactly the change that does not break a reader — which
/// is the claim the tests below check rather than assume.
struct FutureToyProfile;

impl Profile for FutureToyProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index", "covers"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        match kind {
            "covers" => Ok(COVERS_MEDIA_TYPE.to_owned()),
            other => ToyProfile.layer_media_type(other),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        ToyProfile.tag_for_sequence(sequence)
    }
}

/// The toy snapshot as the newer publisher writes it, with its `covers` layer.
///
/// Through the ordinary writer and the ordinary seal, so what the assertions
/// read back is a snapshot somebody could have published — not an inscription
/// with a descriptor pasted into it, which would prove nothing about the layer
/// being real.
fn published_ahead(root: &Path, scope: serde_json::Value) -> Inscription {
    let snapshot = SnapshotDir::create(root).unwrap();
    let (notes, index) = write_layers(&snapshot, &FutureToyProfile);
    let mut inscription = chapter_three(notes.descriptor, index.descriptor);

    let header = encode(|e| {
        e.map(1)?.str("chapter")?.u64(3)?;
        Ok(())
    })
    .unwrap();

    let cover = encode(|e| {
        e.str("a woodcut of the east shelf")?;
        Ok(())
    })
    .unwrap();

    let written = snapshot
        .write_layer(
            &FutureToyProfile,
            &LayerSpec::new("covers", header, scope),
            COMPRESSION_LEVEL,
            &[cover],
        )
        .unwrap();

    inscription.diff.push(written.descriptor);
    snapshot.seal(&FutureToyProfile, &inscription).unwrap();

    inscription
}

/// The blast radius of an additive kind, in one test.
///
/// A profile that gains a kind publishes it as a new media type on a new layer.
/// If an older reader refused the whole snapshot over it, every additive change
/// would brick every deployed reader; so the reader takes the document, keeps
/// the layers it models, and *reports* the one it does not. The publish side
/// keeps the old rule, because a publisher attests every layer it lists.
#[test]
fn an_unknown_layer_kind_is_skippable_at_read_and_refused_at_publish() {
    let temp = tempfile::tempdir().unwrap();
    let ahead = published_ahead(temp.path(), json!({"chapter": 3}));

    // The older reader takes the document.
    ahead.check_profile(&ToyProfile).unwrap();

    // And the layer it cannot model comes back whole — kind and scope — which
    // is what leaves the skip-or-refuse decision with the profile rather than
    // with the protocol.
    let unknown = ahead.unknown_layers(&ToyProfile);
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].kind, "covers");
    assert_eq!(unknown[0].scope, json!({"chapter": 3}));

    // The publisher that wrote it skips nothing, and may chain onto it.
    assert!(ahead.unknown_layers(&FutureToyProfile).is_empty());
    ahead.check_profile_strict(&FutureToyProfile).unwrap();

    // The older binary may not.
    let err = ahead.check_profile_strict(&ToyProfile).unwrap_err();
    assert!(
        matches!(&err, Error::UnknownLayerKind { kind, .. } if kind == "covers"),
        "{err:?}"
    );

    // Skipping is about consumption and nothing else: the layers the old reader
    // does model are still reachable through the identities the inscription
    // pins, and the skipped one is still a layer of the snapshot.
    let snapshot = pulled(temp.path());
    let index = snapshot.blob_index().unwrap();
    assert_eq!(index.len(), 3);

    for kind in ["notes", "index"] {
        let descriptor = ahead.layers_of_kind(kind).next().unwrap();
        snapshot
            .read_layer(&index, &ToyProfile, descriptor)
            .unwrap();
    }
}

/// `required: true` in a layer's scope is a publisher telling older readers
/// that this layer is not optional: refuse the snapshot rather than restore a
/// partial one.
///
/// The protocol never reads the flag — a scope is profile-owned and opaque — so
/// the planner below is the whole of the profile side, written out to show how
/// little `unknown_layers` leaves it to do.
#[test]
fn a_required_unknown_layer_is_the_profiles_own_refusal() {
    fn plan(inscription: &Inscription) -> Result<Vec<String>, String> {
        let unknown = inscription.unknown_layers(&ToyProfile);

        match unknown
            .iter()
            .find(|layer| layer.scope.get("required") == Some(&json!(true)))
        {
            Some(layer) => Err(format!("{} is required: {}", layer.kind, layer.scope)),
            None => Ok(unknown.iter().map(|layer| layer.kind.clone()).collect()),
        }
    }

    let optional = tempfile::tempdir().unwrap();
    let skipped = plan(&published_ahead(optional.path(), json!({"chapter": 3}))).unwrap();
    assert_eq!(skipped, vec!["covers".to_owned()]);

    // The same snapshot, the same reader, one flag apart.
    let required = tempfile::tempdir().unwrap();
    let refusal = plan(&published_ahead(
        required.path(),
        json!({"chapter": 3, "required": true}),
    ))
    .unwrap_err();

    assert!(refusal.contains("covers"), "{refusal}");
    assert!(refusal.contains("chapter"), "{refusal}");

    // `required` is a scope field like any other to the protocol: it neither
    // makes the layer known nor stops the document being read.
    pulled(required.path())
        .read_inscription()
        .unwrap()
        .check_profile(&ToyProfile)
        .unwrap();
}

/// Seal `diff` as chapter 0 of `snapshot` and pull it back.
fn sealed(snapshot: &SnapshotDir, diff: Vec<LayerDescriptor>) -> Snapshot {
    let mut inscription = Inscription::new(
        &ToyProfile,
        0,
        json!({}),
        json!({}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.diff = diff;

    snapshot.seal(&ToyProfile, &inscription).unwrap();
    snapshot.pull_sequence(&ToyProfile, 0).unwrap()
}

const REF_NAME: &str = "org.opencontainers.image.ref.name";

/// Rewrite the entries of `root`'s `index.json` by hand, as a writer that
/// skipped every check would.
fn edit_index(root: &Path, edit: impl FnOnce(&mut Vec<serde_json::Value>)) {
    let path = root.join("index.json");
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();

    let mut entries = index["manifests"].as_array().unwrap().clone();
    edit(&mut entries);
    index["manifests"] = json!(entries);

    std::fs::write(&path, serde_json::to_vec(&index).unwrap()).unwrap();
}

/// Store `manifest` in the layout at `root` and point `tag` at it.
fn retag(root: &Path, tag: &str, manifest: &[u8]) {
    let digest = SnapshotDir::open(root)
        .unwrap()
        .blobs()
        .put(manifest)
        .unwrap();

    edit_index(root, |entries| {
        let entry = entries
            .iter_mut()
            .find(|entry| entry["annotations"][REF_NAME] == tag)
            .unwrap();
        entry["digest"] = json!(digest.to_string());
        entry["size"] = json!(manifest.len());
    });
}

fn chapter_scope(sequence: u64) -> CanonicalCbor {
    encode(|e| {
        e.u64(sequence)?;
        Ok(())
    })
    .unwrap()
}

/// Chapter `sequence`'s own notes, final once written.
fn chapter_notes(sequence: u64) -> Vec<CanonicalCbor> {
    (1..=3)
        .map(|n| {
            encode(|e| {
                e.array(3)?
                    .u64(sequence * 10 + n)?
                    .str("a chapter note")?
                    .bytes(b"inscribed")?;
                Ok(())
            })
            .unwrap()
        })
        .collect()
}

/// An index of every chapter up to `sequence`, rebuilt whole each time.
fn chapter_index(sequence: u64) -> Vec<CanonicalCbor> {
    (1..=sequence)
        .map(|chapter| {
            encode(|e| {
                e.array(2)?.str("chapter")?.u64(chapter)?;
                Ok(())
            })
            .unwrap()
        })
        .collect()
}

/// Chapter `sequence` of one lineage, sealed into `layout` after `previous`:
/// its notes in `diff` and its index in `tip`, under parameters of its own.
fn seal_chapter(
    layout: &SnapshotDir,
    sequence: u64,
    previous: Option<&Inscription>,
) -> Inscription {
    let scope = json!({"chapter": sequence});

    let notes = layout
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("notes", chapter_scope(sequence), scope.clone()),
            COMPRESSION_LEVEL,
            &chapter_notes(sequence),
        )
        .unwrap();

    let index = layout
        .write_layer(
            &ToyProfile,
            &LayerSpec::new("index", chapter_scope(sequence), scope.clone()),
            COMPRESSION_LEVEL,
            &chapter_index(sequence),
        )
        .unwrap();

    let mut inscription = Inscription::new(
        &ToyProfile,
        sequence,
        scope,
        json!({"noteWidth": 40 + sequence}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );
    inscription.previous = link_for(previous, sequence).unwrap();
    inscription.diff = vec![notes.descriptor];
    inscription.tip = vec![index.descriptor];

    layout.seal(&ToyProfile, &inscription).unwrap();

    inscription
}

/// Chapters 1 to `last`, sealed into one layout at `root`.
fn seal_lineage(root: &Path, last: u64) -> (SnapshotDir, Vec<Inscription>) {
    let layout = SnapshotDir::create(root).unwrap();
    let mut published: Vec<Inscription> = Vec::new();

    for sequence in 1..=last {
        let chapter = seal_chapter(&layout, sequence, published.last());
        published.push(chapter);
    }

    (layout, published)
}

fn bytes_of(records: Vec<CanonicalCbor>) -> Vec<Vec<u8>> {
    records.iter().map(|r| r.as_bytes().to_vec()).collect()
}

/// A three-sequence chain sealed into one directory resolves out of it whole:
/// every inscription back to the first, the layer set in order, each layer's
/// own inscription, a blob index across all of it, and the records, on both
/// read paths.
#[test]
fn a_three_sequence_chain_round_trips_through_a_directory() {
    let temp = tempfile::tempdir().unwrap();
    let (layout, published) = seal_lineage(temp.path(), 3);

    let tags: Vec<String> = layout.tags().unwrap().into_keys().collect();
    assert_eq!(
        tags,
        [
            "chapter-1",
            "chapter-1.tip",
            "chapter-2",
            "chapter-2.tip",
            "chapter-3",
            "chapter-3.tip",
            "latest"
        ]
    );

    let tags = layout.tags().unwrap();
    assert_eq!(tags["latest"].digest, tags["chapter-3"].digest);

    let snapshot = SnapshotDir::open(temp.path())
        .unwrap()
        .resolve_latest(&ToyProfile)
        .unwrap();
    let chain = snapshot.chain().unwrap();

    assert_eq!(chain.inscriptions(), &published[..]);
    assert_eq!(chain.identity(), published[2].digest().unwrap());
    assert_eq!(snapshot.read_inscription().unwrap(), published[2]);

    let layers: Vec<LayerDescriptor> = chain.descriptors().cloned().collect();
    let expected: Vec<LayerDescriptor> = published
        .iter()
        .map(|chapter| chapter.diff[0].clone())
        .chain([published[2].tip[0].clone()])
        .collect();
    assert_eq!(layers, expected);

    for layer in chain.layers() {
        assert_eq!(
            chain.origin(layer).unwrap().parameters,
            json!({"noteWidth": 40 + layer.sequence}),
        );
    }

    let index = snapshot.blob_index().unwrap();
    assert_eq!(index.len(), 4);

    let records: Vec<Vec<Vec<u8>>> = published
        .iter()
        .map(|chapter| bytes_of(chapter_notes(chapter.sequence)))
        .chain([bytes_of(chapter_index(3))])
        .collect();

    let read: Vec<_> = layers
        .iter()
        .map(|layer| read_both_ways(&snapshot, &index, layer).unwrap())
        .collect();
    assert_eq!(read, records);

    let streamed: Vec<Vec<Vec<u8>>> = snapshot
        .stream_layers(&index, &ToyProfile, layers.clone(), Limits::default(), 4)
        .map(|reader| {
            let mut reader = reader.unwrap();
            let mut records = Vec::new();
            while let Some(record) = reader.next_record() {
                records.push(record.unwrap().to_vec());
            }
            reader.finish().unwrap();
            records
        })
        .collect();
    assert_eq!(streamed, records);

    let remaining = stelae::Remaining::of(&snapshot, &index, &layers).unwrap();
    assert_eq!((remaining.layers, remaining.unsized_layers), (4, 0));

    // A sequence inside the chain resolves to its own tip.
    let second = layout.resolve_sequence(&ToyProfile, 2).unwrap();
    let chain = second.chain().unwrap();
    assert_eq!(chain.inscriptions(), &published[..2]);
    assert_eq!(chain.descriptors().last(), Some(&published[1].tip[0]));

    // A pull follows no link.
    let head = layout.pull_latest(&ToyProfile).unwrap();
    assert!(head.chain().is_none());
    assert_eq!(head.blob_index().unwrap().len(), 2);
}

/// `oci-layout` and `index.json` have the OCI image layout's shape, and every
/// blob is named by its digest: what a generic OCI tool expects of a layout.
#[test]
fn a_directory_is_an_oci_image_layout() {
    let temp = tempfile::tempdir().unwrap();
    seal_lineage(temp.path(), 3);
    let root = temp.path();

    let names = |dir: &Path| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };

    assert_eq!(names(root), ["blobs", "index.json", "oci-layout"]);
    assert_eq!(names(&root.join("blobs")), ["sha256"]);

    let layout: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("oci-layout")).unwrap()).unwrap();
    assert_eq!(layout, json!({"imageLayoutVersion": "1.0.0"}));

    let index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("index.json")).unwrap()).unwrap();
    assert_eq!(index["schemaVersion"], 2);
    assert_eq!(
        index["mediaType"],
        "application/vnd.oci.image.index.v1+json"
    );

    let blob = |descriptor: &serde_json::Value| -> Vec<u8> {
        let digest = descriptor["digest"].as_str().unwrap();
        let hex = digest.strip_prefix("sha256:").unwrap();
        assert_eq!(hex.len(), 64, "{digest}");

        let bytes = std::fs::read(root.join("blobs").join("sha256").join(hex)).unwrap();
        assert_eq!(
            bytes.len() as u64,
            descriptor["size"].as_u64().unwrap(),
            "{digest}"
        );
        bytes
    };

    let entries = index["manifests"].as_array().unwrap();
    assert_eq!(entries.len(), 7);

    for entry in entries {
        let tag = entry["annotations"][REF_NAME].as_str().unwrap();
        assert_eq!(
            entry["mediaType"], "application/vnd.oci.image.manifest.v1+json",
            "{tag}"
        );
        assert_eq!(
            entry["artifactType"], "application/vnd.stelae.snapshot.v1",
            "{tag}"
        );

        let manifest: serde_json::Value = serde_json::from_slice(&blob(entry)).unwrap();
        assert_eq!(manifest["schemaVersion"], 2, "{tag}");
        assert_eq!(
            manifest["mediaType"], "application/vnd.oci.image.manifest.v1+json",
            "{tag}"
        );

        blob(&manifest["config"]);

        for layer in manifest["layers"].as_array().unwrap() {
            blob(layer);
        }
    }

    for name in names(&root.join("blobs").join("sha256")) {
        let bytes = std::fs::read(root.join("blobs").join("sha256").join(&name)).unwrap();
        assert_eq!(Digest::compute(&bytes).to_hex(), name);
    }
}

/// Sealing the next sequence into a layout that holds the one before it
/// appends to it, through a handle opened afresh. A sequence's tags do not
/// move, and sealing the head's manifests again changes nothing.
#[test]
fn a_seal_appends_and_never_moves_a_sequence_tag() {
    let temp = tempfile::tempdir().unwrap();
    let first = seal_chapter(&SnapshotDir::create(temp.path()).unwrap(), 1, None);

    let layout = SnapshotDir::create(temp.path()).unwrap();
    assert_eq!(layout.tags().unwrap().len(), 3);

    let second = seal_chapter(&layout, 2, Some(&first));
    let tags = layout.tags().unwrap();
    assert_eq!(tags.len(), 5);
    assert_eq!(
        layout
            .resolve_latest(&ToyProfile)
            .unwrap()
            .chain()
            .unwrap()
            .inscriptions(),
        &[first.clone(), second.clone()]
    );

    // The same chapter again writes the same manifests.
    assert_eq!(seal_chapter(&layout, 2, Some(&first)), second);
    assert_eq!(layout.tags().unwrap(), tags);

    // Another chapter 2 is refused before anything is tagged.
    let index_before = std::fs::read(temp.path().join("index.json")).unwrap();

    let revised = layout
        .write_layer(
            &ToyProfile,
            &LayerSpec::new(
                "notes",
                chapter_scope(2),
                json!({"chapter": 2, "revised": true}),
            ),
            COMPRESSION_LEVEL,
            &chapter_notes(20),
        )
        .unwrap();

    let mut other = second.clone();
    other.diff = vec![revised.descriptor];
    other.tip = vec![];

    let err = layout.seal(&ToyProfile, &other).unwrap_err();
    assert!(
        matches!(&err, Error::TagTaken { tag, .. } if tag == "chapter-2"),
        "{err:?}"
    );
    assert_eq!(
        std::fs::read(temp.path().join("index.json")).unwrap(),
        index_before
    );
}

/// The three failures of a directory read stay apart, as a registry's do: an
/// absent head is absence, and a gone tip and a document missing inside the
/// chain are not.
#[test]
fn a_directory_tells_absence_a_gone_tip_and_a_broken_chain_apart() {
    let temp = tempfile::tempdir().unwrap();
    let (layout, _) = seal_lineage(temp.path(), 3);

    let err = layout.resolve_sequence(&ToyProfile, 4).unwrap_err();
    assert!(
        matches!(&err, Error::NoSuchTag { tag } if tag == "chapter-4"),
        "{err:?}"
    );
    assert!(err.is_absent());

    // A tip manifest deleted, as a registry may delete one: only its own
    // sequence is lost.
    edit_index(temp.path(), |entries| {
        entries.retain(|entry| entry["annotations"][REF_NAME] != "chapter-3.tip")
    });

    for err in [
        layout.pull_latest(&ToyProfile).unwrap_err(),
        layout.resolve_latest(&ToyProfile).unwrap_err(),
    ] {
        assert!(
            matches!(&err, Error::TipGone { sequence: 3, tag } if tag == "chapter-3.tip"),
            "{err:?}"
        );
        assert!(!err.is_absent());
    }

    layout.resolve_sequence(&ToyProfile, 2).unwrap();

    // An inscription gone inside the chain.
    let first = &layout.tags().unwrap()["chapter-1"];
    let manifest: stelae::manifest::Manifest = serde_json::from_slice(
        &std::fs::read(layout.blob_path(&first.digest.parse().unwrap())).unwrap(),
    )
    .unwrap();
    std::fs::remove_file(layout.blob_path(&manifest.config.digest.parse().unwrap())).unwrap();

    let err = layout.resolve_sequence(&ToyProfile, 2).unwrap_err();
    assert!(
        matches!(
            err,
            Error::MissingLink {
                from: 2,
                sequence: 1
            }
        ),
        "{err:?}"
    );
    assert!(!err.is_absent());

    // A diff manifest untagged inside the chain is the same failure, and the
    // head alone still reads.
    edit_index(temp.path(), |entries| {
        entries.retain(|entry| entry["annotations"][REF_NAME] != "chapter-1")
    });

    let err = layout.resolve_sequence(&ToyProfile, 2).unwrap_err();
    assert!(
        matches!(
            err,
            Error::MissingLink {
                from: 2,
                sequence: 1
            }
        ),
        "{err:?}"
    );
    layout.pull_sequence(&ToyProfile, 2).unwrap();
}

/// A blob store stages under a `.staging-` name in `blobs/` and commits by
/// digest. An abandoned staging file is removed, a commit onto a blob that is
/// there keeps it, and a seal stores the inscription and tags nothing.
#[test]
fn a_blob_store_stages_and_commits_by_digest() {
    let temp = tempfile::tempdir().unwrap();
    let store = BlobStore::create(temp.path()).unwrap();
    let blobs = temp.path().join("blobs");

    let count = |dir: &Path| std::fs::read_dir(dir).unwrap().count();

    let bytes = b"a blob";
    let digest = Digest::compute(bytes);

    {
        let mut staged = store.stage().unwrap();
        staged.write_all(bytes).unwrap();

        let staging: Vec<String> = std::fs::read_dir(&blobs)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name != "sha256")
            .collect();
        assert_eq!(staging.len(), 1, "{staging:?}");
        assert!(staging[0].starts_with(stelae::dir::STAGING_PREFIX));
    }

    assert_eq!(count(&blobs), 1, "only sha256/");
    assert_eq!(count(&blobs.join("sha256")), 0);

    let mut staged = store.stage().unwrap();
    staged.write_all(bytes).unwrap();
    staged.commit(&digest).unwrap();
    assert_eq!(std::fs::read(store.blob_path(&digest)).unwrap(), bytes);

    assert_eq!(store.put(bytes).unwrap(), digest);

    let mut staged = store.stage().unwrap();
    staged.write_all(bytes).unwrap();
    staged.commit(&digest).unwrap();

    assert_eq!(count(&blobs), 1, "only sha256/");
    assert_eq!(count(&blobs.join("sha256")), 1);

    let (notes, index) = write_layers(&store, &ToyProfile);
    assert!(store.blob_path(&notes.digests.blob_digest).is_file());

    let inscription = chapter_three(notes.descriptor, index.descriptor);
    let identity = store.seal(&ToyProfile, &inscription).unwrap();

    assert_eq!(identity, inscription.digest().unwrap());
    assert_eq!(
        std::fs::read(store.blob_path(&identity)).unwrap(),
        inscription.canonicalize().unwrap()
    );
    assert!(!temp.path().join("index.json").exists());
    assert!(!temp.path().join("oci-layout").exists());
}

/// A directory without `oci-layout`, or with another layout version, is not
/// read as a layout, and neither is one whose `index.json` names a tag twice.
#[test]
fn a_directory_that_is_not_an_image_layout_is_refused() {
    let temp = tempfile::tempdir().unwrap();

    let err = SnapshotDir::open(temp.path()).unwrap_err();
    assert!(matches!(err, Error::Layout(_)), "{err:?}");

    std::fs::write(
        temp.path().join("oci-layout"),
        br#"{"imageLayoutVersion":"2.0.0"}"#,
    )
    .unwrap();

    for err in [
        SnapshotDir::open(temp.path()).unwrap_err(),
        SnapshotDir::create(temp.path()).unwrap_err(),
    ] {
        assert!(matches!(err, Error::Layout(_)), "{err:?}");
    }

    let temp = tempfile::tempdir().unwrap();
    let (layout, _) = seal_lineage(temp.path(), 1);

    edit_index(temp.path(), |entries| {
        let twice = entries[0].clone();
        entries.push(twice);
    });

    let err = layout.pull_latest(&ToyProfile).unwrap_err();
    assert!(matches!(err, Error::Layout(_)), "{err:?}");
}

/// A layer an earlier attempt wrote is carried into a seal by its blob, as a
/// resumed publish carries it, and a blob that is not here is not carried.
#[test]
fn a_carried_layer_is_sealed_without_being_written_again() {
    let temp = tempfile::tempdir().unwrap();
    let layout = SnapshotDir::create(temp.path()).unwrap();

    // Written into the store alone, so no seal lists them yet.
    let (notes, index) = write_layers(layout.blobs(), &ToyProfile);
    let inscription = chapter_three(notes.descriptor.clone(), index.descriptor.clone());

    let err = layout.seal(&ToyProfile, &inscription).unwrap_err();
    assert!(matches!(err, Error::ManifestMismatch(_)), "{err:?}");

    let mut absent = notes.clone();
    absent.digests.blob_digest = Digest::from_bytes([0xee; 32]);
    assert!(!layout.adopt_carried(absent).unwrap());

    let mut resized = notes.clone();
    resized.digests.compressed_size += 1;
    assert!(!layout.adopt_carried(resized).unwrap());

    assert!(layout.adopt_carried(notes).unwrap());
    assert!(layout.adopt_carried(index).unwrap());

    let digest = layout.seal(&ToyProfile, &inscription).unwrap();

    let written = tempfile::tempdir().unwrap();
    assert_eq!(digest, write_snapshot(written.path()).1);

    let snapshot = layout.pull_latest(&ToyProfile).unwrap();
    let index = snapshot.blob_index().unwrap();

    for descriptor in inscription.layers() {
        read_both_ways(&snapshot, &index, descriptor).unwrap();
    }
}
