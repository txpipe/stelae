//! A `blocks` layer end to end: the profile's names against the protocol's
//! rules, a pinned golden, and a write and read through a directory.

use std::fs::File;

use stelae::{
    dir::SnapshotDir,
    profile::{checked_layer_media_type, checked_tag_for_sequence, validate_profile_name},
    CanonicalCbor, Discarding, LayerSpec, MediaType, Profile, RecordSink, SnapshotWriter,
};
use stelae_blocks::{
    record, write_layer, BlockRecord, BlocksProfile, BlocksReader, EpochScope, Error, BLOCKS,
    BLOCKS_MEDIA_TYPE, COMPRESSION_LEVEL, PROFILE_NAME,
};

/// Preview's magic, epoch 3, its 86,400 slots.
fn scope() -> EpochScope {
    EpochScope::new(2, 3, 259_200, 345_599).unwrap()
}

/// Three blocks, the first two sharing a slot as a Byron boundary block and
/// its successor do.
fn fixture() -> Vec<BlockRecord> {
    vec![
        BlockRecord::new(259_200, [0x11; 32], b"boundary".to_vec()),
        BlockRecord::new(259_200, [0x22; 32], b"first block".to_vec()),
        BlockRecord::new(259_260, [0x33; 32], vec![0xab; 300]),
    ]
}

#[test]
fn the_profile_names_pass_the_protocol_rules() {
    let profile = BlocksProfile;

    validate_profile_name(profile.name()).unwrap();
    assert_eq!(profile.name(), PROFILE_NAME);
    assert_eq!(profile.version(), 1);
    assert_eq!(profile.kinds(), &[BLOCKS]);

    let media_type = checked_layer_media_type(&profile, BLOCKS).unwrap();
    assert_eq!(media_type, BLOCKS_MEDIA_TYPE);

    let parsed = MediaType::parse(&media_type).unwrap();
    assert_eq!(
        (parsed.vendor.as_str(), parsed.kind.as_str(), parsed.version),
        ("txpipe", BLOCKS, 1)
    );

    for (sequence, tag) in [(1, "epoch-1"), (667, "epoch-667")] {
        assert_eq!(checked_tag_for_sequence(&profile, sequence).unwrap(), tag);
    }

    assert!(checked_layer_media_type(&profile, "digests").is_err());
}

/// The layer's identity for a fixed input. A change here is a format change.
#[test]
fn the_golden_layer_keeps_its_diff_id() {
    let written = write_layer(&Discarding, scope(), COMPRESSION_LEVEL, fixture()).unwrap();
    let descriptor = written.descriptor;

    assert_eq!(descriptor.kind, BLOCKS);
    assert_eq!(descriptor.media_type, BLOCKS_MEDIA_TYPE);
    assert_eq!(descriptor.records, 4);
    assert_eq!(descriptor.scope, scope().descriptor());
    assert_eq!(descriptor.uncompressed_size, GOLDEN_SIZE);
    assert_eq!(descriptor.diff_id.to_string(), GOLDEN_DIFF_ID);
}

/// Recomputed by hand from the CBOR bytes of the header and the three
/// records, independently of this crate's encoder.
const GOLDEN_DIFF_ID: &str =
    "sha256:ad1cf6fc21f7b95ff33dca6808da01d06cccda3a1388bc59c2687f9a3d3bc982";
const GOLDEN_SIZE: u64 = 492;

#[test]
fn a_layer_reads_back_as_written() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(dir.path().join("snapshot")).unwrap();

    let written = write_layer(&snapshot, scope(), COMPRESSION_LEVEL, fixture()).unwrap();

    let blob = File::open(snapshot.blob_path(&written.digests.blob_digest)).unwrap();
    let mut reader = BlocksReader::open(blob, &written.descriptor).unwrap();

    assert_eq!(reader.scope(), &scope());
    assert_eq!(
        EpochScope::read_descriptor(2, &written.descriptor.scope).unwrap(),
        scope()
    );

    let blocks: Vec<_> = reader.by_ref().collect::<Result<_, _>>().unwrap();
    assert_eq!(blocks, fixture());

    assert_eq!(reader.finish().unwrap(), written.digests);
}

#[test]
fn the_writer_refuses_blocks_out_of_order_or_outside_the_epoch() {
    let backwards = vec![
        BlockRecord::new(259_300, [1; 32], vec![]),
        BlockRecord::new(259_299, [2; 32], vec![]),
    ];
    let err = write_layer(&Discarding, scope(), COMPRESSION_LEVEL, backwards).unwrap_err();
    assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");

    for slot in [259_199, 345_600] {
        let outside = vec![BlockRecord::new(slot, [1; 32], vec![])];
        let err = write_layer(&Discarding, scope(), COMPRESSION_LEVEL, outside).unwrap_err();
        assert!(matches!(err, Error::OutsideEpoch { .. }), "{slot}: {err:?}");
    }
}

#[test]
fn an_empty_epoch_is_a_layer_of_its_header_alone() {
    let written = write_layer(&Discarding, scope(), COMPRESSION_LEVEL, []).unwrap();
    assert_eq!(written.descriptor.records, 1);
}

/// A layer some other writer produced through a raw sink, skipping the
/// profile's checks: the header scope from `header`, the descriptor scope from
/// `descriptor`, the blocks as given. Returns the reader over it.
fn raw_layer(
    dir: &tempfile::TempDir,
    header: EpochScope,
    descriptor: EpochScope,
    blocks: &[BlockRecord],
) -> Result<BlocksReader<File>, Error> {
    raw_records(
        dir,
        header,
        descriptor.descriptor(),
        &blocks_as_records(blocks),
    )
}

fn blocks_as_records(blocks: &[BlockRecord]) -> Vec<CanonicalCbor> {
    blocks.iter().map(|b| record::encode(b).unwrap()).collect()
}

/// As `raw_layer`, with the descriptor scope and the records taken verbatim.
fn raw_records(
    dir: &tempfile::TempDir,
    header: EpochScope,
    descriptor: serde_json::Value,
    records: &[CanonicalCbor],
) -> Result<BlocksReader<File>, Error> {
    let snapshot = SnapshotDir::create(dir.path().join("snapshot")).unwrap();

    let spec = LayerSpec::new(BLOCKS, header.header().unwrap(), descriptor);
    let mut sink = snapshot
        .layer_sink(&BlocksProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    for record in records {
        sink.write_record(record).unwrap();
    }

    let written = sink.finish().unwrap();
    let blob = File::open(snapshot.blob_path(&written.digests.blob_digest)).unwrap();

    BlocksReader::open(blob, &written.descriptor)
}

fn reversed() -> Vec<BlockRecord> {
    fixture().into_iter().rev().collect()
}

/// A layer out of order is refused on read, and cannot then be confirmed.
#[test]
fn the_reader_refuses_a_layer_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut reader = raw_layer(&dir, scope(), scope(), &reversed()).unwrap();

    assert!(reader.next().unwrap().is_ok());
    let err = reader.next().unwrap().unwrap_err();
    assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");
    assert!(reader.next().is_none());
    assert!(reader.finish().is_err());
}

/// `finish` reads the blocks it was not handed through the same checks, so a
/// layer out of order is refused however little of it was iterated.
#[test]
fn finish_refuses_a_layer_out_of_order_it_did_not_iterate() {
    let dir = tempfile::tempdir().unwrap();
    let reader = raw_layer(&dir, scope(), scope(), &reversed()).unwrap();
    let err = reader.finish().unwrap_err();
    assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");

    let dir = tempfile::tempdir().unwrap();
    let mut reader = raw_layer(&dir, scope(), scope(), &reversed()).unwrap();
    assert!(reader.next().unwrap().is_ok());
    let err = reader.finish().unwrap_err();
    assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");
}

/// In order but past the epoch's last slot: refused on read and on finish.
#[test]
fn the_reader_refuses_a_block_outside_the_epoch() {
    let blocks = [
        BlockRecord::new(259_200, [1; 32], vec![]),
        BlockRecord::new(345_600, [2; 32], vec![]),
    ];

    let dir = tempfile::tempdir().unwrap();
    let mut reader = raw_layer(&dir, scope(), scope(), &blocks).unwrap();
    assert!(reader.next().unwrap().is_ok());
    let err = reader.next().unwrap().unwrap_err();
    assert!(
        matches!(err, Error::OutsideEpoch { slot: 345_600, .. }),
        "{err:?}"
    );
    assert!(reader.finish().is_err());

    let dir = tempfile::tempdir().unwrap();
    let reader = raw_layer(&dir, scope(), scope(), &blocks).unwrap();
    let err = reader.finish().unwrap_err();
    assert!(matches!(err, Error::OutsideEpoch { .. }), "{err:?}");
}

/// A header and descriptor that disagree on the epoch or on either bound are
/// refused at open.
#[test]
fn the_reader_refuses_a_header_scope_the_descriptor_does_not_name() {
    let next = EpochScope::new(2, 4, 345_600, 431_999).unwrap();
    let shifted = EpochScope::new(2, 3, 259_200, 345_600).unwrap();
    let late_start = EpochScope::new(2, 3, 259_201, 345_599).unwrap();

    for descriptor in [next, shifted, late_start] {
        let dir = tempfile::tempdir().unwrap();
        let err = raw_layer(&dir, scope(), descriptor, &fixture())
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::Stelae(stelae::Error::LayerMismatch { .. })),
            "{descriptor:?}: {err:?}"
        );
    }
}

/// A descriptor scope that is not the profile's shape is refused at open.
#[test]
fn the_reader_refuses_a_descriptor_scope_it_cannot_read() {
    let mut extra = scope().descriptor();
    extra["shard"] = 0.into();

    for descriptor in [serde_json::json!({}), extra] {
        let dir = tempfile::tempdir().unwrap();
        let records = blocks_as_records(&fixture());
        let err = raw_records(&dir, scope(), descriptor.clone(), &records)
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::MalformedScope(_)),
            "{descriptor}: {err:?}"
        );
    }
}

/// A record that is canonical CBOR but not a block is refused on read and on
/// finish, however little of the layer was iterated.
#[test]
fn the_reader_refuses_a_record_that_is_not_a_block() {
    let not_a_block = stelae::frame::encode(|e| {
        e.array(2)?.u64(259_260)?.bytes(&[0; 32])?;
        Ok(())
    })
    .unwrap();
    let mut records = blocks_as_records(&fixture()[..1]);
    records.push(not_a_block);

    let dir = tempfile::tempdir().unwrap();
    let mut reader = raw_records(&dir, scope(), scope().descriptor(), &records).unwrap();
    assert!(reader.next().unwrap().is_ok());
    let err = reader.next().unwrap().unwrap_err();
    assert!(matches!(err, Error::MalformedRecord(_)), "{err:?}");
    assert!(reader.finish().is_err());

    for iterated in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let mut reader = raw_records(&dir, scope(), scope().descriptor(), &records).unwrap();
        for _ in 0..iterated {
            assert!(reader.next().unwrap().is_ok());
        }
        let err = reader.finish().unwrap_err();
        assert!(
            matches!(err, Error::MalformedRecord(_)),
            "{iterated}: {err:?}"
        );
    }
}
