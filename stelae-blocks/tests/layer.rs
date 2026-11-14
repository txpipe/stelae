//! A `blocks` layer end to end: the profile's names against the protocol's
//! rules, a pinned golden, and a write and read through a directory.

use std::fs::File;

use stelae::{
    dir::SnapshotDir,
    profile::{checked_layer_media_type, checked_tag_for_sequence, validate_profile_name},
    Discarding, MediaType, Profile, RecordSink, SnapshotWriter,
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

/// A layer some other writer produced with its blocks out of order is refused
/// on read, and cannot then be confirmed.
#[test]
fn the_reader_refuses_a_layer_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(dir.path().join("snapshot")).unwrap();

    let spec = scope().layer_spec().unwrap();
    let mut sink = snapshot
        .layer_sink(&BlocksProfile, &spec, COMPRESSION_LEVEL)
        .unwrap();

    for block in fixture().into_iter().rev() {
        sink.write_record(&record::encode(&block).unwrap()).unwrap();
    }

    let written = sink.finish().unwrap();
    let blob = File::open(snapshot.blob_path(&written.digests.blob_digest)).unwrap();
    let mut reader = BlocksReader::open(blob, &written.descriptor).unwrap();

    assert!(reader.next().unwrap().is_ok());
    let err = reader.next().unwrap().unwrap_err();
    assert!(matches!(err, Error::OutOfOrder(_)), "{err:?}");
    assert!(reader.next().is_none());
    assert!(reader.finish().is_err());
}
