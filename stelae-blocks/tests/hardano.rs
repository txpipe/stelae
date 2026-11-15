//! A blocks snapshot built from immutable files, end to end.
//!
//! The fixture is preprod's first three immutable files, as Mithril certifies
//! them: Byron epochs 0, 1 and 2, one 21,600-slot file each. File 0 opens
//! with the chain's epoch boundary block. Epochs 0 and 1 are exported; file 2
//! is there to show that file 1 is closed.
//!
//! The registry test is `#[ignore]`d and spawns its own server:
//!
//! ```text
//! cargo test -p stelae-blocks --features hardano --test hardano -- --ignored
//! ```

use std::{
    fs,
    path::{Path, PathBuf},
};

use pallas_hardano::storage::immutable::secondary;
use stelae::{
    dir::SnapshotDir,
    inscription::{LayerDescriptor, Link},
    Digest, SnapshotReader,
};
use stelae_blocks::{
    hardano::{block_record, export, Epoch, Sealed},
    layer::limits,
    read_position, BlockRecord, BlocksProfile, BlocksReader, EpochScope, Error, Position,
};
use stelae_driver::{First, Predecessor};

const PREPROD: u64 = 1;
const WIDTH: u64 = 21_600;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/preprod-immutable")
}

fn epoch(dir: &Path, number: u64) -> Epoch<'_> {
    Epoch {
        immutable_dir: dir,
        scope: EpochScope::new(PREPROD, number, number * WIDTH, (number + 1) * WIDTH - 1).unwrap(),
        slots_per_file: WIDTH,
    }
}

/// The fixture's files `numbers`, copied to a directory a test may break.
fn copy_of(numbers: &[u64]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();

    for number in numbers {
        for extension in ["chunk", "primary", "secondary"] {
            let name = format!("{number:05}.{extension}");
            fs::copy(fixture().join(&name), dir.path().join(&name)).unwrap();
        }
    }

    dir
}

/// A publish that follows `link`, as a repository would answer.
struct Follows(Option<Link>);

impl Predecessor for Follows {
    fn previous(&self) -> Option<Link> {
        self.0
    }
}

fn link(sealed: &Sealed) -> Option<Link> {
    Some(Link {
        sequence: sealed.inscription.sequence,
        inscription_digest: sealed.digest,
    })
}

/// Every block of a snapshot's one layer, read back and confirmed.
fn read_back<R: SnapshotReader>(snapshot: &R) -> Vec<BlockRecord> {
    let inscription = snapshot.read_inscription().unwrap();
    let index = snapshot.blob_index().unwrap();

    assert_eq!(inscription.diff.len(), 1);
    assert!(inscription.tip.is_empty());

    let descriptor = &inscription.diff[0];
    let layer = snapshot
        .stream_layer(&index, &BlocksProfile, descriptor, limits())
        .unwrap();
    let mut reader = BlocksReader::from_layer(layer, descriptor).unwrap();
    let blocks = reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    reader.finish().unwrap();

    blocks
}

fn export_to_dir(dir: &Path, predecessor: &dyn Predecessor, number: u64) -> Sealed {
    let fixture = fixture();
    let snapshot = SnapshotDir::create(dir).unwrap();

    export(&snapshot, predecessor, &epoch(&fixture, number)).unwrap()
}

/// Every fixture file, the newest one included: an empty file 3 stands for
/// the file a node opens after it, so file 2 counts as closed.
#[test]
fn each_file_s_bodies_concatenate_to_its_chunk() {
    let dir = copy_of(&[0, 1, 2]);
    for extension in ["chunk", "primary", "secondary"] {
        fs::write(dir.path().join(format!("00003.{extension}")), b"").unwrap();
    }

    for number in [0, 1, 2] {
        let out = tempfile::tempdir().unwrap();
        let snapshot = SnapshotDir::create(out.path()).unwrap();
        export(
            &snapshot,
            &Follows(prior(number)),
            &epoch(dir.path(), number),
        )
        .unwrap();

        let blocks = read_back(&snapshot);
        let chunk = fs::read(fixture().join(format!("{number:05}.chunk"))).unwrap();
        let bodies: Vec<u8> = blocks.iter().flat_map(|b| b.body.clone()).collect();

        assert!(!blocks.is_empty());
        assert_eq!(
            Digest::compute(&bodies),
            Digest::compute(&chunk),
            "file {number}"
        );
    }
}

/// A link the test does not check, so each epoch can be exported alone.
fn prior(number: u64) -> Option<Link> {
    (number > 0).then(|| Link {
        sequence: number,
        inscription_digest: Digest::compute(b"an earlier snapshot"),
    })
}

#[test]
fn slots_and_hashes_are_the_ones_the_node_indexed() {
    let fixture = fixture();

    for number in [0, 1] {
        let blocks = epoch(&fixture, number)
            .blocks()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let indexed: Vec<_> = secondary::read_entries(&fixture, &format!("{number:05}"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(blocks.len(), indexed.len());

        for (block, entry) in blocks.iter().zip(&indexed) {
            assert_eq!(block.hash, entry.header_hash);
            assert!(epoch(&fixture, number).scope.covers(block.slot));
        }
    }
}

/// Preprod's boundary block is at slot 0 and the block after it at slot 2;
/// a shared slot is the layer tests' fixture. Either way it stays first.
#[test]
fn a_boundary_block_keeps_its_place_in_the_stream() {
    let fixture = fixture();
    let out = tempfile::tempdir().unwrap();
    export_to_dir(out.path(), &First, 0);
    let blocks = read_back(&SnapshotDir::open(out.path()).unwrap());

    // `[0, ebb]`: era tag 0 is a Byron epoch boundary block.
    let (boundary, rest) = blocks.split_first().unwrap();
    assert_eq!(&boundary.body[..2], &[0x82, 0x00]);
    assert_eq!(boundary.slot, 0);
    assert!(!rest.is_empty());
    assert!(rest.iter().all(|b| b.body[..2] == [0x82, 0x01]));

    let read: Vec<_> = epoch(&fixture, 0)
        .blocks()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(blocks, read);
}

#[test]
fn a_missing_file_is_refused() {
    // The directory starts after the epoch's file.
    let later = copy_of(&[1, 2]);
    let err = epoch(later.path(), 0).check_files().unwrap_err();
    assert!(matches!(err, Error::MissingFile { file: 0, .. }), "{err:?}");

    // One of the file's three parts is gone.
    let partial = copy_of(&[0, 1]);
    fs::remove_file(partial.path().join("00000.secondary")).unwrap();
    let err = epoch(partial.path(), 0).check_files().unwrap_err();
    assert!(
        matches!(&err, Error::MissingFile { file: 0, path } if path.ends_with("00000.secondary")),
        "{err:?}"
    );

    let out = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(out.path()).unwrap();
    assert!(matches!(
        export(&snapshot, &First, &epoch(partial.path(), 0)),
        Err(Error::MissingFile { .. })
    ));
}

#[test]
fn a_gap_in_the_file_numbers_is_refused() {
    let gapped = copy_of(&[0, 2]);

    let err = epoch(gapped.path(), 1).check_files().unwrap_err();
    assert!(
        matches!(err, Error::FileGap { after: 0, next: 2 }),
        "{err:?}"
    );
}

#[test]
fn the_newest_file_is_refused_as_still_open() {
    let newest = copy_of(&[0, 1]);

    let err = epoch(newest.path(), 1).check_files().unwrap_err();
    assert!(matches!(err, Error::OpenFile { file: 1 }), "{err:?}");

    epoch(newest.path(), 0).check_files().unwrap();
}

#[test]
fn bounds_off_the_file_boundaries_are_refused() {
    let fixture = fixture();
    let mut off = epoch(&fixture, 0);
    off.scope = EpochScope::new(PREPROD, 0, 0, WIDTH - 2).unwrap();

    assert!(matches!(off.files(), Err(Error::MalformedScope(_))));

    off.scope = epoch(&fixture, 0).scope;
    off.slots_per_file = 0;
    assert!(matches!(off.files(), Err(Error::MalformedScope(_))));

    // Preview's width over a 20-file epoch.
    let preview = Epoch {
        immutable_dir: &fixture,
        scope: EpochScope::new(2, 3, 259_200, 345_599).unwrap(),
        slots_per_file: 4_320,
    };
    assert_eq!(preview.files().unwrap(), 60..=79);
}

#[test]
fn only_sequence_one_starts_a_lineage() {
    let out = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(out.path()).unwrap();
    let fixture = fixture();

    let unlinked = export(&snapshot, &First, &epoch(&fixture, 1)).unwrap_err();
    assert!(
        matches!(unlinked, Error::Lineage { sequence: 2, .. }),
        "{unlinked:?}"
    );

    let skipped = Follows(Some(Link {
        sequence: 1,
        inscription_digest: Digest::compute(b"x"),
    }));
    let err = export(&snapshot, &skipped, &epoch(&fixture, 2)).unwrap_err();
    assert!(matches!(err, Error::Lineage { sequence: 3, .. }), "{err:?}");
}

/// A publish an interrupted attempt already uploaded the layer for.
struct Resumed(LayerDescriptor);

impl Predecessor for Resumed {
    fn previous(&self) -> Option<Link> {
        None
    }

    fn resume(
        &self,
        _kind: &str,
        _scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, stelae_driver::Error> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn a_resumed_layer_is_not_read_again() {
    let first = tempfile::tempdir().unwrap();
    let sealed = export_to_dir(first.path(), &First, 0);

    let empty = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let snapshot = SnapshotDir::create(out.path()).unwrap();
    let resumed = export(
        &snapshot,
        &Resumed(sealed.inscription.diff[0].clone()),
        &epoch(empty.path(), 0),
    )
    .unwrap();

    assert_eq!(resumed.digest, sealed.digest);
}

#[test]
fn a_block_is_decoded_only_to_its_header() {
    let first = epoch(&fixture(), 0)
        .blocks()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let truncated = first.body[..first.body.len() - 1].to_vec();

    // The body past the header is not parsed, so a truncated tail still
    // yields the header's slot and hash.
    let record = block_record(0, truncated.clone()).unwrap();
    assert_eq!((record.slot, record.hash), (first.slot, first.hash));
    assert_eq!(record.body, truncated);

    for bad in [vec![0x80], vec![0x82, 0x08, 0x80], vec![0x82, 0x01]] {
        assert!(
            matches!(
                block_record(0, bad.clone()),
                Err(Error::MalformedBlock { .. })
            ),
            "{bad:02x?}"
        );
    }
}

/// Follow `previous` from `newest` back to the start of the lineage.
fn resolve(snapshots: &[(&Sealed, stelae::Inscription)]) {
    let mut expected = None;

    for (sealed, read) in snapshots {
        assert_eq!(read, &sealed.inscription);
        assert_eq!(read.digest().unwrap(), sealed.digest);
        assert_eq!(read.previous, expected);
        assert_eq!(
            read_position(&read.position).unwrap(),
            Position {
                network_magic: PREPROD,
                epoch: read.sequence,
            }
        );
        assert_eq!(read.diff[0].scope["epoch"], read.sequence - 1);

        expected = link(sealed);
    }
}

#[test]
fn two_sequences_chain_in_directories() {
    let (one, two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let first = export_to_dir(one.path(), &First, 0);
    let second = export_to_dir(two.path(), &Follows(link(&first)), 1);

    let (one, two) = (
        SnapshotDir::open(one.path()).unwrap(),
        SnapshotDir::open(two.path()).unwrap(),
    );

    resolve(&[
        (&first, one.read_inscription().unwrap()),
        (&second, two.read_inscription().unwrap()),
    ]);
    assert_eq!(first.inscription.sequence, 1);
    assert_eq!(second.inscription.sequence, 2);
    assert!(!read_back(&one).is_empty());
    assert!(!read_back(&two).is_empty());
}

mod over_a_registry {
    use stelae::oci::{Auth, Registry, Repository};
    use stelae_blocks::{compression, parameters, position};
    use stelae_driver::publish::{self, Chained, Origin, Publishing, Tuning};

    use super::*;

    /// A disposable `registry:2`, or the image `STELAE_TEST_REGISTRY_IMAGE`
    /// names, removed on drop.
    struct LocalRegistry {
        container: String,
        port: u16,
    }

    impl LocalRegistry {
        fn spawn() -> Self {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let image = std::env::var("STELAE_TEST_REGISTRY_IMAGE")
                .unwrap_or_else(|_| "registry:2".to_owned());
            let run = std::process::Command::new("docker")
                .args(["run", "--detach", "--rm", "--publish", "127.0.0.1::5000"])
                .arg(&image)
                .output()
                .expect("docker is required for the registry test");
            assert!(
                run.status.success(),
                "docker run {image}: {}",
                String::from_utf8_lossy(&run.stderr)
            );
            let container = String::from_utf8(run.stdout).unwrap().trim().to_owned();
            let ports = std::process::Command::new("docker")
                .args(["port", &container, "5000/tcp"])
                .output()
                .unwrap();
            let port = String::from_utf8(ports.stdout)
                .unwrap()
                .lines()
                .find_map(|line| line.rsplit(':').next()?.trim().parse().ok())
                .expect("docker port");
            let registry = Self { container, port };
            registry.wait();
            registry
        }

        /// Until `/v2/` answers: a mapped port takes connections before the
        /// server behind it listens.
        fn wait(&self) {
            use std::io::{Read as _, Write as _};

            for _ in 0..100 {
                let answer = std::net::TcpStream::connect(("127.0.0.1", self.port)).and_then(
                    |mut stream| {
                        stream.write_all(b"GET /v2/ HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
                        let mut answer = String::new();
                        stream.read_to_string(&mut answer)?;
                        Ok(answer)
                    },
                );
                if answer.is_ok_and(|answer| answer.starts_with("HTTP/1.0 200")) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            panic!("the registry never answered");
        }

        fn open(&self, scratch: &Path) -> Registry {
            let repository: Repository =
                format!("oci://127.0.0.1:{}/cardano/preprod/blocks", self.port)
                    .parse()
                    .unwrap();

            publish::open(
                &repository,
                true,
                Auth::Anonymous,
                scratch.to_owned(),
                Tuning::default(),
            )
            .unwrap()
        }
    }

    impl Drop for LocalRegistry {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.container])
                .output();
        }
    }

    fn publish(registry: &Registry, number: u64) -> Sealed {
        let fixture = fixture();
        let epoch = epoch(&fixture, number);
        let latest = registry.latest(&BlocksProfile).unwrap();
        let chained = Chained::new(
            &BlocksProfile,
            Publishing::new(registry),
            latest.as_ref(),
            number + 1,
            &position(&Position {
                network_magic: PREPROD,
                epoch: number + 1,
            }),
            Origin::of(registry, PREPROD, parameters(), compression()),
        )
        .unwrap();

        export(registry, &chained, &epoch).unwrap()
    }

    #[test]
    #[ignore = "spawns a registry with docker"]
    fn two_sequences_chain_in_a_registry() {
        let server = LocalRegistry::spawn();
        let scratch = tempfile::tempdir().unwrap();
        let registry = server.open(scratch.path());

        let first = publish(&registry, 0);
        let second = publish(&registry, 1);

        let latest = registry.pull_latest(&BlocksProfile).unwrap();
        let newest = latest.read_inscription().unwrap();
        assert_eq!(newest.sequence, 2);
        assert!(latest.tip_manifest().is_none());

        // Walk the chain from the newest snapshot by its links.
        let link = newest.previous.unwrap();
        let older = registry
            .pull_sequence(&BlocksProfile, link.sequence)
            .unwrap();
        let earlier = older.read_inscription().unwrap();
        assert_eq!(earlier.digest().unwrap(), link.inscription_digest);

        resolve(&[(&first, earlier), (&second, newest)]);

        let fixture = fixture();
        for (snapshot, number) in [(&older, 0), (&latest, 1)] {
            let bodies: Vec<u8> = read_back(snapshot)
                .into_iter()
                .flat_map(|b| b.body)
                .collect();
            let chunk = fs::read(fixture.join(format!("{number:05}.chunk"))).unwrap();
            assert_eq!(Digest::compute(&bodies), Digest::compute(&chunk));
        }
    }
}
