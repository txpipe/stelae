//! Executable old/new-host parity over pinned public Preview chain data.
//!
//! The fixture is deliberately not synthetic: it is the first 4,958 blocks
//! obtained from the public Preview relay. Both hosts replay it into
//! independent stores and stop on the first epoch boundary before publishing.
//! The test is ignored in the ordinary unit-test lane because it is a
//! multi-minute replay; the dedicated parity CI lane runs it explicitly.

use std::{
    fs,
    io::{Read as _, Write as _},
    net::TcpStream,
    path::Path,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use dolos::engine::BulkReplaySession;
use dolos_core::{config::RootConfig, Genesis, ReplayProgress};
use dolos_flatfiles::{BlockLocation, FlatFileStore};
use dolos_old::engine::{
    BulkReplaySession as OldBulkReplaySession, ReplayWorkspace as OldReplayWorkspace,
};
use dolos_snapshot::registry::{
    Auth, Point as RepositoryPoint, Repository, SnapshotRepository, Tuning,
};
use dolos_snapshot_old::{
    facade::SnapshotSource as OldSnapshotSource,
    planning as old_planning,
    publisher::{Next as OldNext, Publisher as OldPublisher, RepositoryPublish as OldPublish},
    registry::{Auth as OldAuth, Published as OldPublished, Tuning as OldTuning},
};
use pallas::ledger::traverse::MultiEraBlock;
use stelae::progress::Observer;
use stelae_cardano::{
    initialize,
    publisher::{publish_once, Destination, PublishOutcome},
    Point, RestoreSource, Selection,
};

const OLD_HOST_REVISION: &str = "1ae4e91c18a9e1456a3612af402d7b9b97546d30";
const FIXTURE_SHA256: &str = "728c5aa9c7c78c4221bdee765f3cd109b630cb00266ab824f6db2bfa43276b4d";
const FIXTURE_BLOCKS: usize = 4_958;
const PREVIEW_EPOCH_LENGTH: u64 = 86_400;
const BOUNDARY_HASH: &str = "4a9761ddc291b0c352d1712b624759132936a07b3e02d1d3bdaaf17b9abfe683";
const LAST_HASH: &str = "da6efb517e112a8439820d505286964bcc65ddd073b560a8f04d1c0902f1125f";

type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

struct OldNode {
    root: tempfile::TempDir,
    config: dolos_old::core::config::RootConfig,
}

impl OldNode {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let document = node_config(root.path());
        Self {
            root,
            config: toml::from_str(&document).unwrap(),
        }
    }

    fn stele(&self, name: &str) -> std::path::PathBuf {
        self.root.path().join(name)
    }
}

fn node_config(root: &Path) -> String {
    format!(
        r#"
        [upstream]
        peer_address = "unused.invalid:3001"

        [storage]
        version = "v4"
        path = {}

        [genesis]
        byron_path = "unused"
        shelley_path = "unused"
        alonzo_path = "unused"
        conway_path = "unused"

        [snapshot]
        state_epochs = [1]

        [chain]
        type = "cardano"
        magic = 2
        is_testnet = true
        "#,
        toml::Value::String(root.join("data").display().to_string()),
    )
}

impl Node {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let document = node_config(root.path());
        Self {
            root,
            config: toml::from_str(&document).unwrap(),
        }
    }

    fn stele(&self, name: &str) -> std::path::PathBuf {
        self.root.path().join(name)
    }
}

fn fixture_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/preview-epoch-0/000000.segment")
}

fn fixture_genesis() -> Genesis {
    let root = fixture_path().parent().unwrap().to_owned();
    Genesis::from_file_paths(
        root.join("byron.json"),
        root.join("shelley.json"),
        root.join("alonzo.json"),
        root.join("conway.json"),
        Some(6),
    )
    .unwrap()
}

fn old_fixture_genesis() -> dolos_old::core::Genesis {
    let root = fixture_path().parent().unwrap().to_owned();
    dolos_old::core::Genesis::from_file_paths(
        root.join("byron.json"),
        root.join("shelley.json"),
        root.join("alonzo.json"),
        root.join("conway.json"),
        Some(6),
    )
    .unwrap()
}

fn read_fixture() -> Vec<Arc<Vec<u8>>> {
    let path = fixture_path();
    for (name, expected) in [
        (
            "byron.json",
            "101650cc8ccbd020f6ceae29bcee2aed5cbdb0bebaa26288d132a9dc3299d9cd",
        ),
        (
            "shelley.json",
            "c5ccb45161676718a8c08b1362ec1ef2cee516fd123aecafacf0f3e4625a746a",
        ),
        (
            "alonzo.json",
            "3511a116de9496a84f956ac50c90dbe92e95d0fbee7b2d7134fb4d407efb9d0e",
        ),
        (
            "conway.json",
            "51ae5b86ae59f872462f22c5034b37d8f8b8561bb7a7ad5c6d9b45502ca65b9e",
        ),
    ] {
        assert_eq!(
            sha256(&fs::read(path.with_file_name(name)).unwrap()),
            expected
        );
    }
    let bytes = fs::read(&path).unwrap();
    assert_eq!(sha256(&bytes), FIXTURE_SHA256, "fixture identity drifted");
    let store = FlatFileStore::new(path.parent().unwrap()).unwrap();
    let mut blocks = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let length = zstd_safe::find_frame_compressed_size(&bytes[offset..]).unwrap();
        let body = store
            .read(&BlockLocation {
                segment_id: 0,
                offset: offset as u64,
                length: length as u32,
            })
            .unwrap();
        blocks.push(Arc::new(body));
        offset += length;
    }
    assert_eq!(offset, bytes.len());
    assert_eq!(blocks.len(), FIXTURE_BLOCKS);
    let first = MultiEraBlock::decode(blocks[0].as_slice()).unwrap();
    let last = MultiEraBlock::decode(blocks.last().unwrap().as_slice()).unwrap();
    let boundary = blocks
        .iter()
        .map(|body| MultiEraBlock::decode(body.as_slice()).unwrap())
        .find(|block| block.slot() >= PREVIEW_EPOCH_LENGTH)
        .unwrap();
    assert_eq!((first.slot(), first.number()), (0, 0));
    assert_eq!((boundary.slot(), boundary.number()), (86_400, 4_320));
    assert_eq!(boundary.hash().to_string(), BOUNDARY_HASH);
    assert_eq!((last.slot(), last.number()), (99_140, 4_957));
    assert_eq!(last.hash().to_string(), LAST_HASH);
    blocks
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    hex::encode(Sha256::digest(bytes))
}

fn replay(node: &Node, blocks: Vec<Arc<Vec<u8>>>) -> (u64, u128) {
    let started = Instant::now();
    let genesis = Arc::new(fixture_genesis());
    let mut replay = BulkReplaySession::open(&node.config, genesis, Some(1)).unwrap();
    let ReplayProgress::Boundary { position } = replay.import_blocks(blocks).unwrap() else {
        panic!("real fixture did not stop at the first Preview epoch boundary")
    };
    assert!(position.slot() >= PREVIEW_EPOCH_LENGTH);
    replay.finish().unwrap();
    (position.slot(), started.elapsed().as_millis())
}

fn old_replay(node: &OldNode, blocks: Vec<Arc<Vec<u8>>>) -> (u64, u128) {
    let started = Instant::now();
    let genesis = Arc::new(old_fixture_genesis());
    let mut replay = OldBulkReplaySession::open(&node.config, genesis, Some(1)).unwrap();
    let dolos_old::core::ReplayProgress::Boundary { position } =
        replay.import_blocks(blocks).unwrap()
    else {
        panic!("real fixture did not stop at the first Preview epoch boundary")
    };
    assert!(position.slot() >= PREVIEW_EPOCH_LENGTH);
    replay.finish().unwrap();
    (position.slot(), started.elapsed().as_millis())
}

fn old_host_publish(node: &OldNode, output: &Path) -> (String, u128) {
    let started = Instant::now();
    let genesis = Arc::new(old_fixture_genesis());
    let workspace = OldReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
    let result = (|| {
        let snapshot = workspace.snapshot();
        let retained = old_planning::retained_epochs(&node.config)?;
        let plan = snapshot.selected_plan(
            u64::from(genesis.network_magic()),
            retained,
            dolos_snapshot_old::facade::Selection::default(),
        )?;
        let inscription = snapshot.publish_directory(output, &plan, &Observer::silent())?;
        Ok::<_, dolos_snapshot_old::Error>(inscription.digest()?.to_string())
    })();
    workspace.finish().unwrap();
    (result.unwrap(), started.elapsed().as_millis())
}

fn new_host_publish(node: &Node, output: &Path) -> (String, u128) {
    let started = Instant::now();
    let genesis = fixture_genesis();
    let (_, outcome) = publish_once(
        &node.config,
        &genesis,
        Selection::default(),
        Destination::Directory(output.to_owned()),
        false,
        &Observer::silent(),
    )
    .unwrap();
    let PublishOutcome::Directory { identity, .. } = outcome else {
        panic!("new host did not publish a directory stele")
    };
    (identity, started.elapsed().as_millis())
}

fn old_host_publish_repository(
    node: &OldNode,
    repository: &Repository,
    rebuild: bool,
    dry_run: bool,
    require_new: bool,
) -> Result<Option<OldPublished>, AnyError> {
    let genesis = Arc::new(old_fixture_genesis());
    let workspace = OldReplayWorkspace::open(&node.config, genesis.clone())?;
    let result: Result<Option<OldPublished>, dolos_snapshot_old::Error> = (|| {
        let snapshot = workspace.snapshot();
        let plan = snapshot.selected_plan(
            u64::from(genesis.network_magic()),
            old_planning::retained_epochs(&node.config)?,
            dolos_snapshot_old::facade::Selection::default(),
        )?;
        let settings = OldPublish {
            repo: repository,
            insecure: true,
            scratch_dir: None,
            rebuild,
            dry_run,
            require_new,
            tuning: OldTuning::default(),
        };
        let publisher = OldPublisher::open(&node.config, &settings, OldAuth::Anonymous)?;
        match OldNext::read(publisher.standing(&plan)?, plan.sequence, require_new)? {
            OldNext::Nothing(_) => Ok(None),
            OldNext::First | OldNext::After { .. } => {
                publisher.preflight()?;
                if dry_run {
                    let _ = snapshot.preview(&publisher, &plan)?;
                    Ok(None)
                } else {
                    Ok(Some(snapshot.publish(
                        &publisher,
                        &plan,
                        &Observer::silent(),
                    )?))
                }
            }
        }
    })();
    let finish = workspace
        .finish()
        .map_err(|error| -> AnyError { Box::new(error) });
    match (
        result.map_err(|error| -> AnyError { Box::new(error) }),
        finish,
    ) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(finish)) => Err(format!(
            "publication failed ({operation}); closing stores also failed ({finish})"
        )
        .into()),
    }
}

fn new_host_publish_repository(
    node: &Node,
    repository: &Repository,
    rebuild: bool,
    dry_run: bool,
    require_new: bool,
) -> Result<PublishOutcome, AnyError> {
    let genesis = fixture_genesis();
    let (_, outcome) = publish_once(
        &node.config,
        &genesis,
        Selection::default(),
        Destination::Repository {
            repository: repository.clone(),
            insecure: true,
            scratch_dir: None,
            rebuild,
            concurrency: None,
            verify_carried: false,
            require_new,
        },
        dry_run,
        &Observer::silent(),
    )?;
    Ok(outcome)
}

struct LocalRegistry {
    container: String,
    address: String,
}

impl LocalRegistry {
    fn spawn() -> Self {
        static CRYPTO: std::sync::Once = std::sync::Once::new();
        CRYPTO.call_once(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("nothing installed a rustls provider first");
        });

        let image =
            std::env::var("STELAE_TEST_REGISTRY_IMAGE").unwrap_or_else(|_| "registry:2".to_owned());
        let run = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--publish",
                "127.0.0.1::5000",
                &image,
            ])
            .output()
            .expect("docker is required to run publisher parity");
        assert!(
            run.status.success(),
            "docker run {image}: {}",
            String::from_utf8_lossy(&run.stderr)
        );
        let container = String::from_utf8(run.stdout).unwrap().trim().to_owned();
        let ports = Command::new("docker")
            .args(["port", &container, "5000/tcp"])
            .output()
            .expect("docker port");
        let mapped = String::from_utf8(ports.stdout).unwrap();
        let port = mapped
            .lines()
            .find_map(|line| line.rsplit(':').next())
            .and_then(|port| port.trim().parse::<u16>().ok())
            .unwrap_or_else(|| panic!("no published port in {mapped:?}"));
        let fixture = Self {
            container,
            address: format!("127.0.0.1:{port}"),
        };
        fixture.wait_until_ready();
        fixture
    }

    fn repository(&self, name: &str) -> Repository {
        format!("oci://{}/{name}", self.address).parse().unwrap()
    }

    fn wait_until_ready(&self) {
        for _ in 0..300 {
            if let Ok(mut stream) = TcpStream::connect(&self.address) {
                let request = format!(
                    "GET /v2/ HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                    self.address
                );
                if stream.write_all(request.as_bytes()).is_ok() {
                    let mut response = String::new();
                    if stream.read_to_string(&mut response).is_ok()
                        && response.starts_with("HTTP/1.1 200")
                    {
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("registry never answered on {}", self.address);
    }
}

impl Drop for LocalRegistry {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.container])
            .output();
    }
}

fn inspect_repository(repository: &Repository, scratch: &Path) -> (String, Vec<u8>, u64) {
    let repository = SnapshotRepository::open(
        repository,
        true,
        Auth::Anonymous,
        scratch.to_owned(),
        Tuning::default(),
    )
    .unwrap();
    let inspected = repository.inspect(RepositoryPoint::Latest).unwrap();
    (
        inspected.identity.to_string(),
        inspected.inscription.canonicalize().unwrap(),
        inspected.total_compressed,
    )
}

fn restore_with_unchanged_consumer(source: &Path) -> Node {
    let node = Node::new();
    let genesis = fixture_genesis();
    let outcome = initialize::run(
        &node.config,
        &genesis,
        &initialize::Options {
            source: RestoreSource::Dir(source.to_owned()),
            point: Point::Latest,
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            resume: false,
            allow_genesis: false,
        },
        &Observer::silent(),
    )
    .unwrap();
    assert!(matches!(
        outcome,
        initialize::Outcome::Restored { sequence: 1 }
    ));
    node
}

fn restore_repository_with_unchanged_consumer(source: Repository) -> Node {
    let node = Node::new();
    let genesis = fixture_genesis();
    let outcome = initialize::run(
        &node.config,
        &genesis,
        &initialize::Options {
            source: RestoreSource::Repo(source),
            point: RepositoryPoint::Latest,
            insecure: true,
            scratch_dir: None,
            skip_space_check: false,
            resume: false,
            allow_genesis: false,
        },
        &Observer::silent(),
    )
    .unwrap();
    assert!(matches!(
        outcome,
        initialize::Outcome::Restored { sequence: 1 }
    ));
    node
}

fn directory_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                path.metadata().unwrap().len()
            }
        })
        .sum()
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the supplied rusage on a zero return.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage failed");
    let rss = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        rss
    } else {
        rss * 1024
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

#[test]
#[ignore = "replays 4,958 real Preview blocks; run by the parity CI lane"]
fn old_and_new_hosts_replay_publish_and_restore_the_same_real_boundary() {
    let fixture = read_fixture();
    let old = OldNode::new();
    let new = Node::new();
    let (old_slot, old_replay_ms) = old_replay(&old, fixture.clone());
    let (new_slot, new_replay_ms) = replay(&new, fixture);
    assert_eq!(old_slot, new_slot);

    let old_dir = old.stele("old-host-stele");
    let new_dir = new.stele("new-host-stele");
    let (old_identity, old_publish_ms) = old_host_publish(&old, &old_dir);
    let old_peak_rss_bytes = peak_rss_bytes();
    let (new_identity, new_publish_ms) = new_host_publish(&new, &new_dir);
    let new_peak_rss_bytes = peak_rss_bytes();
    assert_eq!(old_identity, new_identity);
    assert_eq!(
        fs::read(old_dir.join("inscription.json")).unwrap(),
        fs::read(new_dir.join("inscription.json")).unwrap()
    );

    let restored = restore_with_unchanged_consumer(&new_dir);
    let restored_dir = restored.stele("restored-stele");
    let (restored_identity, _) = new_host_publish(&restored, &restored_dir);
    assert_eq!(new_identity, restored_identity);

    let registry = LocalRegistry::spawn();
    let old_repository = registry.repository("parity/old-host");
    let new_repository = registry.repository("parity/new-host");

    assert!(
        old_host_publish_repository(&old, &old_repository, false, true, false)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        new_host_publish_repository(&new, &new_repository, false, true, false).unwrap(),
        PublishOutcome::DryRun { sequence: 1 }
    ));

    let old_published = old_host_publish_repository(&old, &old_repository, false, false, false)
        .unwrap()
        .expect("the old host should make the first publication");
    let PublishOutcome::Repository {
        sequence: 1,
        identity: new_registry_identity,
        built: new_built,
        reused: new_reused,
        transfer: new_transfer,
    } = new_host_publish_repository(&new, &new_repository, false, false, false).unwrap()
    else {
        panic!("the new host should make the first publication")
    };
    assert_eq!(old_published.identity.to_string(), new_registry_identity);
    assert_eq!(old_published.layers_built, new_built);
    assert_eq!(old_published.layers_reused, new_reused);
    assert_eq!(old_published.transfer, new_transfer);

    let old_inspected = inspect_repository(&old_repository, &old.stele("inspect-scratch"));
    let new_inspected = inspect_repository(&new_repository, &new.stele("inspect-scratch"));
    assert_eq!(old_inspected, new_inspected);
    assert_eq!(old_inspected.0, new_identity);

    assert!(
        old_host_publish_repository(&old, &old_repository, false, false, false,)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        new_host_publish_repository(&new, &new_repository, false, false, false).unwrap(),
        PublishOutcome::Nothing(_)
    ));
    assert!(old_host_publish_repository(&old, &old_repository, false, false, true).is_err());
    assert!(new_host_publish_repository(&new, &new_repository, false, false, true).is_err());

    let rebuilt_old_repository = registry.repository("parity/old-host-rebuild");
    let rebuilt_new_repository = registry.repository("parity/new-host-rebuild");
    let rebuilt_old =
        old_host_publish_repository(&old, &rebuilt_old_repository, true, false, false)
            .unwrap()
            .expect("the old host should force a fresh reproduction");
    let PublishOutcome::Repository {
        sequence: 1,
        identity: rebuilt_new_identity,
        built: rebuilt_new_built,
        reused: 0,
        ..
    } = new_host_publish_repository(&new, &rebuilt_new_repository, true, false, false).unwrap()
    else {
        panic!("the new host should force a fresh reproduction")
    };
    assert_eq!(rebuilt_old.identity.to_string(), rebuilt_new_identity);
    assert_eq!(rebuilt_new_identity, new_registry_identity);
    assert_eq!(rebuilt_old.layers_built, rebuilt_new_built);
    assert_eq!(rebuilt_old.layers_reused, 0);

    let restored_registry = restore_repository_with_unchanged_consumer(new_repository.clone());
    let restored_registry_dir = restored_registry.stele("restored-registry-stele");
    let (restored_registry_identity, _) =
        new_host_publish(&restored_registry, &restored_registry_dir);
    assert_eq!(new_registry_identity, restored_registry_identity);

    let report = serde_json::json!({
        "schema": 1,
        "outcome": "pass",
        "hosts": {
            "old": {"revision": OLD_HOST_REVISION, "kind": "dolos snapshot publish"},
            "new": {
                "revision": option_env!("STELAE_PARITY_NEW_REVISION").unwrap_or("worktree"),
                "dolos_revision": stelae_cardano::DOLOS_REVISION,
                "kind": "stelae-publisher"
            }
        },
        "fixture": {
            "network": "preview",
            "block_count": FIXTURE_BLOCKS,
            "sha256": FIXTURE_SHA256,
            "boundary_slot": old_slot
        },
        "expected": {"sequence": 1, "identity": new_identity},
        "checks": [
            {"name": "real-boundary replay", "outcome": "pass"},
            {"name": "source position", "outcome": "pass"},
            {"name": "layer diffIds and canonical inscription", "outcome": "pass"},
            {"name": "old/new host OCI publication", "outcome": "pass"},
            {"name": "dry-run, no-op and force-rebuild agreement", "outcome": "pass"},
            {"name": "unchanged Dolos directory and OCI consumer restore", "outcome": "pass"}
        ],
        "registry": {
            "kind": "disposable registry:2",
            "repositories": "separate",
            "identity": new_registry_identity,
            "compressed_bytes": old_inspected.2,
            "old_transfer": {
                "layers_uploaded": old_published.transfer.layers_uploaded,
                "layers_skipped": old_published.transfer.layers_skipped,
                "layers_reused": old_published.transfer.layers_reused,
                "bytes_uploaded": old_published.transfer.bytes_uploaded,
                "bytes_skipped": old_published.transfer.bytes_skipped,
                "bytes_reused": old_published.transfer.bytes_reused
            },
            "new_transfer": {
                "layers_uploaded": new_transfer.layers_uploaded,
                "layers_skipped": new_transfer.layers_skipped,
                "layers_reused": new_transfer.layers_reused,
                "bytes_uploaded": new_transfer.bytes_uploaded,
                "bytes_skipped": new_transfer.bytes_skipped,
                "bytes_reused": new_transfer.bytes_reused
            }
        },
        "resources": {
            "old": {
                "replay_ms": old_replay_ms,
                "publish_ms": old_publish_ms,
                "peak_rss_bytes": old_peak_rss_bytes,
                "artifact_bytes": directory_bytes(&old_dir),
                "transfer_bytes": 0,
                "scratch_bytes": 0
            },
            "new": {
                "replay_ms": new_replay_ms,
                "publish_ms": new_publish_ms,
                "peak_rss_bytes": new_peak_rss_bytes,
                "artifact_bytes": directory_bytes(&new_dir),
                "transfer_bytes": 0,
                "scratch_bytes": 0
            }
        }
    });
    let rendered = serde_json::to_string_pretty(&report).unwrap();
    eprintln!("{rendered}");
    if let Some(path) = std::env::var_os("STELAE_PARITY_REPORT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, format!("{rendered}\n")).unwrap();
    }
}
