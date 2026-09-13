use std::{cell::Cell, io, sync::Arc};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::{
    config::{ChainConfig, MithrilConfig, RootConfig},
    ChainPoint,
};
use dolos_snapshot::{export::Plan, source::SnapshotSource};
use dolos_testing::{
    blocks::make_conway_block_with_prev,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
};
use stelae::progress::Observer;
use stelae_cardano::{
    backfill::{self, Driver},
    initialize,
    publisher::{publish_once, Destination, PublishOutcome},
    Point, RestoreSource, Selection,
};

struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

impl Node {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let document = format!(
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
            force_protocol = 6

            [chain]
            type = "cardano"
            magic = 2
            is_testnet = true
            "#,
            toml::Value::String(root.path().join("data").display().to_string()),
        );
        Self {
            root,
            config: toml::from_str(&document).unwrap(),
        }
    }
}

#[test]
fn a_bounded_fixture_publishes_to_a_directory_through_the_new_host() {
    let mut node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 2,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = ChainConfig::Cardano(chain);

    let mut session =
        BulkReplaySession::open(&node.config, Arc::new(genesis.clone()), None).unwrap();
    session.import_blocks(blocks).unwrap();
    session.finish().unwrap();

    let destination = node.root.path().join("stele");
    let (_, outcome) = publish_once(
        &node.config,
        &genesis,
        Selection::default(),
        Destination::Directory(destination.clone()),
        false,
        &Observer::silent(),
    )
    .unwrap();

    let PublishOutcome::Directory { layers, .. } = outcome else {
        panic!("fixture was not published to the directory");
    };
    assert!(layers > 0);
    assert!(destination.join("inscription.json").is_file());

    let restored = Node::new();
    let outcome = initialize::run(
        &restored.config,
        &genesis,
        &initialize::Options {
            source: RestoreSource::Dir(destination),
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
    assert!(matches!(outcome, initialize::Outcome::Restored { .. }));
    assert!(dolos::storage::has_existing_data(&restored.config).unwrap());
}

#[test]
fn relocated_backfill_publishes_a_pending_boundary_before_any_advance() {
    struct Publisher {
        expected: ChainPoint,
        fail: bool,
        called: Cell<bool>,
        cancel: tokio_util::sync::CancellationToken,
    }

    impl backfill::Publish for Publisher {
        fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
            assert_eq!(plan.sequence, 1);
            assert_eq!(
                source.committed_position().unwrap(),
                Some(self.expected.clone())
            );
            self.called.set(true);
            if self.fail {
                self.cancel.cancel();
                Err(backfill::Error::caller(io::Error::other(
                    "publication failed",
                )))
            } else {
                Ok(())
            }
        }
    }

    let mut node = Node::new();
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.force_protocol = Some(9);
    let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
    let genesis = Arc::new(genesis);
    let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
    let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
    node.config.chain = ChainConfig::Cardano(Default::default());
    let mut session = BulkReplaySession::open(&node.config, genesis.clone(), Some(1)).unwrap();
    session
        .import_blocks(vec![block_before, block_boundary])
        .unwrap();
    session.finish().unwrap();

    let mithril = MithrilConfig {
        aggregator: "http://unused.invalid".to_owned(),
        genesis_key: String::new(),
        ancillary_key: None,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for fail in [true, false] {
        let cancel = tokio_util::sync::CancellationToken::new();
        let publish = Publisher {
            expected: boundary.clone(),
            fail,
            called: Cell::new(false),
            cancel: cancel.clone(),
        };
        let open_workspace = || {
            ReplayWorkspace::open(&node.config, genesis.clone()).map_err(backfill::Error::caller)
        };
        let driver = Driver {
            config: &node.config,
            genesis: &genesis,
            mithril: &mithril,
            download_dir: node.root.path().join("unused"),
            window: 1,
            until_epoch: Some(1),
            skip_validation: false,
            runtime: runtime.handle().clone(),
            cancel,
            mithril_feedback: &|| None,
            replay: &(),
            open_workspace: &open_workspace,
            publish: &publish,
        };
        assert_eq!(driver.run().is_err(), fail);
        assert!(publish.called.get());

        let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
        assert_eq!(
            workspace.snapshot().committed_position().unwrap(),
            Some(boundary.clone())
        );
        workspace.finish().unwrap();
    }
}

#[test]
fn cold_start_fails_closed_and_genesis_requires_explicit_opt_in() {
    let node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let mut options = initialize::Options {
        source: RestoreSource::Dir(node.root.path().join("missing-stele")),
        point: Point::Latest,
        insecure: false,
        scratch_dir: None,
        skip_space_check: false,
        resume: false,
        allow_genesis: false,
    };

    let error = initialize::run(&node.config, &genesis, &options, &Observer::silent())
        .unwrap_err()
        .to_string();
    assert!(error.contains("refusing genesis fallback"), "{error}");

    options.allow_genesis = true;
    assert_eq!(
        initialize::run(&node.config, &genesis, &options, &Observer::silent()).unwrap(),
        initialize::Outcome::Genesis
    );
    // A restart before the first committed block preserves the explicit
    // genesis decision only while the invocation opts in again.
    assert_eq!(
        initialize::run(&node.config, &genesis, &options, &Observer::silent()).unwrap(),
        initialize::Outcome::Genesis
    );
}
