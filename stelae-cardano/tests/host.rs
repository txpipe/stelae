use std::{cell::Cell, io, sync::Arc};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::{
    config::{ChainConfig, RootConfig},
    ChainPoint, Genesis,
};
use dolos_snapshot::{export::Plan, source::SnapshotSource};
use dolos_testing::blocks::make_conway_block_with_prev;
use stelae::progress::Observer;
use stelae_cardano::{
    backfill::{self, Driver},
    initialize,
    publisher::{publish_once, Destination, PublishOutcome},
    Auth, MithrilSource, Point, RestoreSource, RetainedEpochs, Selection,
};

struct Node {
    root: tempfile::TempDir,
    config: RootConfig,
}

impl Node {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let compatibility = RootConfig::new(ChainConfig::known("preview")).compatibility();
        let document = format!(
            r#"
            compatibility = "{compatibility}"

            [chain]
            protocol = "cardano"
            network = "preview"

            [storage]
            path = {}
            "#,
            toml::Value::String(root.path().join("data").display().to_string()),
        );
        let path = root.path().join("dolos.toml");
        std::fs::write(&path, document).unwrap();
        let config = stelae_cardano::config::load(Some(&path)).unwrap();
        Self { root, config }
    }

    /// Replays the last block of epoch 0 and the first of epoch 1, stopping on
    /// the boundary a stele is cut at.
    fn replay_to_the_first_boundary(&self) -> (Arc<Genesis>, ChainPoint) {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.force_protocol = Some(9);
        let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
        let genesis = Arc::new(genesis);
        let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
        let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
        let mut session = BulkReplaySession::open(&self.config, genesis.clone(), Some(1)).unwrap();
        session
            .import_blocks(vec![block_before, block_boundary])
            .unwrap();
        session.finish().unwrap();
        (genesis, boundary)
    }
}

#[test]
fn a_bounded_fixture_publishes_to_a_directory_through_the_new_host() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let destination = node.root.path().join("stele");
    let (_, outcome) = publish_once(
        &node.config,
        &genesis,
        &RetainedEpochs::default(),
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
            auth: Auth::Anonymous,
            insecure: false,
            scratch_dir: None,
            skip_space_check: false,
            resume: false,
            allow_genesis: false,
        },
        &Observer::silent(),
    )
    .unwrap();
    assert_eq!(outcome, initialize::Outcome::Restored { sequence: 1 });
    assert!(dolos::storage::has_existing_data(&restored.config).unwrap());
}

#[test]
fn retained_state_epochs_reach_the_signed_inscription() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let destination = node.root.path().join("stele");
    let retained = dolos_snapshot::planning::retained_epochs(&[1]).unwrap();
    publish_once(
        &node.config,
        &genesis,
        &retained,
        Selection::default(),
        Destination::Directory(destination.clone()),
        false,
        &Observer::silent(),
    )
    .unwrap();

    let inscription: serde_json::Value =
        serde_json::from_slice(&std::fs::read(destination.join("inscription.json")).unwrap())
            .unwrap();
    assert_eq!(
        inscription["parameters"]["stateEpochs"],
        serde_json::json!([1])
    );
    let retained_dump = inscription["layers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|layer| {
            layer["kind"].as_str().unwrap().starts_with("state-") && layer["scope"]["epoch"] == 1
        });
    assert!(retained_dump, "no state dump of epoch 1 was published");
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
            assert_eq!(plan.retained.as_slice(), &[1]);
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

    let node = Node::new();
    let (genesis, boundary) = node.replay_to_the_first_boundary();

    let mithril = MithrilSource {
        aggregator: "http://unused.invalid".to_owned(),
        genesis_vkey: String::new(),
    };
    let retained = dolos_snapshot::planning::retained_epochs(&[1]).unwrap();
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
            genesis: &genesis,
            retained: &retained,
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
        auth: Auth::Anonymous,
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
