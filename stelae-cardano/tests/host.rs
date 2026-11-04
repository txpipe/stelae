use std::{cell::Cell, io, path::PathBuf};

use dolos::engine::ReplayWorkspace;
use dolos_core::ChainPoint;
use dolos_snapshot::{export::Plan, source::SnapshotSource};
use stelae::progress::Observer;
use stelae_cardano::{
    backfill::{self, Driver},
    initialize,
    publisher::{publish_once, Destination, PublishOutcome},
    testing::Node,
    Auth, MithrilSource, Point, RestoreSource, RetainedEpochs, Selection,
};

#[test]
fn a_bounded_fixture_publishes_to_a_directory_through_the_new_host() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let destination = node.path().join("stele");
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

/// A fresh lineage's first stele reproduces itself: the stores it restores
/// publish the same inscription again.
#[test]
fn a_restored_stele_publishes_the_same_inscription() {
    fn publish(node: &Node, genesis: &dolos_core::Genesis, destination: PathBuf) -> String {
        let retained = dolos_snapshot::planning::retained_epochs(&[1]).unwrap();
        let (_, outcome) = publish_once(
            &node.config,
            genesis,
            &retained,
            Selection::default(),
            Destination::Directory(destination),
            false,
            &Observer::silent(),
        )
        .unwrap();
        let PublishOutcome::Directory { identity, .. } = outcome else {
            panic!("fixture was not published to the directory");
        };
        identity
    }

    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();
    let first = node.path().join("first");
    let published = publish(&node, &genesis, first.clone());

    let restored = Node::new();
    let outcome = initialize::run(
        &restored.config,
        &genesis,
        &initialize::Options {
            source: RestoreSource::Dir(first),
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

    let republished = publish(&restored, &genesis, restored.path().join("second"));
    assert_eq!(republished, published);
}

#[test]
fn retained_state_epochs_reach_the_signed_inscription() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let destination = node.path().join("stele");
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
            download_dir: node.path().join("unused"),
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
        source: RestoreSource::Dir(node.path().join("missing-stele")),
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

#[test]
fn a_rolling_history_is_refused_before_any_store_opens() {
    let node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let mut config = node.config.clone();
    config.storage.path = node.path().join("rolling");
    config.history = dolos_core::config::HistoryConfig::rolling(
        dolos_core::config::HistoryWindow::slots(432_000),
    );
    let options = initialize::Options {
        source: RestoreSource::Dir(node.path().join("missing-stele")),
        point: Point::Latest,
        auth: Auth::Anonymous,
        insecure: false,
        scratch_dir: None,
        skip_space_check: false,
        resume: false,
        allow_genesis: true,
    };

    let error = initialize::run(&config, &genesis, &options, &Observer::silent())
        .unwrap_err()
        .to_string();
    assert!(error.contains("history.mode = \"full\""), "{error}");
    assert!(!config.storage.path.exists());
}

#[test]
fn a_rolling_history_is_refused_before_backfill_creates_anything() {
    let node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let mut config = node.config.clone();
    config.storage.path = node.path().join("rolling");
    config.history = dolos_core::config::HistoryConfig::rolling(
        dolos_core::config::HistoryWindow::slots(432_000),
    );
    let options = backfill::Options {
        direct_s3: None,
        repository: "oci://registry.invalid/cardano/preview".parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: false,
        scratch_dir: None,
        concurrency: None,
        verify_carried: false,
        download_dir: None,
        window: 1,
        until_epoch: None,
        skip_validation: false,
        mithril: MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        },
        retained: RetainedEpochs::default(),
    };

    let error = backfill::run(
        &config,
        &genesis,
        &options,
        tokio_util::sync::CancellationToken::new(),
        &Observer::silent(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("history.mode = \"full\""), "{error}");
    assert!(!config.storage.path.exists());
}
