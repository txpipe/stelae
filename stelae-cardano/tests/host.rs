use std::{cell::Cell, io, path::PathBuf};

use dolos::engine::ReplayWorkspace;
use dolos_core::ChainPoint;
use dolos_snapshot::{export::Plan, source::SnapshotSource, DolosProfile};
use stelae::{dir::SnapshotDir, progress::Observer, Digest, SnapshotReader as _};
use stelae_blocks::BlocksProfile;
use stelae_cardano::{
    backfill::{self, Batching, Driver},
    batch, initialize,
    publisher::{publish_once, BackfillPublisher, Destination, PublishOutcome, RepositoryPublish},
    snapshot::BlocksRefusal,
    testing::{loopback::Loopback, open_repository, seal_blank_blocks, Node},
    Auth, MithrilSource, Point, Repository, RestoreSource, Selection,
};

#[test]
fn a_bounded_fixture_publishes_to_a_directory_through_the_new_host() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let destination = node.path().join("root");
    let (_, outcome) = publish_once(
        &node.config,
        &genesis,
        Selection::default(),
        Destination::Directory {
            path: destination.clone(),
            scratch_dir: None,
        },
        false,
        &Observer::silent(),
    )
    .unwrap();

    let PublishOutcome::Directory { layers, .. } = outcome else {
        panic!("fixture was not published to the directory");
    };
    assert!(layers > 0);
    for layout in ["dolos", "blocks"] {
        assert!(
            destination.join(layout).join("oci-layout").is_file(),
            "{layout}"
        );
    }
    assert!(
        node.config.storage.path.join("scratch").is_dir(),
        "the index runs spool under the storage path, not the system's"
    );

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
        let (_, outcome) = publish_once(
            &node.config,
            genesis,
            Selection::default(),
            Destination::Directory {
                path: destination,
                scratch_dir: None,
            },
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

/// A directory stele lists its own layers and names the blocks snapshot Dolos
/// wrote beside it, and its parameters carry no retained epochs.
#[test]
fn a_directory_stele_names_the_blocks_snapshot_beside_it() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();

    let root = node.path().join("root");
    publish_once(
        &node.config,
        &genesis,
        Selection::default(),
        Destination::Directory {
            path: root.clone(),
            scratch_dir: None,
        },
        false,
        &Observer::silent(),
    )
    .unwrap();

    let read = |layout: &str, profile: &dyn stelae::Profile| {
        SnapshotDir::open(root.join(layout))
            .unwrap()
            .pull_sequence(profile, 1)
            .unwrap()
            .read_inscription()
            .unwrap()
    };
    let stele = read("dolos", &DolosProfile);
    let blocks = read("blocks", &BlocksProfile);
    let position = dolos_snapshot::read_position(&stele.position).unwrap();
    assert_eq!(position.blocks, Some(blocks.digest().unwrap()));
    assert_eq!(stele.previous, None);
    assert!(!stele.diff.is_empty() && !stele.tip.is_empty());
    assert!(
        stele.parameters.get("stateEpochs").is_none(),
        "{}",
        stele.parameters
    );
}

#[test]
fn relocated_backfill_publishes_a_pending_boundary_before_any_advance() {
    struct Publisher {
        expected: ChainPoint,
        scratch: PathBuf,
        blocks: Option<Digest>,
        fail: bool,
        called: Cell<bool>,
        cancel: tokio_util::sync::CancellationToken,
    }

    impl backfill::Publish for Publisher {
        fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
            assert_eq!(plan.sequence, 1);
            assert_eq!(plan.blocks, self.blocks);
            assert_eq!(plan.scratch.as_ref(), Some(&self.scratch));
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

        fn latest(&self, _: &Plan) -> Result<Option<batch::Committed>, backfill::Error> {
            unreachable!("a batch of one never asks")
        }

        fn stele(&self, _: u64) -> Result<Option<batch::Held>, backfill::Error> {
            unreachable!("a batch of one never asks")
        }

        fn blocks(&self, plan: &Plan) -> Result<Option<Digest>, backfill::Error> {
            assert_eq!(plan.sequence, 1);
            Ok(self.blocks)
        }

        fn finalize(
            &self,
            _: &dolos_snapshot::prepare::Finalizing<'_>,
            _: Option<&batch::Committed>,
            _: &dyn SnapshotSource,
        ) -> Result<batch::Committed, backfill::Error> {
            unreachable!("a batch of one prepares nothing")
        }
    }

    let node = Node::new();
    let (genesis, boundary) = node.replay_to_the_first_boundary();

    let mithril = MithrilSource {
        aggregator: "http://unused.invalid".to_owned(),
        genesis_vkey: String::new(),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let observer = Observer::silent();
    let named = Some(Digest::compute(b"the blocks snapshot at 1"));

    // A boundary without its blocks snapshot is refused before the publish.
    for (fail, blocks) in [(true, named), (false, named), (false, None)] {
        let cancel = tokio_util::sync::CancellationToken::new();
        let publish = Publisher {
            expected: boundary.clone(),
            scratch: node.path().join("spool"),
            blocks,
            fail,
            called: Cell::new(false),
            cancel: cancel.clone(),
        };
        let open_workspace = || {
            ReplayWorkspace::open(&node.config, genesis.clone()).map_err(backfill::Error::caller)
        };
        let driver = Driver {
            genesis: &genesis,
            mithril: &mithril,
            download_dir: node.path().join("unused"),
            scratch: node.path().join("spool"),
            window: 1,
            until_epoch: Some(1),
            skip_validation: false,
            runtime: runtime.handle().clone(),
            cancel,
            mithril_feedback: &|| None,
            beacon: &|| unreachable!("the publish decides this run"),
            replay: &(),
            open_workspace: &open_workspace,
            check_stores: &|| backfill::refuse_archive_behind(&node.config, genesis.clone()),
            publish: &publish,
            batching: Batching {
                limits: batch::Limits::ONE,
                storage_path: node.config.storage.path.clone(),
                repository: "oci://registry.invalid/cardano/preview".to_owned(),
                room: &|_, _| true,
                observer: &observer,
            },
        };
        match driver.run() {
            Err(backfill::Error::BlocksAbsent { sequence: 1 }) if blocks.is_none() => {
                assert!(!publish.called.get())
            }
            outcome => {
                assert_eq!(outcome.is_err(), fail, "{outcome:?}");
                assert!(publish.called.get());
            }
        }

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
        download_dir: None,
        window: 1,
        until_epoch: None,
        skip_validation: false,
        mithril: MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        },
        limits: batch::Limits::ONE,
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

/// The check asks what opening a workspace would refuse before it opens a store
/// itself, which would create it.
#[test]
fn a_configuration_the_engine_refuses_creates_no_store_in_the_check() {
    let node = Node::new();
    let mut config = node.config.clone();
    config.storage.path = node.path().join("refused");
    config.chain.network = "custom".to_owned();

    let error =
        backfill::refuse_archive_behind(&config, dolos_cardano::include::preview::load().into())
            .unwrap_err();
    assert!(matches!(error, backfill::Error::Caller(_)), "{error}");
    assert!(!config.storage.path.exists());
}

/// The host's own run looks at the stores before it replays or publishes.
#[test]
fn a_backfill_refuses_stores_whose_state_passed_the_archive() {
    use dolos_core::{StateStore as _, StateWriter as _};

    let node = Node::new();
    let (genesis, boundary) = node.replay_to_the_first_boundary();

    let state = dolos::storage::open_state_store(&node.config).unwrap();
    let writer = state.start_writer().unwrap();
    let past = ChainPoint::Specific(boundary.slot() + 1, boundary.hash().unwrap());
    writer.set_cursor(past).unwrap();
    writer.commit().unwrap();
    state.shutdown().unwrap();
    drop(state);

    let options = backfill::Options {
        direct_s3: None,
        repository: "oci://registry.invalid/cardano/preview".parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: false,
        scratch_dir: None,
        concurrency: None,
        download_dir: Some(node.path().join("unused")),
        window: 1,
        until_epoch: Some(1),
        skip_validation: true,
        mithril: MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        },
        limits: batch::Limits::ONE,
    };
    let error = backfill::run(
        &node.config,
        &genesis,
        &options,
        tokio_util::sync::CancellationToken::new(),
        &Observer::silent(),
    )
    .unwrap_err();
    assert!(
        matches!(error, backfill::Error::ArchiveBehind { .. }),
        "{error}"
    );
}

/// The option reaches both a backfill's spool and its staging: nothing lands on
/// the store volume.
#[test]
fn a_backfill_spools_under_the_scratch_directory_it_is_given() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();
    let default = node.config.storage.path.join("scratch");
    assert!(!default.exists());
    let spool = node.path().join("named-spool");

    let registry = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(registry.path()).unwrap();
    let epoch = genesis.shelley.epoch_length.unwrap() as u64;
    let seeding = tempfile::tempdir().unwrap();
    seal_blank_blocks(
        &open_repository(&loopback.repository("cardano/test/blocks"), seeding.path()),
        u64::from(genesis.network_magic()),
        1,
        (0, 0, epoch - 1),
    );

    let options = backfill::Options {
        direct_s3: None,
        repository: loopback.repository("cardano/test").parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: true,
        scratch_dir: Some(spool.clone()),
        concurrency: None,
        download_dir: Some(node.path().join("unused")),
        window: 1,
        until_epoch: Some(1),
        skip_validation: true,
        mithril: MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        },
        limits: batch::Limits::ONE,
    };
    let outcome = backfill::run(
        &node.config,
        &genesis,
        &options,
        tokio_util::sync::CancellationToken::new(),
        &Observer::silent(),
    );

    assert_eq!(
        outcome.as_ref().ok(),
        Some(&backfill::Outcome::UntilEpoch { sequence: 1 }),
        "{outcome:?}"
    );
    assert!(spool.is_dir());
    assert!(!default.exists());
    assert!(loopback.manifest("cardano/test/dolos", "epoch-1").is_some());
}

/// A larger batch through the host entry point, with a source nothing answers
/// at.
fn batched_options() -> backfill::Options {
    backfill::Options {
        direct_s3: None,
        repository: "oci://registry.invalid/cardano/preview".parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: false,
        scratch_dir: None,
        concurrency: None,
        download_dir: None,
        window: 1,
        until_epoch: None,
        skip_validation: false,
        mithril: MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        },
        limits: batch::Limits {
            max_epochs: std::num::NonZeroU64::new(2).unwrap(),
            scratch: u64::MAX,
            reserve_per_epoch: 0,
        },
    }
}

/// The host entry point starts a larger batch: this one stops only at the
/// cancellation it was handed, past where its stores opened.
#[test]
fn a_batch_above_one_runs_through_the_host() {
    let node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let mut config = node.config.clone();
    config.storage.path = node.path().join("batched");
    let options = batched_options();

    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let error =
        backfill::run(&config, &genesis, &options, cancel, &Observer::silent()).unwrap_err();
    assert!(matches!(error, backfill::Error::Interrupted), "{error}");
    assert!(config.storage.path.exists());
    // A batch opens before the first check of the cancellation, and a batch of
    // one never opens.
    assert!(batch::dir_in(&config.storage.path).exists());
}

/// A real listing that fails stays the source failure a larger batch closes on,
/// so an outage can't hold back what it prepared. Takes the retries' whole
/// backoff.
#[test]
fn an_aggregator_that_doesnt_answer_fails_a_batched_run_as_the_source() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let node = Node::new();
    let genesis = dolos_cardano::include::preview::load();
    let mut config = node.config.clone();
    config.storage.path = node.path().join("batched");

    let error = backfill::run(
        &config,
        &genesis,
        &batched_options(),
        tokio_util::sync::CancellationToken::new(),
        &Observer::silent(),
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Mithril {
                what: "listing mithril snapshots",
                ..
            }
        ),
        "{error}"
    );
}

/// A back-fill names the blocks snapshot at a boundary through the refusals a
/// publish makes: one of another sequence or network is refused, whatever tag
/// the root holds it under. Called on the publisher itself, short of the
/// driver's retries.
#[test]
fn a_backfill_names_no_blocks_snapshot_of_another_sequence_or_network() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();
    let magic = u64::from(genesis.network_magic());
    let epoch = genesis.shelley.epoch_length.unwrap() as u64;
    let plan = {
        let workspace = ReplayWorkspace::open(&node.config, genesis.clone()).unwrap();
        let plan = workspace.snapshot().plan(magic).unwrap();
        workspace.finish().unwrap();
        plan
    };
    assert_eq!(plan.sequence, 1);

    let named = |seed: &dyn Fn(&Loopback, &std::path::Path) -> Option<Digest>| {
        let registry = tempfile::tempdir().unwrap();
        let loopback = Loopback::serve(registry.path()).unwrap();
        let sealed = seed(&loopback, registry.path());
        let repo: Repository = loopback.repository("cardano/test").parse().unwrap();
        let observer = Observer::silent();
        let publisher = BackfillPublisher {
            config: &node.config,
            settings: RepositoryPublish {
                repo: &repo,
                auth: Auth::Anonymous,
                insecure: true,
                scratch_dir: None,
                tuning: Default::default(),
            },
            observer: &observer,
        };
        (sealed, backfill::Publish::blocks(&publisher, &plan))
    };
    let seal = |loopback: &Loopback, magic: u64, sequence: u64| {
        let scratch = tempfile::tempdir().unwrap();
        let first = (sequence - 1) * epoch;
        seal_blank_blocks(
            &open_repository(&loopback.repository("cardano/test/blocks"), scratch.path()),
            magic,
            sequence,
            (sequence - 1, first, first + epoch - 1),
        )
    };

    let (sealed, found) = named(&|loopback, _| Some(seal(loopback, magic, 1)));
    assert_eq!(found.unwrap(), sealed);

    let (_, found) = named(&|_, _| None);
    assert_eq!(found.unwrap(), None);

    let (_, found) = named(&|loopback, registry| {
        // Sequence 2's snapshot, tagged as sequence 1's too.
        seal(loopback, magic, 2);
        let tags = registry.join("cardano/test/blocks/manifests/tags");
        std::fs::copy(tags.join("epoch-2"), tags.join("epoch-1")).unwrap();
        None
    });
    let refused = refusal(found);
    assert!(
        matches!(
            refused,
            BlocksRefusal::OtherSequence {
                sequence: 1,
                found: 2,
                ..
            }
        ),
        "{refused:?}"
    );

    let (_, found) = named(&|loopback, _| {
        seal(loopback, magic + 1, 1);
        None
    });
    let refused = refusal(found);
    assert!(
        matches!(refused, BlocksRefusal::OtherNetwork { found, .. } if found == magic + 1),
        "{refused:?}"
    );
}

/// The blocks refusal a back-fill's publisher gave.
fn refusal(found: Result<Option<Digest>, backfill::Error>) -> BlocksRefusal {
    match found {
        Err(backfill::Error::Caller(source)) => *source
            .downcast::<BlocksRefusal>()
            .unwrap_or_else(|source| panic!("not a blocks refusal: {source}")),
        other => panic!("not refused: {other:?}"),
    }
}
