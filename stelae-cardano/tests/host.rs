use std::{cell::Cell, io, path::PathBuf};

use dolos::engine::ReplayWorkspace;
use dolos_core::ChainPoint;
use dolos_snapshot::{export::Plan, source::SnapshotSource, DolosProfile};
use stelae::{dir::SnapshotDir, progress::Observer, Digest, SnapshotReader as _};
use stelae_blocks::BlocksProfile;
use stelae_cardano::{
    backfill::{self, Batching, Driver},
    batch, blocks, initialize,
    publisher::{publish_once, BackfillPublisher, Destination, PublishOutcome, RepositoryPublish},
    snapshot::BlocksRefusal,
    testing::{
        chain::Chain,
        loopback::{Loopback, Verdict},
        open_repository, seal_blank_blocks, Node,
    },
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

/// A fresh lineage's first snapshot reproduces itself: the stores it restores
/// publish the same inscription again.
#[test]
fn a_restored_snapshot_publishes_the_same_inscription() {
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

/// A directory Dolos snapshot lists its own layers and names the blocks
/// snapshot Dolos wrote beside it, and its parameters carry no retained epochs.
#[test]
fn a_directory_dolos_snapshot_names_the_blocks_snapshot_beside_it() {
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
    let snapshot = read("dolos", &DolosProfile);
    let blocks = read("blocks", &BlocksProfile);
    let position = dolos_snapshot::read_position(&snapshot.position).unwrap();
    assert_eq!(position.blocks, Some(blocks.digest().unwrap()));
    assert_eq!(snapshot.previous, None);
    assert!(!snapshot.diff.is_empty() && !snapshot.tip.is_empty());
    assert!(
        snapshot.parameters.get("stateEpochs").is_none(),
        "{}",
        snapshot.parameters
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

        /// An empty repository, which the boundary starts.
        fn latest(&self, _: &Plan) -> Result<Option<batch::Committed>, backfill::Error> {
            Ok(None)
        }

        fn snapshot(&self, _: u64) -> Result<Option<batch::Held>, backfill::Error> {
            unreachable!("a batch of one never asks")
        }

        fn blocks(
            &self,
            plan: &Plan,
            _: &dyn Fn() -> Result<blocks::EpochFiles, backfill::Error>,
        ) -> Result<Digest, backfill::Error> {
            assert_eq!(plan.sequence, 1);
            assert!(!self.called.get(), "named before the publish");
            self.blocks.ok_or_else(|| {
                blocks::Error::Differs {
                    repository: "oci://registry.invalid/cardano/preview/blocks".to_owned(),
                    sequence: 1,
                    epoch: 0,
                    held: Digest::compute(b"held"),
                    reproduced: Digest::compute(b"reproduced"),
                }
                .into()
            })
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

    let observer = Observer::silent();
    let named = Some(Digest::compute(b"the blocks snapshot at 1"));

    // A boundary whose blocks snapshot is refused is refused before the publish.
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
            download_dir: node.path().join("unused"),
            scratch: node.path().join("spool"),
            window: 1,
            until_epoch: Some(1),
            cancel,
            beacon: &|| unreachable!("the publish decides this run"),
            fetch: &|_, _, _| unreachable!("the publish decides this run"),
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
            Err(backfill::Error::Blocks(blocks::Error::Differs { sequence: 1, .. }))
                if blocks.is_none() =>
            {
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
        source: RestoreSource::Dir(node.path().join("missing-snapshot")),
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
        source: RestoreSource::Dir(node.path().join("missing-snapshot")),
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

/// The option reaches a backfill's spool and the staging of both its
/// snapshots: nothing lands on the store volume. The source is the immutable
/// files on disk, so the run replays to its first boundary and asks nothing
/// more.
#[test]
fn a_backfill_spools_under_the_scratch_directory_it_is_given() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let chain = Chain::shelley(Chain::blocks_per_epoch() + 1, |_| {});
    let node = chain.node();
    let download = chain.write_immutable(&node);
    let default = node.config.storage.path.join("scratch");
    let spool = node.path().join("named-spool");

    let registry = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(registry.path()).unwrap();

    let options = backfill::Options {
        direct_s3: None,
        repository: loopback.repository("cardano/test").parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: true,
        scratch_dir: Some(spool.clone()),
        concurrency: None,
        download_dir: Some(download),
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
        &chain.genesis,
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
    assert!(loopback
        .manifest("cardano/test/blocks", "epoch-1")
        .is_some());
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
/// publish makes: one of another sequence, above or below, or of another
/// network is refused, whatever tag the root holds it under, and a read the
/// registry refuses fails rather than reading as absent. Each is decided
/// before the epoch's files are asked for, which a snapshot of its own
/// sequence and network, or none, goes on to. Called on the publisher itself,
/// short of the driver's retries.
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

    let named = |plan: &Plan, seed: &dyn Fn(&Loopback, &std::path::Path) -> Option<Digest>| {
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
        let asked = Cell::new(false);
        let files = || {
            asked.set(true);
            Err(backfill::Error::Interrupted)
        };
        let found = backfill::Publish::blocks(&publisher, plan, &files);
        assert_eq!(
            asked.get(),
            matches!(found, Err(backfill::Error::Interrupted)),
            "{found:?}"
        );
        (sealed, found)
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

    let (sealed, found) = named(&plan, &|loopback, _| Some(seal(loopback, magic, 1)));
    assert!(sealed.is_some());
    assert!(
        matches!(found, Err(backfill::Error::Interrupted)),
        "{found:?}"
    );

    let (_, found) = named(&plan, &|_, _| None);
    assert!(
        matches!(found, Err(backfill::Error::Interrupted)),
        "{found:?}"
    );

    let (_, found) = named(&plan, &|loopback, registry| {
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

    let second = Plan {
        sequence: 2,
        ..plan.clone()
    };
    let (_, found) = named(&second, &|loopback, registry| {
        // Sequence 1's snapshot, tagged as sequence 2's too.
        seal(loopback, magic, 1);
        let tags = registry.join("cardano/test/blocks/manifests/tags");
        std::fs::copy(tags.join("epoch-1"), tags.join("epoch-2")).unwrap();
        None
    });
    let refused = refusal(found);
    assert!(
        matches!(
            refused,
            BlocksRefusal::OtherSequence {
                sequence: 2,
                found: 1,
                ..
            }
        ),
        "{refused:?}"
    );

    let (_, found) = named(&plan, &|loopback, _| {
        seal(loopback, magic + 1, 1);
        None
    });
    let refused = refusal(found);
    assert!(
        matches!(refused, BlocksRefusal::OtherNetwork { found, .. } if found == magic + 1),
        "{refused:?}"
    );

    // A 403 ends the read on its first attempt, and is no absence.
    let (_, found) = named(&plan, &|loopback, _| {
        seal(loopback, magic, 1);
        loopback.rule(|seen| {
            match seen.method == "GET" && seen.path.ends_with("/blocks/manifests/epoch-1") {
                true => Verdict::Refuse(403),
                false => Verdict::Proceed,
            }
        });
        None
    });
    match found {
        Err(backfill::Error::Blocks(error @ blocks::Error::Repository { .. })) => {
            assert!(!error.is_refusal(), "{error}")
        }
        other => panic!("not a failed read: {other:?}"),
    }
}

/// The blocks refusal a back-fill's publisher gave.
fn refusal(found: Result<Digest, backfill::Error>) -> BlocksRefusal {
    match found {
        Err(backfill::Error::Blocks(blocks::Error::Refused(refusal))) => refusal,
        other => panic!("not refused: {other:?}"),
    }
}
