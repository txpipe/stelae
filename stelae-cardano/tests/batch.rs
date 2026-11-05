//! The batch state machine over real replays: a synthetic chain in an immutable
//! db, read by the driver, imported by `BulkReplaySession` and prepared through
//! the Dolos facade.

use std::{
    cell::{Cell, RefCell},
    io,
    num::NonZeroU64,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::{ArchiveIndexDelta, ChainPoint, Genesis, RawBlock, ReplayProgress};
use dolos_snapshot::{
    export::{Document, First, Following, Plan, Predecessor, Producers},
    prepare::{self, Bundle, Finalizing},
    registry::{Preview, Published, Publishing},
    source::SnapshotSource,
    staging::{Budget, Staged},
};
use dolos_testing::{
    blocks::write_immutable_chunks,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
};
use pallas::ledger::traverse::MultiEraBlock;
use stelae::{inscription::Inscription, progress::Observer};
use stelae_cardano::{
    backfill::{self, Batching, Driver, Outcome, Session, Workspace},
    batch::{self, Batch, Committed, Flush, Frontiers, Limits},
    testing::Node,
    MithrilSource, RetainedEpochs,
};
use tokio_util::sync::CancellationToken;

/// The preview genesis's own, which the custom chain runs over.
const MAGIC: u32 = 2;
const REPOSITORY: &str = "oci://registry.invalid/cardano/test";
/// Preview's: `k * 10`.
const SLOTS_PER_FILE: u64 = 4_320;
/// A retained dump falls inside the batch, at its second boundary.
const RETAINED: [u64; 1] = [2];

/// Blocks half an epoch apart from slot 0: an epoch boundary every second
/// block.
struct Chain {
    blocks: Vec<RawBlock>,
    utxos: Vec<dolos_core::config::CustomUtxo>,
    genesis: Arc<Genesis>,
}

impl Chain {
    fn spanning(count: usize) -> Self {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.force_protocol = Some(9);
        let epoch = genesis.shelley.epoch_length.unwrap() as u64;
        let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: count,
            txs_per_block: 1,
            slot: 0,
            start_block: 0,
            slot_step: epoch / 2,
            plain_txs: true,
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        });
        Self {
            blocks,
            utxos: chain.custom_utxos,
            genesis: Arc::new(genesis),
        }
    }

    fn node(&self) -> Node {
        let mut node = Node::new();
        node.on_custom_chain(MAGIC, self.utxos.clone());
        node
    }

    /// One immutable chunk a block, numbered as the network's geometry numbers
    /// its files, because that is how cleanup reads them.
    fn write_immutable(&self, node: &Node) -> PathBuf {
        self.write_immutable_over(node, self.blocks.len())
    }

    fn write_immutable_over(&self, node: &Node, count: usize) -> PathBuf {
        let download = node.path().join("mithril");
        let immutable = download.join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();
        let blocks = &self.blocks[..count];
        let chunks: Vec<&[RawBlock]> = blocks.chunks(1).collect();
        write_immutable_chunks(&immutable, &chunks);

        let tail = immutable.join(format!("{:05}.chunk", chunks.len()));
        std::fs::remove_file(tail).unwrap();
        let mut last = 0;
        for (index, block) in blocks.iter().enumerate().rev() {
            let slot = MultiEraBlock::decode(block).unwrap().slot();
            let number = slot / SLOTS_PER_FILE;
            last = last.max(number);
            for extension in ["chunk", "primary", "secondary"] {
                std::fs::rename(
                    immutable.join(format!("{index:05}.{extension}")),
                    immutable.join(format!("{number:05}.{extension}")),
                )
                .unwrap();
            }
        }
        // The volatile tail a reader leaves out.
        std::fs::write(immutable.join(format!("{:05}.chunk", last + 1)), []).unwrap();
        download
    }

    /// An independent replay stopped at `stop`, and what it would publish.
    fn frozen(&self, stop: u64, previous: Option<&Document>) -> Document {
        let node = self.node();
        let mut session =
            BulkReplaySession::open(&node.config, self.genesis.clone(), Some(stop)).unwrap();
        let progress = session.import_blocks(self.blocks.clone()).unwrap();
        assert!(progress.is_boundary(), "stop {stop}: {progress:?}");
        let snapshot = session.snapshot();
        let plan = snapshot.plan(MAGIC.into(), retained()).unwrap();
        let document = match previous {
            None => snapshot.digest_document(&plan, &First).unwrap(),
            Some(previous) => snapshot
                .digest_document(&plan, &Following::read(&previous.canonical, &plan).unwrap())
                .unwrap(),
        };
        drop(snapshot);
        session.finish().unwrap();
        document
    }
}

fn retained() -> RetainedEpochs {
    dolos_snapshot::planning::retained_epochs(&RETAINED).unwrap()
}

fn limits(max_epochs: u64) -> Limits {
    Limits {
        max_epochs: NonZeroU64::new(max_epochs).unwrap(),
        scratch: u64::MAX,
        reserve_per_epoch: 0,
    }
}

/// What the machine asked of the stores and the repository.
#[derive(Default)]
struct Calls {
    /// The run's, replaced when a test restarts it.
    cancel: Restartable,
    /// Sequences prepared, in order, each once per call.
    prepared: RefCell<Vec<u64>>,
    traversed: RefCell<Vec<u64>>,
    published: Cell<u32>,
    pruned: Cell<u32>,
    sessions: Cell<u32>,
    frontiers: RefCell<Vec<Frontiers>>,
    /// Make the next capture of this sequence fail as a full volume.
    full_at: Cell<Option<u64>>,
    /// Cancel once this sequence is captured.
    cancel_after: Cell<Option<u64>>,
    /// Fail the session's close after this sequence is captured.
    fail_finish_after: Cell<Option<u64>>,
    /// Cancel as replay stops on this boundary, before its capture.
    cancel_at_boundary: Cell<Option<u64>>,
    /// Cancel when the source runs out, which leaves replay in mid-epoch.
    cancel_at_end_of_source: Cell<bool>,
}

/// A cancellation token a restarted run gets afresh, as a new process would.
#[derive(Default)]
struct Restartable(RefCell<CancellationToken>);

impl Restartable {
    fn get(&self) -> CancellationToken {
        self.0.borrow().clone()
    }

    fn restart(&self) {
        *self.0.borrow_mut() = CancellationToken::new();
    }
}

/// Forwards the profile operations, counting the ones the batch must not reach.
struct Counted<'a, S> {
    inner: S,
    calls: &'a Calls,
}

impl<S: SnapshotSource> SnapshotSource for Counted<'_, S> {
    fn committed_position(&self) -> Result<Option<ChainPoint>, dolos_snapshot::Error> {
        self.inner.committed_position()
    }
    fn epoch(&self) -> Result<Option<u64>, dolos_snapshot::Error> {
        self.inner.epoch()
    }
    fn plan(&self, magic: u64, retained: RetainedEpochs) -> Result<Plan, dolos_snapshot::Error> {
        self.inner.plan(magic, retained)
    }
    fn boundary(
        &self,
        plan: &Plan,
    ) -> Result<dolos_snapshot::boundary::Boundary, dolos_snapshot::Error> {
        self.inner.boundary(plan)
    }
    fn publish_directory(
        &self,
        destination: &Path,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Inscription, dolos_snapshot::Error> {
        self.calls.published.set(self.calls.published.get() + 1);
        self.inner.publish_directory(destination, plan, observer)
    }
    fn digest_document(
        &self,
        plan: &Plan,
        previous: &dyn Predecessor,
    ) -> Result<Document, dolos_snapshot::Error> {
        self.inner.digest_document(plan, previous)
    }
    fn verify_reproduction(
        &self,
        published: &Inscription,
        plan: &Plan,
    ) -> Result<Inscription, dolos_snapshot::Error> {
        self.inner.verify_reproduction(published, plan)
    }
    fn preview_repository(
        &self,
        publishing: Publishing<'_>,
        plan: &Plan,
    ) -> Result<Preview, dolos_snapshot::Error> {
        self.inner.preview_repository(publishing, plan)
    }
    fn publish_repository(
        &self,
        publishing: Publishing<'_>,
        plan: &Plan,
        observer: &Observer,
    ) -> Result<Published, dolos_snapshot::Error> {
        self.calls.published.set(self.calls.published.get() + 1);
        self.inner.publish_repository(publishing, plan, observer)
    }
    fn prepare_boundary(
        &self,
        store: &prepare::Store,
        plan: &Plan,
        request: &prepare::Request<'_>,
        observer: &Observer,
    ) -> Result<Bundle, dolos_snapshot::Error> {
        if self.calls.full_at.get() == Some(plan.sequence) {
            self.calls.full_at.set(None);
            return Err(prepare::Refusal::Io(io::ErrorKind::StorageFull.into()).into());
        }
        self.calls.prepared.borrow_mut().push(plan.sequence);
        if matches!(request.tail, prepare::IndexTail::Traverse) {
            self.calls.traversed.borrow_mut().push(plan.sequence);
        }
        self.inner.prepare_boundary(store, plan, request, observer)
    }
    fn stage_batch(
        &self,
        root: &Path,
        bundles: &[Bundle],
        previous: &dyn Predecessor,
        budget: Budget,
        observer: &Observer,
    ) -> Result<Staged, dolos_snapshot::Error> {
        self.inner
            .stage_batch(root, bundles, previous, budget, observer)
    }
    fn finalize_document(
        &self,
        finalizing: &Finalizing<'_>,
        previous: &dyn Predecessor,
    ) -> Result<Document, dolos_snapshot::Error> {
        self.inner.finalize_document(finalizing, previous)
    }
    fn finalize_repository(
        &self,
        publishing: Publishing<'_>,
        finalizing: &Finalizing<'_>,
        observer: &Observer,
    ) -> Result<Published, dolos_snapshot::Error> {
        self.calls.published.set(self.calls.published.get() + 1);
        self.inner
            .finalize_repository(publishing, finalizing, observer)
    }
}

struct CountedWorkspace<'a> {
    inner: ReplayWorkspace<'a>,
    calls: &'a Calls,
    after_capture: &'a dyn Fn(&Calls),
}

struct CountedSession<'a> {
    inner: BulkReplaySession,
    calls: &'a Calls,
    after_capture: &'a dyn Fn(&Calls),
    target: u64,
}

impl<'a> Workspace for CountedWorkspace<'a> {
    type Session = CountedSession<'a>;

    fn snapshot(&self) -> impl SnapshotSource + '_ {
        Counted {
            inner: self.inner.snapshot(),
            calls: self.calls,
        }
    }

    fn start(self, target_epoch: u64) -> Result<Self::Session, backfill::Error> {
        self.calls.sessions.set(self.calls.sessions.get() + 1);
        let inner = self
            .inner
            .start(Some(target_epoch))
            .map_err(backfill::Error::caller)?;
        Ok(CountedSession {
            inner,
            calls: self.calls,
            after_capture: self.after_capture,
            target: target_epoch,
        })
    }

    fn finish(self) -> Result<(), backfill::Error> {
        self.inner.finish().map_err(backfill::Error::caller)
    }
}

impl Session for CountedSession<'_> {
    fn snapshot(&self) -> impl SnapshotSource + '_ {
        Counted {
            inner: self.inner.snapshot(),
            calls: self.calls,
        }
    }

    fn boundary_index_delta(&self) -> Option<&ArchiveIndexDelta> {
        self.inner.boundary_index_delta()
    }

    fn committed_position(&self) -> Result<Option<ChainPoint>, backfill::Error> {
        self.inner
            .committed_position()
            .map_err(backfill::Error::caller)
    }

    fn import_blocks(&mut self, blocks: Vec<RawBlock>) -> Result<ReplayProgress, backfill::Error> {
        let progress = self
            .inner
            .import_blocks(blocks)
            .map_err(backfill::Error::caller)?;
        if progress.is_boundary() && self.calls.cancel_at_boundary.get() == Some(self.target) {
            self.calls.cancel_at_boundary.set(None);
            self.calls.cancel.get().cancel();
        }
        Ok(progress)
    }

    fn prune_history(&mut self) -> Result<u64, backfill::Error> {
        self.calls.pruned.set(self.calls.pruned.get() + 1);
        self.inner.prune_history().map_err(backfill::Error::caller)
    }

    fn finish(self) -> Result<(), backfill::Error> {
        (self.after_capture)(self.calls);
        let finish = self.inner.finish().map_err(backfill::Error::caller);
        let last = self.calls.prepared.borrow().last().copied();
        if last.is_some() && self.calls.fail_finish_after.get() == last {
            self.calls.fail_finish_after.set(None);
            finish?;
            return Err(backfill::Error::caller(io::Error::other(
                "the stores would not close",
            )));
        }
        finish
    }
}

/// A repository holding what the test says it holds, which only moves if the
/// test moves it.
struct Repository {
    latest: RefCell<Option<Committed>>,
    calls: Rc<Calls>,
}

impl backfill::Publish for Repository {
    fn publish(&self, _: &Plan, _: &dyn SnapshotSource) -> Result<(), backfill::Error> {
        self.calls.published.set(self.calls.published.get() + 1);
        Ok(())
    }

    fn latest(&self, _: &Plan) -> Result<Option<Committed>, backfill::Error> {
        Ok(self.latest.borrow().clone())
    }
}

struct Frontier<'a>(&'a Calls);

impl backfill::Replay for Frontier<'_> {
    fn frontiers(&self, frontiers: &Frontiers) {
        self.0.frontiers.borrow_mut().push(frontiers.clone());
    }
}

type Room = dyn Fn(&Path, u64) -> bool;

/// One publisher over one node, run as many times as a test restarts it.
struct Rig<'a> {
    chain: &'a Chain,
    node: Node,
    download: PathBuf,
    calls: Rc<Calls>,
    repository: Repository,
    runtime: tokio::runtime::Runtime,
    room: Box<Room>,
}

impl<'a> Rig<'a> {
    fn new(chain: &'a Chain) -> Self {
        let node = chain.node();
        let download = chain.write_immutable(&node);
        let calls = Rc::<Calls>::default();
        Self {
            chain,
            node,
            download,
            repository: Repository {
                latest: RefCell::new(None),
                calls: calls.clone(),
            },
            calls,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            room: Box::new(|_, _| true),
        }
    }

    fn run(&self, limits: Limits, until_epoch: Option<u64>) -> Result<Outcome, backfill::Error> {
        let calls = &*self.calls;
        let retained = retained();
        let mithril = MithrilSource {
            aggregator: "http://unused.invalid".to_owned(),
            genesis_vkey: String::new(),
        };
        let after_capture = |calls: &Calls| {
            let last = calls.prepared.borrow().last().copied();
            if last.is_some() && calls.cancel_after.get() == last {
                calls.cancel_after.set(None);
                calls.cancel.get().cancel();
            }
        };
        let open_workspace = || {
            Ok(CountedWorkspace {
                inner: ReplayWorkspace::open(&self.node.config, self.chain.genesis.clone())
                    .map_err(backfill::Error::caller)?,
                calls,
                after_capture: &after_capture,
            })
        };
        let replay = Frontier(calls);
        let observer = Observer::silent();
        let room = |path: &Path, bytes: u64| (self.room)(path, bytes);
        Driver {
            genesis: &self.chain.genesis,
            retained: &retained,
            mithril: &mithril,
            download_dir: self.download.clone(),
            window: 1,
            until_epoch,
            skip_validation: true,
            runtime: self.runtime.handle().clone(),
            cancel: calls.cancel.get(),
            mithril_feedback: &|| None,
            // The source offers nothing the db does not already hold.
            beacon: &|| {
                if calls.cancel_at_end_of_source.replace(false) {
                    calls.cancel.get().cancel();
                    return Ok(None);
                }
                Ok(Some(0))
            },
            replay: &replay,
            open_workspace: &open_workspace,
            publish: &self.repository,
            batching: Batching {
                limits,
                storage_path: self.node.config.storage.path.clone(),
                repository: REPOSITORY.to_owned(),
                room: &room,
                observer: &observer,
            },
        }
        .run()
    }

    /// Leave the immutable db holding only the first `count` blocks.
    fn source_ends_after(&self, count: usize) {
        std::fs::remove_dir_all(self.download.join("immutable")).unwrap();
        self.chain.write_immutable_over(&self.node, count);
    }

    fn restart_with_the_whole_source(&self) {
        self.calls.cancel.restart();
        self.source_ends_after(self.chain.blocks.len());
    }

    fn batch(&self) -> Batch {
        self.batch_under(limits(16))
    }

    fn batch_under(&self, limits: Limits) -> Batch {
        let source = batch::Source {
            repository: REPOSITORY.to_owned(),
            network_magic: MAGIC.into(),
            store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
            mutable_slots: dolos_cardano::mutable_slots(&self.chain.genesis),
            retained: RETAINED.to_vec(),
            dolos_revision: stelae_cardano::DOLOS_REVISION.to_owned(),
        };
        let dir = batch::dir_in(&self.node.config.storage.path);
        Batch::open(dir, limits, source).unwrap()
    }

    fn cursor(&self) -> ChainPoint {
        let workspace =
            ReplayWorkspace::open(&self.node.config, self.chain.genesis.clone()).unwrap();
        let cursor = workspace.snapshot().committed_position().unwrap().unwrap();
        workspace.finish().unwrap();
        cursor
    }

    fn manifest(&self, sequence: u64) -> Vec<u8> {
        let path = batch::dir_in(&self.node.config.storage.path)
            .join("prepared/bundles")
            .join(format!("{sequence}.json"));
        std::fs::read(path).unwrap()
    }

    fn sequences(&self) -> Vec<u64> {
        let batch = self.batch();
        batch.prepared().iter().map(|p| p.sequence).collect()
    }

    fn immutable_files(&self) -> Vec<u64> {
        let mut numbers: Vec<u64> = std::fs::read_dir(self.download.join("immutable"))
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.strip_suffix(".chunk")?.parse().ok()
            })
            .collect();
        numbers.sort_unstable();
        numbers
    }
}

fn ready(outcome: Result<Outcome, backfill::Error>) -> (Flush, Frontiers) {
    match outcome {
        Ok(Outcome::BatchReady { flush, frontiers }) => (flush, frontiers),
        other => panic!("expected a closed batch, got {other:?}"),
    }
}

/// Prepares four boundaries and finishes each through the facade, against an
/// independent replay stopped at each one.
#[test]
fn four_boundaries_prepare_once_each_and_finish_as_frozen_stores_publish() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    let (flush, frontiers) = ready(rig.run(limits(4), None));

    assert_eq!(flush, Flush::EpochLimit);
    assert_eq!(
        *rig.calls.prepared.borrow(),
        [1, 2, 3, 4],
        "each exactly once"
    );
    assert!(
        rig.calls.traversed.borrow().is_empty(),
        "no boundary took a hidden whole-index scan"
    );
    assert_eq!(rig.calls.published.get(), 0, "nothing published");
    assert_eq!(rig.calls.pruned.get(), 0, "nothing pruned");
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
    assert_eq!(frontiers.prepared, Some(4));
    assert_eq!(frontiers.published, None);
    assert_eq!(frontiers.staged, None);

    // Each frontier was reported as it moved, with the replay frontier on the
    // boundary just journaled.
    let reported: Vec<_> = rig
        .calls
        .frontiers
        .borrow()
        .iter()
        .map(|frontiers| frontiers.prepared)
        .collect();
    assert_eq!(reported[..4], [Some(1), Some(2), Some(3), Some(4)]);

    // The batch stands on its last boundary, and replay did not pass it.
    let at_four = rig.cursor();
    assert_eq!(
        frontiers.replay.as_ref().map(|replay| replay.slot),
        Some(at_four.slot())
    );

    let mut batch = rig.batch();
    let bundles = batch.bundles().unwrap();
    assert_eq!(bundles.len(), 4);
    assert!(frontiers.horizon.is_some(), "the journal names the horizon");

    let workspace = ReplayWorkspace::open(&rig.node.config, chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let observer = Observer::silent();
    let staged = batch.stage(&snapshot, &First, &observer).unwrap();

    let report = staged.report();
    assert_eq!(report.tag_traversals, 1, "one tag traversal for the batch");
    assert_eq!(report.exact_traversals, 1, "one exact traversal");
    assert!(
        report.entity_traversals <= 1,
        "at most one entity traversal"
    );

    let mut previous: Option<Document> = None;
    for (bundle, stop) in bundles.iter().zip(1..) {
        let finalizing = Finalizing {
            store: batch.store(),
            bundle,
            staged: &staged,
            producers: Producers::DEFAULT,
        };
        let finished = match &previous {
            None => snapshot.finalize_document(&finalizing, &First).unwrap(),
            Some(previous) => {
                let following = Following::read(&previous.canonical, &bundle.plan()).unwrap();
                snapshot.finalize_document(&finalizing, &following).unwrap()
            }
        };
        let frozen = chain.frozen(stop, previous.as_ref());
        assert_eq!(finished.canonical, frozen.canonical, "at sequence {stop}");

        // The retained boundary carries its dump, and so does what follows it:
        // the equality above is not over an empty list.
        let carries_dump = Inscription::parse(&finished.canonical)
            .unwrap()
            .layers
            .iter()
            .any(|layer| layer.kind.starts_with("state-") && layer.scope["epoch"] == RETAINED[0]);
        assert_eq!(carries_dump, stop >= RETAINED[0], "at sequence {stop}");
        previous = Some(finished);
    }
    drop(snapshot);
    workspace.finish().unwrap();

    let frontier = rig.batch().frontiers();
    let staged_at = frontier.staged.expect("the staging frontier is journaled");
    assert_eq!(staged_at.slot, at_four.slot());
}

#[test]
fn a_run_continues_the_planned_batch_rather_than_reading_the_cursor() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    ready(rig.run(limits(2), None));
    assert_eq!(rig.sequences(), [1, 2]);
    let one = rig.manifest(1);
    let two = rig.manifest(2);

    // A closed batch stays closed under a larger limit. Nothing moves.
    let at = rig.cursor();
    let (flush, _) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::EpochLimit);
    assert_eq!(rig.cursor(), at);
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(
        *rig.calls.prepared.borrow(),
        [1, 2],
        "nothing captured again"
    );
    assert_eq!(rig.manifest(1), one);
    assert_eq!(rig.manifest(2), two);
}

#[test]
fn a_cancelled_run_keeps_its_work_and_the_next_one_continues_it() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // Once 2 is journaled and its session closed, standing on its boundary.
    rig.calls.cancel_after.set(Some(2));
    assert!(matches!(
        rig.run(limits(4), None),
        Err(backfill::Error::Interrupted)
    ));
    assert_eq!(rig.sequences(), [1, 2]);

    rig.calls.cancel.restart();
    let (flush, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(flush, Flush::EpochLimit);
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3, 4]);
    assert!(rig.calls.traversed.borrow().is_empty());
    assert_eq!(frontiers.prepared, Some(4));
}

/// The cursor stands past the published sequence, in an epoch with no boundary
/// yet. It is the journal that says what still has to be published.
#[test]
fn a_restart_in_mid_epoch_keeps_every_sequence_it_passed() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // Through 2 and one block past it: slots 0, half, 1, one and a half, 2, and
    // two and a half.
    rig.source_ends_after(6);
    rig.calls.cancel_at_end_of_source.set(true);
    assert!(matches!(
        rig.run(limits(4), None),
        Err(backfill::Error::Interrupted)
    ));
    assert_eq!(rig.sequences(), [1, 2]);
    assert!(
        rig.cursor().slot() > rig.batch().prepared()[1].point.slot,
        "replay stands past the last boundary"
    );
    assert_eq!(rig.calls.published.get(), 0);

    rig.restart_with_the_whole_source();
    let (flush, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(flush, Flush::EpochLimit);
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3, 4], "1 and 2 stand");
    assert!(rig.calls.traversed.borrow().is_empty());
    assert_eq!(frontiers.published, None);
}

/// Cancelled between the commit and the capture, the boundary has lost the
/// delta that lived in its session, so a rerun takes the explicit recovery.
#[test]
fn a_cancellation_on_a_boundary_captures_it_on_the_rerun() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    rig.calls.cancel_at_boundary.set(Some(2));
    assert!(matches!(
        rig.run(limits(3), None),
        Err(backfill::Error::Interrupted)
    ));
    assert_eq!(rig.sequences(), [1], "nothing manufactured at the boundary");
    assert_eq!(*rig.calls.prepared.borrow(), [1]);

    rig.calls.cancel.restart();
    ready(rig.run(limits(3), None));
    assert_eq!(rig.sequences(), [1, 2, 3]);
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3]);
    assert_eq!(*rig.calls.traversed.borrow(), [2]);
}

/// Stores that moved on outside the batch leave sequences nothing holds. The
/// cursor is no permission to skip them.
#[test]
fn stores_past_what_the_journal_holds_are_refused() {
    let chain = Chain::spanning(12);

    // Block 5 is one block into epoch 2, whose boundary went uncaptured, and
    // block 6 opens epoch 3, two past the journal's 1.
    for last_block in [5, 6] {
        let rig = Rig::new(&chain);
        rig.calls.cancel_after.set(Some(1));
        assert!(rig.run(limits(4), None).is_err());
        assert_eq!(rig.sequences(), [1]);
        rig.calls.cancel.restart();

        let mut session =
            BulkReplaySession::open(&rig.node.config, chain.genesis.clone(), Some(3)).unwrap();
        session
            .import_blocks(chain.blocks[3..=last_block].to_vec())
            .unwrap();
        session.finish().unwrap();

        let error = rig.run(limits(4), None).unwrap_err();
        assert!(
            matches!(
                error,
                backfill::Error::Batch(batch::Error::Unrepresented { last: 1, .. })
            ),
            "at block {last_block}: {error}"
        );
        assert_eq!(rig.calls.published.get(), 0);
        assert_eq!(rig.sequences(), [1]);
    }
}

/// A session closing badly leaves a complete bundle the journal does not name,
/// and the rerun finds it through the explicit recovery, once.
#[test]
fn a_boundary_lost_between_capture_and_journal_is_captured_by_traversal() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    rig.calls.fail_finish_after.set(Some(2));
    let error = rig.run(limits(3), None).unwrap_err();
    assert!(error.to_string().contains("would not close"), "{error}");
    assert_eq!(rig.sequences(), [1], "the journal waits for durable stores");

    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(rig.sequences(), [1, 2, 3]);
    assert_eq!(
        *rig.calls.traversed.borrow(),
        [2],
        "only the boundary whose delta died with its process"
    );
    assert_eq!(frontiers.prepared, Some(3));
}

#[test]
fn until_epoch_closes_on_a_captured_boundary() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    let (flush, frontiers) = ready(rig.run(limits(9), Some(3)));
    assert_eq!(flush, Flush::UntilEpoch);
    assert_eq!(frontiers.prepared, Some(3));
    assert_eq!(rig.sequences(), [1, 2, 3]);
}

#[test]
fn the_end_of_the_source_flushes_only_what_it_captured() {
    // Ends halfway through epoch 3: no boundary at 4, and the block past 3 is
    // replayed and left unpublished.
    let chain = Chain::spanning(8);
    let rig = Rig::new(&chain);

    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::EndOfSource);
    assert_eq!(frontiers.prepared, Some(3));
    assert_eq!(rig.sequences(), [1, 2, 3]);

    let cursor = rig.cursor();
    let replay = frontiers.replay.unwrap();
    assert_eq!(
        replay.slot,
        cursor.slot(),
        "the journal has the true cursor"
    );
    assert!(replay.slot > rig.batch().prepared()[2].point.slot);
}

#[test]
fn an_end_of_source_before_any_boundary_is_up_to_date_and_holds_nothing() {
    // Two blocks in epoch 0.
    let chain = Chain::spanning(2);
    let rig = Rig::new(&chain);

    assert_eq!(rig.run(limits(9), None).unwrap(), Outcome::UpToDate);
    assert!(rig.calls.prepared.borrow().is_empty());
    assert!(rig.sequences().is_empty());
}

#[test]
fn a_scratch_budget_flushes_a_smaller_batch_at_a_real_boundary() {
    let chain = Chain::spanning(12);

    // Measure one run to learn what two boundaries hold, then allow just that.
    let measure = Rig::new(&chain);
    ready(measure.run(limits(2), None));
    let held = tree_bytes(&batch::dir_in(&measure.node.config.storage.path));

    let rig = Rig::new(&chain);
    let (flush, frontiers) = ready(rig.run(
        Limits {
            max_epochs: NonZeroU64::new(9).unwrap(),
            scratch: held,
            reserve_per_epoch: 0,
        },
        None,
    ));
    assert_eq!(flush, Flush::ScratchBudget);
    let prepared = frontiers.prepared.unwrap();
    assert!((1..=2).contains(&prepared), "flushed at {prepared}");
    assert_eq!(rig.sequences().last(), Some(&prepared));
    assert_eq!(
        rig.cursor().slot(),
        rig.batch().prepared().last().unwrap().point.slot,
        "replay stopped on the last boundary rather than passing it"
    );
    assert_eq!(rig.calls.published.get(), 0);
}

#[test]
fn free_space_flushes_independently_of_the_budget() {
    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);

    // Enough to finish what it holds until the second boundary estimates past it.
    rig.room = Box::new(|_, bytes| bytes == 0);
    let error = rig
        .run(
            Limits {
                reserve_per_epoch: 1,
                ..limits(9)
            },
            None,
        )
        .unwrap_err();
    assert!(
        matches!(error, backfill::Error::NoRoom { needed: 1, used: 0 }),
        "{error}"
    );
    assert!(
        rig.calls.sessions.get() == 0,
        "stopped before replay advanced"
    );

    // Admitting the first boundary, refusing whatever the second would need.
    let mut rig = Rig::new(&chain);
    rig.room = Box::new(|_, bytes| bytes <= 1);
    let (flush, frontiers) = ready(rig.run(
        Limits {
            reserve_per_epoch: 1,
            ..limits(9)
        },
        None,
    ));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(1));
}

#[test]
fn a_volume_filling_during_a_capture_flushes_what_is_journaled() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    rig.calls.full_at.set(Some(3));
    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
    assert_eq!(rig.sequences(), [1, 2], "the failed capture marked nothing");
}

#[test]
fn a_volume_filling_on_the_first_capture_stops_recoverably() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    rig.calls.full_at.set(Some(1));
    let error = rig.run(limits(2), None).unwrap_err();
    assert!(matches!(error, backfill::Error::NoRoom { .. }), "{error}");
    assert!(rig.sequences().is_empty());

    // With room again, the boundary it stands on is captured and the run goes on.
    let (_, frontiers) = ready(rig.run(limits(2), None));
    assert_eq!(frontiers.prepared, Some(2));
    assert_eq!(*rig.calls.traversed.borrow(), [1]);
}

/// Damage to what only the batch holds is never read as permission to go on.
#[test]
fn a_lost_or_damaged_bundle_stops_the_batch_with_its_recovery() {
    let chain = Chain::spanning(12);

    type Damage = (&'static str, fn(&Path));
    let damage: [Damage; 3] = [
        ("marker gone", |dir| {
            std::fs::remove_file(dir.join("prepared/bundles/2.json")).unwrap()
        }),
        ("marker corrupt", |dir| {
            std::fs::write(dir.join("prepared/bundles/2.json"), b"{").unwrap()
        }),
        ("payload gone", |dir| {
            let blobs = dir.join("prepared/blobs/sha256");
            for entry in std::fs::read_dir(blobs).unwrap().flatten() {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }),
    ];

    for (what, damage) in damage {
        let rig = Rig::new(&chain);
        ready(rig.run(limits(3), None));
        damage(&batch::dir_in(&rig.node.config.storage.path));

        // Before replay moves, even under a limit that would let it.
        let at = rig.cursor();
        let error = match rig.run(limits(9), None) {
            Err(backfill::Error::Batch(error)) => error,
            other => panic!("{what}: {other:?}"),
        };
        assert!(
            matches!(
                error,
                batch::Error::BundleLost {
                    sequence: 1 | 2,
                    ..
                }
            ),
            "{what}: {error}"
        );
        assert!(
            error.to_string().contains("delete the batch"),
            "{what}: {error}"
        );
        assert_eq!(rig.cursor(), at, "{what}");
        assert_eq!(rig.sequences(), [1, 2, 3], "{what}");
    }
}

/// Staging gets what the budget has left after the bundles, and a staging that
/// can't fit leaves the batch to retry rather than marking it staged.
#[test]
fn staging_is_bounded_by_what_the_budget_has_left() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    let prepared = tree_bytes(&batch::dir_in(&rig.node.config.storage.path).join("prepared"));
    let mut tight = rig.batch_under(Limits {
        scratch: prepared + 1,
        ..limits(4)
    });

    let workspace = ReplayWorkspace::open(&rig.node.config, chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let observer = Observer::silent();
    let error = match tight.stage(&snapshot, &First, &observer) {
        Err(error) => error,
        Ok(_) => panic!("staged within one byte"),
    };
    assert!(
        matches!(
            &error,
            batch::Error::Preparing(source)
                if matches!(
                    **source,
                    dolos_snapshot::Error::Staging(
                        dolos_snapshot::staging::Refusal::ScratchExceeded { .. }
                    )
                )
        ),
        "{error}"
    );
    assert_eq!(rig.batch().frontiers().staged, None);

    rig.batch().stage(&snapshot, &First, &observer).unwrap();
    assert!(rig.batch().frontiers().staged.is_some());
    drop(snapshot);
    workspace.finish().unwrap();
}

/// A sound bundle holding other layers is not the one the journal vouched for.
#[test]
fn a_bundle_that_is_not_the_one_recorded_is_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));

    let path = batch::dir_in(&rig.node.config.storage.path).join("journal.json");
    let mut journal: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    journal["prepared"][1]["bundle"] = "another capture".into();
    std::fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();

    let error = rig.run(limits(3), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::BundleChanged { sequence: 2, .. })
        ),
        "{error}"
    );
}

#[test]
fn a_cursor_behind_the_journal_is_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(2), None));

    // Stores rebuilt behind the batch: another replay under the same journal.
    let journal = batch::dir_in(&rig.node.config.storage.path);
    let kept = rig.node.path().join("kept");
    std::fs::rename(&journal, &kept).unwrap();
    dolos::storage::clear_storage(&rig.node.config.storage.path).unwrap();
    std::fs::rename(&kept, &journal).unwrap();

    let error = rig.run(limits(2), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::StoresBehind { .. })
        ),
        "{error}"
    );
}

#[test]
fn a_repository_that_moved_during_the_batch_is_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(2), None));

    *rig.repository.latest.borrow_mut() = Some(Committed {
        sequence: 1,
        identity: "sha256:another-writer".to_owned(),
    });
    let error = rig.run(limits(2), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::PredecessorMoved { .. })
        ),
        "{error}"
    );
}

#[test]
fn a_changed_source_refuses_the_batch() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(2), None));

    let dir = batch::dir_in(&rig.node.config.storage.path);
    let source = batch::Source {
        repository: "oci://registry.invalid/cardano/other".to_owned(),
        network_magic: MAGIC.into(),
        store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
        mutable_slots: dolos_cardano::mutable_slots(&chain.genesis),
        retained: RETAINED.to_vec(),
        dolos_revision: stelae_cardano::DOLOS_REVISION.to_owned(),
    };
    assert!(matches!(
        Batch::open(dir, limits(2), source),
        Err(batch::Error::SourceChanged {
            what: "repository",
            ..
        })
    ));
}

/// A one-boundary run can't consume a pending batch, and the cursor past the
/// repository is no permission to carry on without it.
#[test]
fn a_batch_of_one_refuses_a_pending_batch() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(2), None));

    let error = rig.run(Limits::ONE, None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::Pending { prepared: 2, .. })
        ),
        "{error}"
    );
    assert_eq!(rig.calls.published.get(), 0);

    let error = stelae_cardano::publisher::publish_once(
        &rig.node.config,
        &chain.genesis,
        &retained(),
        stelae_cardano::Selection::default(),
        stelae_cardano::publisher::Destination::Repository {
            direct_s3: None,
            repository: REPOSITORY.parse().unwrap(),
            auth: stelae_cardano::Auth::Anonymous,
            insecure: false,
            scratch_dir: None,
            rebuild: false,
            concurrency: None,
            verify_carried: false,
            require_new: false,
        },
        false,
        &Observer::silent(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("pending"), "{error}");
}

/// The one-boundary path still publishes before it advances and prunes after.
#[test]
fn a_batch_of_one_publishes_each_boundary_and_keeps_no_journal() {
    let chain = Chain::spanning(6);
    let rig = Rig::new(&chain);

    assert_eq!(
        rig.run(Limits::ONE, Some(2)).unwrap(),
        Outcome::UntilEpoch { sequence: 2 }
    );
    assert!(rig.calls.prepared.borrow().is_empty());
    assert_eq!(rig.calls.published.get(), 2);
    assert_eq!(rig.calls.pruned.get(), 1, "after the publish of 1");
    assert!(!batch::dir_in(&rig.node.config.storage.path).exists());
}

/// Input behind a boundary goes only once the journal names that boundary.
#[test]
fn consumed_input_goes_only_after_the_journal_names_the_boundary() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // Stands on 3 with its capture refused, so the last cleanup was at 2, whose
    // first block is in file 40. Files 40 and 50 lead to 3, and it never named
    // them as consumed.
    rig.calls.full_at.set(Some(3));
    ready(rig.run(limits(9), None));
    let files = rig.immutable_files();
    assert_eq!(files.first(), Some(&40), "{files:?}");
}

fn tree_bytes(path: &Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(path).unwrap().flatten() {
        let meta = entry.metadata().unwrap();
        total += match meta.is_dir() {
            true => tree_bytes(&entry.path()),
            false => meta.len(),
        };
    }
    total
}
