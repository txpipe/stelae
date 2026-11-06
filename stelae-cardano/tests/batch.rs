//! The batch state machine over real replays: a synthetic chain in an immutable
//! db, read by the driver, imported by `BulkReplaySession` and prepared through
//! the Dolos facade.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
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
    staging::{Budget, Report, Staged},
};
use dolos_testing::{
    blocks::write_immutable_chunks,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
};
use pallas::ledger::traverse::MultiEraBlock;
use stelae::{inscription::Inscription, progress::Observer};
use stelae_cardano::{
    backfill::{self, Batching, Driver, Outcome, Session, Workspace},
    batch::{self, Batch, Committed, Flush, Frontiers, Held, Limits},
    testing::{empty_registry, Node},
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
    /// Write this sequence's payloads, then fail as a full volume before its
    /// marker stands: what a later shard hitting ENOSPC leaves behind.
    payload_then_full: Cell<Option<u64>>,
    prepared_root: RefCell<Option<PathBuf>>,
    /// Fail the import aiming at this epoch as a full volume.
    import_full_at: Cell<Option<u64>>,
    /// Fail the session's close as a full volume, once this sequence is
    /// captured.
    finish_full_after: Cell<Option<u64>>,
    /// Fail the import aiming at this epoch as a store whose worker failed.
    poison_import_at: Cell<Option<u64>>,
    /// Fail the close of the session aiming at this epoch that way.
    poison_finish_at: Cell<Option<u64>>,
    /// Fail the close of the session aiming at this epoch for another reason.
    break_finish_at: Cell<Option<u64>>,
    /// Fail the close of the session aiming at this epoch with what a close on
    /// a full volume really says.
    full_finish_at: Cell<Option<u64>>,
    /// Fail the next close of a workspace as a store whose worker failed.
    poison_workspace_close: Cell<bool>,
    /// Fail the next check of the stores as a full volume.
    check_full: Cell<bool>,
    /// Fail the next read of the stores' epoch, as a store that can't be read.
    epoch_fails: Cell<bool>,
    /// Fail the next workspace to open as a full volume.
    open_full: Cell<bool>,
    /// Fail the workspace to open after this many more, as a full volume.
    open_full_after: Cell<Option<u32>>,
    /// Fail the next session to start as a full volume.
    start_full: Cell<bool>,
    /// Fail the next prune as a full volume.
    prune_full: Cell<bool>,
    /// The highest immutable file the source offers.
    beacon: Cell<u64>,
    /// What each staging cost, including those that reused every run.
    stagings: RefCell<Vec<Report>>,
    /// Die as the next staging finishes, its markers on disk.
    die_after_staging: Cell<bool>,
    /// Refuse the next staging this way.
    stage_refusal: Cell<Option<fn() -> dolos_snapshot::Error>>,
    /// How many stagings the batch asked for, whether or not they finished.
    stage_attempts: Cell<u32>,
    /// Fail every read of the source's height this way.
    source_fails: Cell<Option<SourceFailure>>,
}

type SourceFailure = fn(&Calls) -> backfill::Error;

/// What a store says once a background worker has failed.
const POISONED: &str = "state error: internal store error: fjall error: FjallError: Poisoned";

/// What a registry's 5xx on an immutable tag reads as.
const UNREADABLE_TAG: &str = "the registry answered 503 for that tag";

/// What a real close said when the WAL's checkpoint, its one foreground write,
/// met the full volume.
const CLOSE_ON_FULL_VOLUME: &str = "finishing replay: persisting replay checkpoint: IO error: I/O \
     error: No space left on device (os error 28)";

fn full_volume() -> backfill::Error {
    backfill::Error::caller(io::Error::from(io::ErrorKind::StorageFull))
}

fn poisoned() -> backfill::Error {
    backfill::Error::caller(io::Error::other(POISONED))
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
        if self.calls.epoch_fails.replace(false) {
            let unread = dolos_core::StateError::InternalStoreError("read failed".to_owned());
            return Err(dolos_snapshot::Error::Chain(unread.into()));
        }
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
        if self.calls.payload_then_full.get() == Some(plan.sequence) {
            self.calls.payload_then_full.set(None);
            self.inner
                .prepare_boundary(store, plan, request, observer)?;
            let root = self.calls.prepared_root.borrow().clone().unwrap();
            std::fs::remove_file(root.join("bundles").join(format!("{}.json", plan.sequence)))
                .unwrap();
            return Err(prepare::Refusal::Io(io::ErrorKind::StorageFull.into()).into());
        }
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
        let attempts = &self.calls.stage_attempts;
        attempts.set(attempts.get() + 1);
        if let Some(refusal) = self.calls.stage_refusal.take() {
            return Err(refusal());
        }
        let staged = self
            .inner
            .stage_batch(root, bundles, previous, budget, observer)?;
        self.calls.stagings.borrow_mut().push(staged.report());
        if self.calls.die_after_staging.replace(false) {
            self.calls.cancel.get().cancel();
            return Err(dolos_snapshot::Error::Staging(
                dolos_snapshot::staging::Refusal::Io(io::Error::other("the process died")),
            ));
        }
        Ok(staged)
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
        if self.calls.start_full.replace(false) {
            self.inner.finish().map_err(backfill::Error::caller)?;
            return Err(full_volume());
        }
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
        let finish = self.inner.finish().map_err(backfill::Error::caller);
        if self.calls.poison_workspace_close.replace(false) {
            finish?;
            return Err(poisoned());
        }
        finish
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
        if self.calls.import_full_at.get() == Some(self.target) {
            self.calls.import_full_at.set(None);
            return Err(full_volume());
        }
        if self.calls.poison_import_at.get() == Some(self.target) {
            self.calls.poison_import_at.set(None);
            return Err(poisoned());
        }
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
        if self.calls.prune_full.replace(false) {
            return Err(full_volume());
        }
        self.calls.pruned.set(self.calls.pruned.get() + 1);
        self.inner.prune_history().map_err(backfill::Error::caller)
    }

    fn finish(self) -> Result<(), backfill::Error> {
        (self.after_capture)(self.calls);
        let finish = self.inner.finish().map_err(backfill::Error::caller);
        if self.calls.poison_finish_at.get() == Some(self.target) {
            self.calls.poison_finish_at.set(None);
            finish?;
            return Err(poisoned());
        }
        if self.calls.full_finish_at.get() == Some(self.target) {
            self.calls.full_finish_at.set(None);
            finish?;
            return Err(backfill::Error::caller(io::Error::other(
                CLOSE_ON_FULL_VOLUME,
            )));
        }
        if self.calls.break_finish_at.get() == Some(self.target) {
            self.calls.break_finish_at.set(None);
            finish?;
            return Err(backfill::Error::caller(io::Error::other(
                "the stores would not close",
            )));
        }
        let last = self.calls.prepared.borrow().last().copied();
        if last.is_some() && self.calls.finish_full_after.get() == last {
            self.calls.finish_full_after.set(None);
            finish?;
            return Err(full_volume());
        }
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

/// Where a publication stops short, as a process dying or a volume filling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Die before anything reaches the repository.
    Upload,
    /// Die with the immutable tag written and the latest tag not moved.
    SealHalfWay,
    /// Die with the stele published, before its answer is read.
    Acknowledgement,
    /// The answer is lost, and the process lives to try again.
    AcknowledgementLostAlive,
    /// Die just after the answer, so the journal commits and nothing follows.
    AfterAnswer,
    /// The volume fills, with nothing written.
    FullVolume,
    /// Make the journal unwritable as the answer comes back.
    UnwritableJournal,
}

/// A repository holding what the test says it holds, which otherwise moves only
/// by what is published into it. Identities are real: each stele is the
/// document a chained publish produces from the same bundle, runs and
/// predecessor.
struct Repository {
    latest: RefCell<Option<Committed>>,
    calls: Rc<Calls>,
    /// The immutable tags.
    steles: RefCell<BTreeMap<u64, Document>>,
    /// Sequences that reached the repository, each time one did.
    uploaded: RefCell<Vec<u64>>,
    fault: Cell<Option<(u64, Fault)>>,
    /// Keep what a batch of one publishes, for a batch to follow it.
    keep_singles: Cell<bool>,
    /// The sequence whose immutable tag can't be read.
    unreadable_tag: Cell<Option<u64>>,
    /// The payloads the batch held as each sequence was published.
    payloads: RefCell<Vec<(u64, usize)>>,
    batch_dir: PathBuf,
}

impl Repository {
    fn committed(sequence: u64, document: &Document) -> Committed {
        Committed {
            sequence,
            identity: document.identity.to_string(),
        }
    }

    fn inscription(&self, sequence: u64) -> Option<Inscription> {
        let steles = self.steles.borrow();
        let document = steles.get(&sequence)?;
        Some(Inscription::parse(&document.canonical).unwrap())
    }

    /// What the repository's latest stele chains the next one to.
    fn followed(&self) -> Option<Inscription> {
        let latest = self.latest.borrow();
        latest.as_ref().and_then(|l| self.inscription(l.sequence))
    }

    fn die(&self) -> backfill::Error {
        self.calls.cancel.get().cancel();
        backfill::Error::caller(io::Error::other("the process died"))
    }

    fn tags(&self) -> BTreeMap<u64, Vec<u8>> {
        let steles = self.steles.borrow();
        steles
            .iter()
            .map(|(sequence, document)| (*sequence, document.canonical.clone()))
            .collect()
    }
}

impl backfill::Publish for Repository {
    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
        self.calls.published.set(self.calls.published.get() + 1);
        if self.keep_singles.get() {
            let previous = self.followed();
            let following = backfill::following(previous.as_ref(), plan.sequence)?;
            let document = source
                .digest_document(plan, &*following)
                .map_err(backfill::Error::caller)?;
            *self.latest.borrow_mut() = Some(Self::committed(plan.sequence, &document));
            self.steles.borrow_mut().insert(plan.sequence, document);
        }
        Ok(())
    }

    fn latest(&self, _: &Plan) -> Result<Option<Committed>, backfill::Error> {
        Ok(self.latest.borrow().clone())
    }

    fn stele(&self, sequence: u64) -> Result<Option<Held>, backfill::Error> {
        if self.unreadable_tag.get() == Some(sequence) {
            // Spares the retries' backoff: this failure is all a test asks of them.
            self.calls.cancel.get().cancel();
            return Err(backfill::Error::caller(io::Error::other(UNREADABLE_TAG)));
        }
        let steles = self.steles.borrow();
        Ok(steles.get(&sequence).map(|document| Held {
            committed: Self::committed(sequence, document),
            inscription: Inscription::parse(&document.canonical).unwrap(),
        }))
    }

    fn finalize(
        &self,
        finalizing: &Finalizing<'_>,
        predecessor: Option<&Committed>,
        source: &dyn SnapshotSource,
    ) -> Result<Committed, backfill::Error> {
        let sequence = finalizing.bundle.sequence();
        let payloads = std::fs::read_dir(self.batch_dir.join("prepared/blobs/sha256"))
            .unwrap()
            .count();
        self.payloads.borrow_mut().push((sequence, payloads));

        let fault = self
            .fault
            .get()
            .filter(|(at, _)| *at == sequence)
            .map(|(_, fault)| fault);
        if fault.is_some() {
            self.fault.set(None);
        }
        match fault {
            Some(Fault::Upload) => return Err(self.die()),
            Some(Fault::FullVolume) => {
                let full = prepare::Refusal::Io(io::ErrorKind::StorageFull.into());
                return Err(backfill::Error::Finalizing(Box::new(full.into())));
            }
            _ => {}
        }

        // The chain read, held to the journal as the real one is.
        let latest = self.latest.borrow().clone();
        if latest.as_ref() != predecessor {
            let named = |held: Committed| (held.sequence, held.identity);
            let moved = stelae::Error::PredecessorMoved {
                expected: predecessor.cloned().map(named),
                found: latest.map(named),
            };
            return Err(backfill::Error::Finalizing(Box::new(moved.into())));
        }

        let previous = self.followed();
        let following = backfill::following(previous.as_ref(), sequence)?;
        let document = source
            .finalize_document(finalizing, &*following)
            .map_err(|error| backfill::Error::Finalizing(Box::new(error)))?;
        let committed = Self::committed(sequence, &document);
        self.steles.borrow_mut().insert(sequence, document);
        if fault == Some(Fault::SealHalfWay) {
            return Err(self.die());
        }
        *self.latest.borrow_mut() = Some(committed.clone());
        self.uploaded.borrow_mut().push(sequence);
        match fault {
            Some(Fault::Acknowledgement) => Err(self.die()),
            Some(Fault::AcknowledgementLostAlive) => Err(backfill::Error::caller(
                io::Error::other("the registry stopped answering"),
            )),
            Some(Fault::AfterAnswer) => {
                self.calls.cancel.get().cancel();
                Ok(committed)
            }
            #[cfg(unix)]
            Some(Fault::UnwritableJournal) => {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(&self.batch_dir, std::fs::Permissions::from_mode(0o555))
                    .unwrap();
                Ok(committed)
            }
            _ => Ok(committed),
        }
    }
}

/// A publisher answering as `inner`, whose publications go through `finalize`:
/// how a test acts at a moment only a publication reaches.
struct Around<'a> {
    inner: &'a dyn backfill::Publish,
    finalize: &'a Finalize<'a>,
}

type Finalize<'a> = dyn Fn(
        &Finalizing<'_>,
        Option<&Committed>,
        &dyn SnapshotSource,
    ) -> Result<Committed, backfill::Error>
    + 'a;

impl backfill::Publish for Around<'_> {
    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
        self.inner.publish(plan, source)
    }

    fn latest(&self, plan: &Plan) -> Result<Option<Committed>, backfill::Error> {
        self.inner.latest(plan)
    }

    fn stele(&self, sequence: u64) -> Result<Option<Held>, backfill::Error> {
        self.inner.stele(sequence)
    }

    fn finalize(
        &self,
        finalizing: &Finalizing<'_>,
        predecessor: Option<&Committed>,
        source: &dyn SnapshotSource,
    ) -> Result<Committed, backfill::Error> {
        (self.finalize)(finalizing, predecessor, source)
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
    /// The repository the batch is told it publishes to.
    named: RefCell<String>,
    /// The genesis the driver reads, which the stores' own replay doesn't.
    driver_genesis: RefCell<Arc<Genesis>>,
    retained: RefCell<RetainedEpochs>,
}

impl<'a> Rig<'a> {
    fn new(chain: &'a Chain) -> Self {
        let node = chain.node();
        let download = chain.write_immutable(&node);
        let calls = Rc::<Calls>::default();
        let batch_dir = batch::dir_in(&node.config.storage.path);
        Self {
            chain,
            node,
            download,
            repository: Repository {
                latest: RefCell::new(None),
                calls: calls.clone(),
                steles: RefCell::default(),
                uploaded: RefCell::default(),
                fault: Cell::new(None),
                keep_singles: Cell::new(false),
                unreadable_tag: Cell::new(None),
                payloads: RefCell::default(),
                batch_dir,
            },
            calls,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            room: Box::new(|_, _| true),
            named: RefCell::new(REPOSITORY.to_owned()),
            driver_genesis: RefCell::new(chain.genesis.clone()),
            retained: RefCell::new(retained()),
        }
    }

    /// Prepare until the batch closes. A batch of one has only its own path.
    fn run(&self, limits: Limits, until_epoch: Option<u64>) -> Result<Outcome, backfill::Error> {
        self.drive(limits, until_epoch, false, &self.repository)
    }

    /// Run as a process would: replay and publish until there is nothing left.
    fn drain(&self, limits: Limits, until_epoch: Option<u64>) -> Result<Outcome, backfill::Error> {
        self.drive(limits, until_epoch, true, &self.repository)
    }

    /// As a new process would, after one that stopped short.
    fn restart_and_drain(&self, limits: Limits) -> Result<Outcome, backfill::Error> {
        self.calls.cancel.restart();
        self.drain(limits, None)
    }

    fn drive(
        &self,
        limits: Limits,
        until_epoch: Option<u64>,
        drain: bool,
        publish: &dyn backfill::Publish,
    ) -> Result<Outcome, backfill::Error> {
        let calls = &*self.calls;
        let retained = self.retained.borrow().clone();
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
            if calls.open_full.replace(false) {
                return Err(full_volume());
            }
            match calls.open_full_after.get() {
                Some(0) => {
                    calls.open_full_after.set(None);
                    return Err(full_volume());
                }
                Some(more) => calls.open_full_after.set(Some(more - 1)),
                None => {}
            }
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
        let driver_genesis = self.driver_genesis.borrow().clone();
        let driver = Driver {
            genesis: &driver_genesis,
            retained: &retained,
            mithril: &mithril,
            download_dir: self.download.clone(),
            scratch: self.node.path().join("spool"),
            window: 1,
            until_epoch,
            skip_validation: true,
            runtime: self.runtime.handle().clone(),
            cancel: calls.cancel.get(),
            mithril_feedback: &|| None,
            // Unless a test says otherwise, the source offers nothing the db does
            // not already hold.
            beacon: &|| {
                if let Some(failure) = calls.source_fails.get() {
                    return Err(failure(calls));
                }
                if calls.cancel_at_end_of_source.replace(false) {
                    calls.cancel.get().cancel();
                    return Ok(None);
                }
                Ok(Some(calls.beacon.get()))
            },
            replay: &replay,
            open_workspace: &open_workspace,
            check_stores: &|| {
                if calls.check_full.replace(false) {
                    return Err(full_volume());
                }
                backfill::refuse_archive_behind(&self.node.config, self.chain.genesis.clone())
            },
            publish,
            batching: Batching {
                limits,
                storage_path: self.node.config.storage.path.clone(),
                repository: self.named.borrow().clone(),
                room: &room,
                observer: &observer,
            },
        };
        match drain || limits.is_one() {
            true => driver.run(),
            false => driver.prepare(),
        }
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
            mutable_slots: dolos_cardano::mutable_slots(&self.driver_genesis.borrow()),
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
            batch::Error::Staging { source, .. }
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

    // Past the next sequence, which no lost answer can explain.
    *rig.repository.latest.borrow_mut() = Some(Committed {
        sequence: 2,
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

/// Stand the stores on `stop`'s boundary with no batch: what a restore of the
/// repository's latest stele, or a batch of one, leaves behind.
fn stand_on(rig: &Rig<'_>, stop: u64) {
    let mut session =
        BulkReplaySession::open(&rig.node.config, rig.chain.genesis.clone(), Some(stop)).unwrap();
    let progress = session.import_blocks(rig.chain.blocks.clone()).unwrap();
    assert!(progress.is_boundary(), "stop {stop}: {progress:?}");
    session.finish().unwrap();
}

fn repository_at(rig: &Rig<'_>, sequence: u64) {
    *rig.repository.latest.borrow_mut() = Some(Committed {
        sequence,
        identity: format!("sha256:{sequence}"),
    });
}

fn batch_dir(rig: &Rig<'_>) -> PathBuf {
    batch::dir_in(&rig.node.config.storage.path)
}

fn copy_tree(from: &Path, to: &Path) {
    let status = std::process::Command::new("cp")
        .arg("-R")
        .arg(from)
        .arg(to)
        .status()
        .unwrap();
    assert!(status.success());
}

/// Stores on boundary 1 with 1 published, a copy of them standing for what a
/// restore of the repository's latest gives, and a batch of 2..=4 prepared.
fn batch_over_a_held_repository(rig: &Rig<'_>) -> PathBuf {
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );
    repository_at(rig, 1);
    let kept = rig.node.path().join("kept");
    copy_tree(&rig.node.config.storage.path, &kept);
    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(frontiers.prepared, Some(4));
    assert_eq!(rig.sequences(), [2, 3, 4]);
    kept
}

fn restore_stores(rig: &Rig<'_>, kept: &Path) {
    let data = &rig.node.config.storage.path;
    std::fs::remove_dir_all(data).unwrap();
    copy_tree(kept, data);
    assert!(!batch_dir(rig).exists());
}

fn stage_under(rig: &Rig<'_>, limits: Limits) -> Result<u64, batch::Error> {
    let workspace = ReplayWorkspace::open(&rig.node.config, rig.chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let observer = Observer::silent();
    let mut batch = rig.batch_under(limits);
    let staged = batch
        .stage(&snapshot, &First, &observer)
        .map(|staged| staged.report().bytes);
    drop(snapshot);
    workspace.finish().unwrap();
    staged
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

/// The journal is gone, and the bundles it named with it as far as anyone can
/// tell: an empty repository has no gap to refuse by.
#[test]
fn a_lost_journal_on_an_empty_repository_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let (_, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(frontiers.prepared, Some(4));

    std::fs::remove_file(batch_dir(&rig).join("journal.json")).unwrap();
    assert!(batch_dir(&rig).join("prepared/bundles/1.json").exists());

    let error = rig.run(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::HistoryLost {
                first: 1,
                sequence: 4,
                ..
            })
        ),
        "{error}"
    );
    assert!(error.to_string().contains("--allow-genesis"), "{error}");
    assert!(rig.sequences().is_empty());
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3, 4]);
}

/// Deleting the batch is the recovery over a held repository, so it is the step
/// an operator can reach for over an empty one too.
#[test]
fn a_deleted_batch_on_an_empty_repository_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    std::fs::remove_dir_all(batch_dir(&rig)).unwrap();

    let error = rig.run(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::HistoryLost { .. })
        ),
        "{error}"
    );
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3, 4]);
}

#[test]
fn a_batch_of_one_after_a_deleted_batch_publishes_nothing() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    std::fs::remove_dir_all(batch_dir(&rig)).unwrap();

    let error = rig.run(Limits::ONE, Some(4)).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::HistoryLost { .. })
        ),
        "{error}"
    );
    assert_eq!(rig.calls.published.get(), 0);
}

/// A repository publish reads the same record, once the repository is empty.
#[test]
fn a_repository_publish_after_a_deleted_batch_publishes_nothing() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    std::fs::remove_dir_all(batch_dir(&rig)).unwrap();

    let _ = rustls::crypto::ring::default_provider().install_default();
    let publish = |dry_run| {
        stelae_cardano::publisher::publish_once(
            &rig.node.config,
            &chain.genesis,
            &retained(),
            stelae_cardano::Selection::default(),
            stelae_cardano::publisher::Destination::Repository {
                direct_s3: None,
                repository: empty_registry().parse().unwrap(),
                auth: stelae_cardano::Auth::Anonymous,
                insecure: true,
                scratch_dir: None,
                rebuild: false,
                concurrency: None,
                verify_carried: false,
                require_new: false,
            },
            dry_run,
            &Observer::silent(),
        )
    };
    let error = publish(false).unwrap_err();
    assert!(error.to_string().contains("--allow-genesis"), "{error}");

    // A dry run publishes nothing, so it has nothing to refuse here.
    let previewed = publish(true).map(|_| ()).map_err(|error| error.to_string());
    assert!(
        !previewed.is_err_and(|error| error.contains("--allow-genesis")),
        "a dry run stops at the record"
    );
}

/// Over a held repository the gap refuses on its own, and nothing else is
/// asked.
#[test]
fn a_deleted_batch_over_a_held_repository_refuses_the_gap() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    batch_over_a_held_repository(&rig);

    std::fs::remove_dir_all(batch_dir(&rig)).unwrap();

    let error = rig.run(limits(3), None).unwrap_err();
    assert!(error.to_string().contains("gap"), "{error}");
}

/// A batch of one over a record it hasn't passed is the same first stele.
#[test]
fn a_first_capture_that_failed_leaves_the_first_stele_possible() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(1));
    assert!(matches!(
        rig.run(limits(2), None),
        Err(backfill::Error::NoRoom { .. })
    ));

    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );
    assert_eq!(rig.calls.published.get(), 1);
}

/// Once the repository holds a stele, the record has nothing left to say.
#[test]
fn the_record_of_a_beginning_is_inert_over_a_held_repository() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(1));
    assert!(rig.run(limits(2), None).is_err());
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );
    repository_at(&rig, 1);

    assert_eq!(
        rig.run(Limits::ONE, Some(2)).unwrap(),
        Outcome::UntilEpoch { sequence: 2 }
    );
    assert_eq!(rig.calls.published.get(), 3);
}

/// Clearing the storage path, the recovery the refusal names, takes the record.
#[test]
fn clearing_the_storage_path_starts_a_history_again() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    dolos::storage::clear_storage(&rig.node.config.storage.path).unwrap();
    rig.restart_with_the_whole_source();

    let (_, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(frontiers.prepared, Some(4));
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
}

/// A window a lost replay left behind can't continue a restored one, and the
/// refusal says which directory to empty.
#[test]
fn a_replay_over_a_surviving_immutable_dir_names_the_directory() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let kept = batch_over_a_held_repository(&rig);

    restore_stores(&rig, &kept);
    let error = rig.run(limits(3), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::StaleWindow { .. }),
        "{error}"
    );
    let immutable = rig.download.join("immutable");
    assert!(
        error.to_string().contains(immutable.to_str().unwrap()),
        "{error}"
    );

    // Past the directory, replay reaches the source, which offers nothing here.
    std::fs::remove_dir_all(&immutable).unwrap();
    assert_eq!(rig.run(limits(3), None).unwrap(), Outcome::UpToDate);
}

/// The same from a cleared store, the route for an empty repository.
#[test]
fn a_genesis_replay_over_a_surviving_immutable_dir_names_the_directory() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    dolos::storage::clear_storage(&rig.node.config.storage.path).unwrap();
    let error = rig.run(limits(4), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::StaleWindow { .. }),
        "{error}"
    );
    assert!(error.to_string().contains("first block"), "{error}");
}

/// Only the stale directory stands in the way: restored stores with their files
/// replay the same sequences.
#[test]
fn restored_stores_with_their_files_replay_the_batch() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let kept = batch_over_a_held_repository(&rig);

    restore_stores(&rig, &kept);
    rig.restart_with_the_whole_source();
    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(frontiers.prepared, Some(4));
    assert_eq!(rig.sequences(), [2, 3, 4]);
}

fn initialize_from(
    rig: &Rig<'_>,
    source: &Path,
    allow_genesis: bool,
) -> Result<stelae_cardano::initialize::Outcome, String> {
    stelae_cardano::initialize::run(
        &rig.node.config,
        &rig.chain.genesis,
        &stelae_cardano::initialize::Options {
            source: dolos_snapshot::restore::Source::Dir(source.to_owned()),
            point: dolos_snapshot::facade::Point::Latest,
            auth: stelae_cardano::Auth::Anonymous,
            insecure: false,
            scratch_dir: None,
            skip_space_check: true,
            resume: false,
            allow_genesis,
        },
        &Observer::silent(),
    )
    .map_err(|error| error.to_string())
}

/// The refusal's own steps, in its order, over a repository holding a stele.
#[test]
fn the_recovery_over_a_held_repository_works_as_written() {
    use stelae_cardano::initialize::Outcome as Init;

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );
    repository_at(&rig, 1);
    let stele = rig.node.path().join("stele");
    stelae_cardano::publisher::publish_once(
        &rig.node.config,
        &chain.genesis,
        &retained(),
        stelae_cardano::Selection::default(),
        stelae_cardano::publisher::Destination::Directory {
            path: stele.clone(),
            scratch_dir: None,
        },
        false,
        &Observer::silent(),
    )
    .unwrap();
    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(frontiers.prepared, Some(4));

    std::fs::remove_file(batch_dir(&rig).join("prepared/bundles/3.json")).unwrap();
    let message = rig.run(limits(3), None).unwrap_err().to_string();
    assert!(
        message.contains("clear the storage path and any store configured outside it"),
        "{message}"
    );

    // Out of order, the restore finds the stores there and changes nothing.
    assert_eq!(initialize_from(&rig, &stele, false), Ok(Init::Existing));

    dolos::storage::clear_storage(&rig.node.config.storage.path).unwrap();
    assert_eq!(
        initialize_from(&rig, &stele, false),
        Ok(Init::Restored { sequence: 1 })
    );
    rig.restart_with_the_whole_source();
    ready(rig.run(limits(3), None));
    assert_eq!(rig.sequences(), [2, 3, 4]);
}

/// A store outside the storage path survives clearing it, and reads as existing
/// stores: the recovery names it, and with it gone a history starts over.
#[test]
fn the_recovery_clears_stores_configured_outside_the_storage_path() {
    use stelae_cardano::initialize::Outcome as Init;

    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);
    let outside = rig.node.path().join("elsewhere");
    rig.node.config.storage.state.path = Some(outside.join("state"));
    rig.node.config.storage.archive.path = Some(outside.join("archive"));
    let no_stele = rig.node.path().join("no-stele");

    assert_eq!(initialize_from(&rig, &no_stele, true), Ok(Init::Genesis));
    ready(rig.run(limits(4), None));
    std::fs::remove_file(batch_dir(&rig).join("prepared/bundles/2.json")).unwrap();
    let message = rig.run(limits(4), None).unwrap_err().to_string();
    assert!(message.contains("configured outside"), "{message}");

    let storage = &rig.node.config.storage.path;
    dolos::storage::clear_storage(storage).unwrap();
    assert_eq!(initialize_from(&rig, &no_stele, true), Ok(Init::Existing));

    std::fs::remove_dir_all(&outside).unwrap();
    assert_eq!(initialize_from(&rig, &no_stele, true), Ok(Init::Genesis));
    rig.restart_with_the_whole_source();
    ready(rig.run(limits(4), None));
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
}

/// The history's own refusal names the same clearing.
#[test]
fn a_lost_history_names_stores_outside_the_storage_path() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));
    std::fs::remove_dir_all(batch_dir(&rig)).unwrap();

    let message = rig.run(limits(3), None).unwrap_err().to_string();
    assert!(message.contains("past the sequence"), "{message}");
    assert!(message.contains("configured outside"), "{message}");
}

/// Every route the refusal names for a batch nothing holds, and what
/// `initialize` does for each.
#[test]
fn the_recovery_names_a_route_for_an_empty_repository() {
    use stelae_cardano::initialize::{self, Outcome as Init};

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));

    std::fs::remove_file(batch_dir(&rig).join("prepared/bundles/2.json")).unwrap();
    let message = rig.run(limits(3), None).unwrap_err().to_string();
    assert!(message.contains("delete the batch"), "{message}");
    assert!(message.contains("clear the storage path"), "{message}");
    assert!(message.contains("--allow-genesis"), "{message}");

    let options = |allow_genesis| initialize::Options {
        source: dolos_snapshot::restore::Source::Dir(rig.node.path().join("no-stele")),
        point: dolos_snapshot::facade::Point::Latest,
        auth: stelae_cardano::Auth::Anonymous,
        insecure: false,
        scratch_dir: None,
        skip_space_check: true,
        resume: false,
        allow_genesis,
    };
    let observer = Observer::silent();

    let at = rig.cursor();
    let existing =
        initialize::run(&rig.node.config, &chain.genesis, &options(false), &observer).unwrap();
    assert_eq!(existing, Init::Existing);
    assert_eq!(rig.cursor(), at);

    dolos::storage::clear_storage(&rig.node.config.storage.path).unwrap();
    let refused =
        initialize::run(&rig.node.config, &chain.genesis, &options(false), &observer).unwrap_err();
    assert!(
        refused.to_string().contains("refusing genesis"),
        "{refused}"
    );
    assert_eq!(
        initialize::run(&rig.node.config, &chain.genesis, &options(true), &observer).unwrap(),
        Init::Genesis
    );
}

/// `Io` is the machine: the bundle may be intact, so nothing advises discarding
/// it, and with the machine fixed the batch goes on.
#[cfg(unix)]
#[test]
fn an_unreadable_intact_bundle_is_not_called_lost() {
    use std::os::unix::fs::PermissionsExt as _;

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));

    let manifest = batch_dir(&rig).join("prepared/bundles/2.json");
    let set =
        |mode| std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(mode)).unwrap();
    set(0o000);
    let outcome = rig.run(limits(3), None);
    set(0o644);
    let error = outcome.unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::BundleUnreadable { sequence: 2, .. })
        ),
        "{error}"
    );
    assert!(!error.to_string().contains("delete the batch"), "{error}");

    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(frontiers.prepared, Some(3));
}

#[cfg(unix)]
#[test]
fn an_unreadable_journal_or_batch_is_reported_as_io() {
    use std::os::unix::fs::PermissionsExt as _;

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));

    let dir = batch_dir(&rig);
    let journal = dir.join("journal.json");
    let set = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    set(&journal, 0o000);
    let outcome = rig.run(limits(3), None);
    set(&journal, 0o644);
    let error = outcome.unwrap_err();
    assert!(
        matches!(error, backfill::Error::Batch(batch::Error::Io { .. })),
        "{error}"
    );
    assert!(!error.to_string().contains("delete the batch"), "{error}");

    set(&dir, 0o000);
    let outcome = rig.run(limits(3), None);
    set(&dir, 0o755);
    let error = outcome.unwrap_err();
    assert!(!error.to_string().contains("delete the batch"), "{error}");
}

/// What the volume can't hold mustn't starve the flush it closes for.
#[test]
fn a_failed_capture_leaves_no_payload_to_starve_the_flush() {
    let chain = Chain::spanning(12);

    let clean = Rig::new(&chain);
    clean.calls.full_at.set(Some(3));
    let (flush, _) = ready(clean.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    let clean_prepared = tree_bytes(&batch_dir(&clean).join("prepared"));
    let clean_needs = stage_under(&clean, limits(9)).unwrap();

    let rig = Rig::new(&chain);
    *rig.calls.prepared_root.borrow_mut() = Some(batch_dir(&rig).join("prepared"));
    rig.calls.payload_then_full.set(Some(3));
    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
    let prepared = tree_bytes(&batch_dir(&rig).join("prepared"));
    assert!(prepared <= clean_prepared + 256, "{prepared}");

    // Room for exactly what the journaled batch needs, plus slack for capture ids.
    let budget = Limits {
        scratch: clean_prepared + clean_needs + 256,
        ..limits(9)
    };
    stage_under(&rig, budget).unwrap();
}

/// A failed first capture has nothing to flush, and charges a rerun nothing.
#[test]
fn a_failed_first_capture_leaves_no_payload_behind() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    *rig.calls.prepared_root.borrow_mut() = Some(batch_dir(&rig).join("prepared"));
    rig.calls.payload_then_full.set(Some(1));

    let error = rig.run(limits(2), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::NoRoom { used: 0, .. }),
        "{error}"
    );
}

/// A bundle the journal never named goes too: nothing vouches for it.
#[test]
fn a_close_on_a_full_volume_drops_a_bundle_the_journal_never_named() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    rig.calls.fail_finish_after.set(Some(3));
    rig.run(limits(9), None).unwrap_err();
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().store().sequences().unwrap(), [1, 2, 3]);

    rig.calls.full_at.set(Some(3));
    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
    assert_eq!(rig.batch().store().sequences().unwrap(), [1, 2]);
    rig.batch().bundles().unwrap();
}

/// A replay filling the volume is a recoverable stop, and a rerun on the short
/// volume closes a batch holding more than one boundary. The stop itself says
/// only what holds whatever the batch: the stores may not stand, a batch of one
/// has no reserve, and a rerun with one boundary held doesn't close.
#[test]
fn a_replay_filling_the_volume_stops_recoverably() {
    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);
    rig.calls.import_full_at.set(Some(3));
    let reserving = Limits {
        reserve_per_epoch: 1000,
        ..limits(9)
    };

    let error = rig.run(reserving, None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::ReplayFilledVolume(_)),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("Make room and rerun"), "{message}");
    assert!(message.contains("the rerun refuses"), "{message}");
    for promise in ["stand", "as it is", "--batch-reserve-mib"] {
        assert!(!message.contains(promise), "{message}");
    }
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None);

    rig.room = Box::new(|_, bytes| bytes <= 2000);
    let (flush, frontiers) = ready(rig.run(reserving, None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
}

/// A close that runs out of room is the same stop as a replay that does, and
/// the capture it lost is taken again on the rerun.
#[test]
fn a_session_close_filling_the_volume_stops_recoverably() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.finish_full_after.set(Some(3));

    let error = rig.run(limits(9), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::ReplayFilledVolume(_)),
        "{error}"
    );
    assert_eq!(rig.sequences(), [1, 2]);

    let (_, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(frontiers.prepared, Some(4));
    assert_eq!(*rig.calls.traversed.borrow(), [3]);
}

/// A damaged chunk isn't another replay's window, and doesn't say so.
#[test]
fn a_damaged_chunk_is_not_read_as_a_stale_window() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let kept = batch_over_a_held_repository(&rig);
    restore_stores(&rig, &kept);

    let chunks = std::fs::read_dir(rig.download.join("immutable"))
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "chunk"));
    for chunk in chunks {
        std::fs::write(chunk.path(), [0x01]).unwrap();
    }

    let error = rig.run(limits(3), None).unwrap_err();
    assert!(matches!(error, backfill::Error::ImmutableDb(_)), "{error}");
}

/// A batch of one has no batch to close and no reserve to raise, and the same
/// stop holds: with room made the rerun goes on.
#[test]
fn a_batch_of_one_filling_the_volume_stops_the_same_way() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.import_full_at.set(Some(2));

    let error = rig.run(Limits::ONE, Some(2)).unwrap_err();
    assert!(
        matches!(error, backfill::Error::ReplayFilledVolume(_)),
        "{error}"
    );
    assert!(!error.to_string().contains("--batch"), "{error}");

    assert_eq!(
        rig.run(Limits::ONE, Some(2)).unwrap(),
        Outcome::UntilEpoch { sequence: 2 }
    );
}

/// A worker's failure reaches the import and the close alike, as one stop that
/// doesn't call the volume full, and the rerun goes on.
#[test]
fn a_failed_worker_stops_the_run_once() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.poison_import_at.set(Some(3));
    rig.calls.poison_finish_at.set(Some(3));

    let error = rig.run(limits(9), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::StorePoisoned(_)),
        "{error}"
    );
    assert_eq!(rig.sequences(), [1, 2]);

    let (_, frontiers) = ready(rig.run(limits(4), None));
    assert_eq!(frontiers.prepared, Some(4));
}

/// The close of a capture that ran out can fail the same way, and the batch
/// still closes on what earlier closes made durable.
#[test]
fn a_failed_close_keeps_a_capture_that_ran_out() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(3));
    rig.calls.poison_finish_at.set(Some(3));

    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
}

/// The same close with no capture lost is no reason to go on, and neither is a
/// close that failed for another reason.
#[test]
fn a_failed_close_stops_unless_a_capture_ran_out_on_the_same_volume() {
    let chain = Chain::spanning(12);

    let rig = Rig::new(&chain);
    rig.calls.poison_finish_at.set(Some(3));
    let error = rig.run(limits(9), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::StorePoisoned(_)),
        "{error}"
    );
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None);

    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(3));
    rig.calls.break_finish_at.set(Some(3));
    let error = rig.run(limits(9), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Caller(_)), "{error}");
    assert_eq!(rig.batch().ready(), None);
}

/// The close can say the volume is full itself, when its own write is the first
/// to meet it, with no worker failed. The batch closes the same.
#[test]
fn a_close_reporting_a_full_volume_keeps_a_capture_that_ran_out() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(3));
    rig.calls.full_finish_at.set(Some(3));

    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
}

/// Every open a run makes reads a full volume the same way.
#[test]
fn a_full_volume_at_any_open_is_the_same_stop() {
    let chain = Chain::spanning(12);
    let mut raw = Vec::new();
    for (what, batching, opens_before) in [
        ("extending a batch of one", Limits::ONE, 1),
        ("settling a batch", limits(9), 0),
        ("extending a batch", limits(9), 1),
    ] {
        let rig = Rig::new(&chain);
        rig.calls.open_full_after.set(Some(opens_before));
        let error = rig.run(batching, None).unwrap_err();
        if !matches!(error, backfill::Error::ReplayFilledVolume(_)) {
            raw.push(format!("{what}: {error}"));
        }
    }
    assert!(raw.is_empty(), "{raw:?}");
}

/// A capture a restart took closes over a workspace rather than a session.
#[test]
fn a_failed_workspace_close_keeps_a_capture_that_ran_out() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.cancel_at_boundary.set(Some(3));
    assert!(matches!(
        rig.run(limits(9), None),
        Err(backfill::Error::Interrupted)
    ));
    assert_eq!(rig.sequences(), [1, 2]);
    rig.calls.cancel.restart();

    rig.calls.full_at.set(Some(3));
    rig.calls.poison_workspace_close.set(true);
    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
}

/// Every place a run touches the stores reads their failure the same way, on a
/// batch of one and a larger one alike.
#[test]
fn a_store_failing_anywhere_in_a_run_is_the_same_stop() {
    let chain = Chain::spanning(12);

    type Fault = (&'static str, fn(&Calls));
    let filling: [Fault; 4] = [
        ("checking", |calls| calls.check_full.set(true)),
        ("opening", |calls| calls.open_full.set(true)),
        ("starting", |calls| calls.start_full.set(true)),
        ("pruning", |calls| calls.prune_full.set(true)),
    ];
    for (what, fault) in filling {
        let rig = Rig::new(&chain);
        assert_eq!(
            rig.run(Limits::ONE, Some(1)).unwrap(),
            Outcome::UntilEpoch { sequence: 1 }
        );
        fault(&rig.calls);
        let error = rig.run(Limits::ONE, Some(2)).unwrap_err();
        assert!(
            matches!(error, backfill::Error::ReplayFilledVolume(_)),
            "{what}: {error}"
        );
        assert_eq!(
            rig.run(Limits::ONE, Some(2)).unwrap(),
            Outcome::UntilEpoch { sequence: 2 },
            "{what}"
        );
    }

    for batching in [Limits::ONE, limits(9)] {
        let rig = Rig::new(&chain);
        rig.source_ends_after(2);
        assert!(rig.run(batching, None).is_ok());
        rig.calls.poison_workspace_close.set(true);
        let error = rig.run(batching, None).unwrap_err();
        assert!(
            matches!(error, backfill::Error::StorePoisoned(_)),
            "{error}"
        );
    }
}

/// The client names only the locations it tried, whatever filled the volume, so
/// the failure carries what the volume holds. Takes the retries' whole backoff.
#[test]
fn a_failed_download_says_what_its_volume_holds() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    std::fs::remove_dir_all(rig.download.join("immutable")).unwrap();
    rig.calls.beacon.set(100);

    let error = rig.run(Limits::ONE, None).unwrap_err();
    assert!(matches!(error, backfill::Error::Mithril { .. }), "{error}");
    let message = error.to_string();
    assert!(message.contains("bytes free"), "{message}");
    assert!(
        message.contains(&rig.download.display().to_string()),
        "{message}"
    );
}

/// Where `index`'s block stands, as a commit of it would leave the cursor.
fn block_point(chain: &Chain, index: usize) -> ChainPoint {
    let block = MultiEraBlock::decode(&chain.blocks[index]).unwrap();
    ChainPoint::Specific(block.slot(), block.hash())
}

/// Write the state cursor alone, as the state's commit does when the archive's
/// after it never lands.
fn state_cursor_at(rig: &Rig<'_>, cursor: ChainPoint) {
    use dolos_core::{StateStore as _, StateWriter as _};

    let state = dolos::storage::open_state_store(&rig.node.config).unwrap();
    let writer = state.start_writer().unwrap();
    writer.set_cursor(cursor).unwrap();
    writer.commit().unwrap();
    state.shutdown().unwrap();
}

fn refuses_archive_behind(rig: &Rig<'_>) -> Result<(), backfill::Error> {
    backfill::refuse_archive_behind(&rig.node.config, rig.chain.genesis.clone())
}

/// A kill or a full volume between the state's commit and the archive's leaves
/// the cursor past blocks nothing can restore. Replaying on would hide the gap,
/// so every rerun refuses before it imports or publishes, whatever it batches.
#[test]
fn stores_whose_state_passed_the_archive_are_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );
    let at = block_point(&chain, 2).slot();
    let past = block_point(&chain, 4);
    state_cursor_at(&rig, past.clone());
    let sessions = rig.calls.sessions.get();
    let published = rig.calls.published.get();

    for batching in [Limits::ONE, limits(3), Limits::ONE] {
        let error = rig.run(batching, None).unwrap_err();
        let backfill::Error::ArchiveBehind { cursor, tip } = error else {
            panic!("{error}");
        };
        assert_eq!(cursor, past.slot());
        assert_eq!(tip, Some(at));
        assert_eq!(rig.cursor(), past, "the refusal moved nothing");
        assert_eq!(rig.calls.sessions.get(), sessions, "no import ran");
        assert_eq!(rig.calls.published.get(), published, "nothing published");
        assert!(rig.calls.prepared.borrow().is_empty(), "nothing captured");
    }

    let message = rig.run(Limits::ONE, None).unwrap_err().to_string();
    assert!(message.contains("latest stele"), "{message}");
    assert!(message.contains("--allow-genesis"), "{message}");
}

/// A batch resuming is no exception: what it holds doesn't vouch for the blocks
/// it would replay past.
#[test]
fn a_batch_doesnt_resume_past_a_gap_in_the_archive() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.cancel_after.set(Some(1));
    assert!(matches!(
        rig.run(limits(4), None),
        Err(backfill::Error::Interrupted)
    ));
    assert_eq!(rig.sequences(), [1]);
    rig.calls.cancel.restart();
    state_cursor_at(&rig, block_point(&chain, 4));

    let error = rig.run(limits(4), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::ArchiveBehind { .. }),
        "{error}"
    );
    assert_eq!(rig.sequences(), [1]);
}

/// Stores with a cursor and no archive at all have lost every block they
/// passed.
#[test]
fn an_empty_archive_under_a_hashed_cursor_is_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    state_cursor_at(&rig, block_point(&chain, 1));
    assert!(matches!(
        refuses_archive_behind(&rig),
        Err(backfill::Error::ArchiveBehind { tip: None, .. })
    ));
}

/// Only a block's commit moves a hashed cursor, so these have no block to miss.
#[test]
fn stores_with_no_hashed_cursor_pass() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    refuses_archive_behind(&rig).unwrap();
    state_cursor_at(&rig, ChainPoint::Origin);
    refuses_archive_behind(&rig).unwrap();
    state_cursor_at(&rig, ChainPoint::Slot(block_point(&chain, 4).slot()));
    refuses_archive_behind(&rig).unwrap();
}

/// With no archive kept there is nothing to fall behind.
#[test]
fn stores_keeping_no_archive_pass() {
    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);
    rig.node.config.history = dolos_core::config::HistoryConfig::none();
    state_cursor_at(&rig, block_point(&chain, 1));
    refuses_archive_behind(&rig).unwrap();
}

/// A stop between ESTART's commit and the epoch's first block leaves a cursor
/// with no hash that nothing anchors again, so its refusal says how to recover.
#[test]
fn a_cursor_estart_left_names_how_to_recover() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(2);
    assert_eq!(rig.run(Limits::ONE, Some(1)).unwrap(), Outcome::UpToDate);
    state_cursor_at(&rig, ChainPoint::Slot(block_point(&chain, 2).slot()));
    rig.restart_with_the_whole_source();

    for batching in [Limits::ONE, limits(3)] {
        let message = rig.run(batching, None).unwrap_err().to_string();
        assert!(message.contains("latest stele"), "{message}");
        assert!(message.contains("--allow-genesis"), "{message}");
    }
}

/// A failed read says nothing of the cursor, so it keeps its cause and sends no
/// one to clear stores that may be sound.
#[test]
fn a_failed_read_of_the_epoch_doesnt_ask_to_clear_the_stores() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(2);
    assert_eq!(rig.run(Limits::ONE, Some(1)).unwrap(), Outcome::UpToDate);

    for batching in [Limits::ONE, limits(3)] {
        rig.calls.epoch_fails.set(true);
        let error = rig.run(batching, None).unwrap_err();
        let backfill::Error::Planning(cause) = &error else {
            panic!("{error}");
        };
        assert!(cause.to_string().contains("read failed"), "{error}");
        assert!(!error.to_string().contains("latest stele"), "{error}");
    }
}

/// An archive no other process sees can't be behind anything, and clearing
/// can't help.
#[test]
fn an_ephemeral_archive_is_not_called_a_gap() {
    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);
    rig.node.config.storage.archive.kind = dolos_core::config::StoreType::Ephemeral;
    state_cursor_at(&rig, block_point(&chain, 1));
    refuses_archive_behind(&rig).unwrap();
}

/// Replay rewrites the blocks ahead only up to the next boundary. Past that the
/// check still passes, and the boundary refuses the publish.
#[test]
fn an_archive_past_the_next_boundary_refuses_the_publish() {
    use dolos_core::{ArchiveStore as _, ArchiveWriter as _};

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(2);
    assert_eq!(rig.run(Limits::ONE, Some(1)).unwrap(), Outcome::UpToDate);

    let archive = dolos::storage::open_archive_store(&rig.node.config).unwrap();
    let writer = archive.start_writer().unwrap();
    for index in 2..=4 {
        writer
            .apply(&block_point(&chain, index), &chain.blocks[index])
            .unwrap();
    }
    writer.commit().unwrap();
    archive.shutdown().unwrap();
    drop(archive);

    refuses_archive_behind(&rig).unwrap();
    rig.restart_with_the_whole_source();
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );

    let workspace = ReplayWorkspace::open(&rig.node.config, chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let plan = snapshot.plan(MAGIC.into(), retained()).unwrap();
    let error = snapshot.digest_document(&plan, &First).unwrap_err();
    drop(snapshot);
    workspace.finish().unwrap();
    assert!(
        error.to_string().contains("runs on past the cursor"),
        "{error}"
    );
}

/// What an archive-first commit order would leave. It heals as the same block
/// imports again, so "behind" is the test rather than "differs".
#[test]
fn an_archive_ahead_of_the_state_heals_on_replay() {
    use dolos_core::{ArchiveStore as _, ArchiveWriter as _};

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(2);
    assert_eq!(rig.run(Limits::ONE, Some(1)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.cursor(), block_point(&chain, 1));

    let archive = dolos::storage::open_archive_store(&rig.node.config).unwrap();
    let writer = archive.start_writer().unwrap();
    writer
        .apply(&block_point(&chain, 2), &chain.blocks[2])
        .unwrap();
    writer.commit().unwrap();
    archive.shutdown().unwrap();
    drop(archive);

    rig.restart_with_the_whole_source();
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );

    let workspace = ReplayWorkspace::open(&rig.node.config, chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let plan = snapshot.plan(MAGIC.into(), retained()).unwrap();
    let healed = snapshot.digest_document(&plan, &First).unwrap();
    drop(snapshot);
    workspace.finish().unwrap();
    assert_eq!(healed.canonical, chain.frozen(1, None).canonical);
}

/// Another failure while replaying stays what it was.
#[test]
fn only_a_full_volume_is_read_as_one_during_replay() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.fail_finish_after.set(Some(2));
    let error = rig.run(limits(9), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Caller(_)), "{error}");
}

/// Stores further than one sequence past the repository can't begin a batch:
/// the first publication would leave a gap.
#[test]
fn stores_past_the_repository_begin_no_batch() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stand_on(&rig, 3);
    repository_at(&rig, 1);

    let error = rig.run(limits(4), None).unwrap_err();
    assert!(error.to_string().contains("gap"), "{error}");
    assert!(rig.calls.prepared.borrow().is_empty());
    assert!(rig.sequences().is_empty());
}

/// A batch over a non-empty repository follows its latest stele: at every
/// boundary, in the journal, in the frontiers and across a restart.
#[test]
fn a_batch_keeps_following_the_stele_it_began_over() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stand_on(&rig, 2);
    repository_at(&rig, 1);

    let (_, frontiers) = ready(rig.run(limits(3), None));
    assert_eq!(rig.sequences(), [2, 3, 4]);
    assert_eq!(frontiers.published, Some(1));
    assert_eq!(*rig.calls.traversed.borrow(), [2]);

    assert_eq!(
        rig.batch().frontiers().published,
        Some(1),
        "the journal holds what the batch follows"
    );

    // A closed batch restarts over the same repository, and nothing moved.
    ready(rig.run(limits(3), None));
    assert_eq!(*rig.calls.prepared.borrow(), [2, 3, 4]);
}

/// A node the repository already holds at `until_epoch` has nothing to do.
#[test]
fn stores_already_published_at_until_epoch_are_done() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stand_on(&rig, 2);
    repository_at(&rig, 2);

    assert_eq!(
        rig.run(limits(4), Some(2)).unwrap(),
        Outcome::UntilEpoch { sequence: 2 }
    );
    assert_eq!(rig.calls.sessions.get(), 0, "replay never started");
    assert!(rig.calls.prepared.borrow().is_empty());
    assert!(rig.sequences().is_empty());
}

/// Sequences the repository holds are replayed, pruned behind and never
/// prepared, and the batch begins after the last of them.
#[test]
fn a_batch_begins_after_what_the_repository_already_holds() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stand_on(&rig, 2);
    repository_at(&rig, 3);

    let (_, frontiers) = ready(rig.run(limits(2), None));
    assert_eq!(*rig.calls.prepared.borrow(), [4, 5], "3 is already held");
    assert_eq!(rig.sequences(), [4, 5]);
    assert_eq!(frontiers.published, Some(3));
    assert_eq!(rig.calls.pruned.get(), 2, "after each published boundary");
    assert_eq!(rig.calls.published.get(), 0);
}

/// Only a full volume closes a batch. Any other failed capture is a failed run:
/// it closes nothing and flushes nothing.
#[test]
fn a_capture_failing_for_another_reason_fails_the_run() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // Stands on 3 with a bundle the journal doesn't name yet, then damaged.
    rig.calls.fail_finish_after.set(Some(3));
    rig.run(limits(9), None).unwrap_err();
    assert_eq!(rig.sequences(), [1, 2]);
    let dir = batch::dir_in(&rig.node.config.storage.path);
    std::fs::write(dir.join("prepared/bundles/3.json"), b"{").unwrap();

    let error = rig.run(limits(9), None).unwrap_err();
    assert!(
        matches!(error, backfill::Error::Batch(batch::Error::Preparing(_))),
        "{error}"
    );
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None, "the batch did not close");
}

/// A sound bundle standing on the recorded boundary and holding other layers:
/// the bundle changes here, where
/// a_bundle_that_is_not_the_one_recorded_is_refused changes the journal's
/// record of it.
#[test]
fn a_sound_bundle_with_other_layers_on_the_same_boundary_is_refused() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(3), None));

    let batch = rig.batch();
    let recorded = batch.store().load(3).unwrap();
    let dir = batch::dir_in(&rig.node.config.storage.path);
    std::fs::remove_file(dir.join("prepared/bundles/3.json")).unwrap();

    let workspace = ReplayWorkspace::open(&rig.node.config, chain.genesis.clone()).unwrap();
    let snapshot = workspace.snapshot();
    let mut plan = snapshot.plan(MAGIC.into(), retained()).unwrap();
    plan.epochs.retain(|window| window.epoch == 3);
    let request = prepare::Request {
        store_contract: batch.source().store_contract.clone(),
        mutable_slots: batch.source().mutable_slots,
        tail: prepare::IndexTail::Traverse,
    };
    let other = snapshot
        .prepare_boundary(batch.store(), &plan, &request, &Observer::silent())
        .unwrap();
    assert_eq!(other.cursor(), recorded.cursor(), "the same boundary");
    assert!(other.layers().count() < recorded.layers().count());
    drop(snapshot);
    workspace.finish().unwrap();

    let error = rig.run(limits(3), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::BundleChanged { sequence: 3, .. })
        ),
        "{error}"
    );
}

/// A batch closed with the stores on a boundary it never captured stays closed:
/// no traversal, and no growth on the volume that just filled.
#[test]
fn a_batch_closed_behind_the_stores_stays_closed() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(3));
    let (flush, _) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(rig.sequences(), [1, 2]);

    let (flush, frontiers) = ready(rig.run(limits(9), None));
    assert_eq!(flush, Flush::DiskPressure);
    assert_eq!(frontiers.prepared, Some(2));
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2]);
    assert!(rig.calls.traversed.borrow().is_empty());
}

/// Free space is really measured, and a shortfall really refuses.
#[test]
fn free_space_is_measured() {
    let node = Node::new();
    assert!(batch::volume_has_room(node.path(), 0));
    assert!(!batch::volume_has_room(node.path(), u64::MAX));
}

/// What admission measures is the volume holding the batch.
#[test]
fn admission_measures_the_volume_holding_the_batch() {
    let chain = Chain::spanning(12);
    let mut rig = Rig::new(&chain);
    let seen = Rc::new(RefCell::new(Vec::<PathBuf>::new()));
    let record = seen.clone();
    rig.room = Box::new(move |path, _| {
        record.borrow_mut().push(path.to_owned());
        true
    });

    ready(rig.run(limits(2), None));

    let dir = batch::dir_in(&rig.node.config.storage.path);
    let seen = seen.borrow();
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|path| *path == dir), "{seen:?}");
}

/// Consumed files outlast the session that captured the boundary, because until
/// it closes the stores may fall back to a cursor that still reads them.
#[test]
fn consumed_input_outlasts_the_session_that_captured_the_boundary() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // 2 is captured and its session fails to close, so the last cleanup was at
    // 1, whose first block is in file 20. Files 20 and 30 lead to 2.
    rig.calls.fail_finish_after.set(Some(2));
    rig.run(limits(9), None).unwrap_err();
    assert_eq!(rig.sequences(), [1]);
    let files = rig.immutable_files();
    assert_eq!(files.first(), Some(&20), "{files:?}");
}

/// A failed read of what the batch holds says the batch, not its journal.
#[cfg(unix)]
#[test]
fn a_scan_failure_does_not_blame_the_journal() {
    use std::os::unix::fs::PermissionsExt as _;

    let chain = Chain::spanning(6);
    let rig = Rig::new(&chain);
    drop(rig.batch());

    let blobs = batch_dir(&rig).join("prepared/blobs/sha256");
    std::fs::set_permissions(&blobs, std::fs::Permissions::from_mode(0o000)).unwrap();
    let outcome = rig.run(limits(2), None);
    std::fs::set_permissions(&blobs, std::fs::Permissions::from_mode(0o755)).unwrap();

    let message = outcome.err().map(|error| error.to_string()).unwrap();
    assert!(!message.contains("journal"), "{message}");
}

/// Publishes the whole chain with nothing going wrong, as what every
/// interrupted run below has to converge on.
fn uninterrupted(chain: &Chain) -> BTreeMap<u64, Vec<u8>> {
    let rig = Rig::new(chain);
    assert_eq!(rig.drain(limits(4), None).unwrap(), Outcome::UpToDate);
    rig.repository.tags()
}

fn latest_sequence(rig: &Rig<'_>) -> Option<u64> {
    rig.repository.latest.borrow().as_ref().map(|l| l.sequence)
}

fn journal_bytes(rig: &Rig<'_>) -> Vec<u8> {
    std::fs::read(batch_dir(rig).join("journal.json")).unwrap()
}

fn assert_batch_is_gone(rig: &Rig<'_>) {
    let dir = batch_dir(rig);
    assert!(!dir.join("journal.json").exists());
    assert!(!dir.join("staging").exists());
    for held in ["prepared/bundles", "prepared/blobs/sha256"] {
        let held = std::fs::read_dir(dir.join(held)).map_or(0, |entries| entries.count());
        assert_eq!(held, 0);
    }
}

/// Every sequence in order, one staging for the batch, each stele what stores
/// frozen at its boundary publish, chained to the real digest before it.
#[test]
fn a_batch_publishes_in_order_as_frozen_stores_do_from_one_staging() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    assert_eq!(
        rig.drain(limits(4), Some(4)).unwrap(),
        Outcome::UntilEpoch { sequence: 4 }
    );

    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4]);
    assert_eq!(*rig.calls.prepared.borrow(), [1, 2, 3, 4]);
    assert!(rig.calls.traversed.borrow().is_empty(), "no hidden scan");
    assert_eq!(rig.calls.published.get(), 0, "no publish from the stores");

    let stagings = rig.calls.stagings.borrow();
    assert_eq!(stagings.len(), 1, "one staging: {stagings:?}");
    assert_eq!(stagings[0].tag_traversals, 1);
    assert_eq!(stagings[0].exact_traversals, 1);
    assert!(stagings[0].entity_traversals <= 1);

    let mut previous: Option<Document> = None;
    for sequence in 1..=4 {
        let frozen = chain.frozen(sequence, previous.as_ref());
        let tags = rig.repository.tags();
        assert_eq!(tags[&sequence], frozen.canonical, "at sequence {sequence}");

        let inscription = rig.repository.inscription(sequence).unwrap();
        let chained = inscription
            .history
            .last()
            .map(|entry| entry.inscription_digest);
        assert_eq!(
            chained,
            previous.as_ref().map(|previous| previous.identity),
            "at sequence {sequence}"
        );
        previous = Some(frozen);
    }

    let committed = Repository::committed(4, previous.as_ref().unwrap());
    assert_eq!(*rig.repository.latest.borrow(), Some(committed));
    assert_batch_is_gone(&rig);
}

/// A payload stays until the last bundle naming it has committed, and goes
/// then.
#[test]
fn shared_payloads_stay_until_their_last_consumer_commits() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    // A mutable window past an epoch, as Byron's is, so boundaries capture the
    // logs of a window still unsealed alike. Preview's own can't be that long.
    let mut longer = (*chain.genesis).clone();
    longer.byron.protocol_consts.k *= 8;
    *rig.driver_genesis.borrow_mut() = Arc::new(longer);
    ready(rig.run(limits(4), None));

    let bundles = rig.batch().bundles().unwrap();
    let held = |from: usize| -> BTreeSet<_> {
        bundles[from..]
            .iter()
            .flat_map(|bundle| bundle.references())
            .collect()
    };
    let named: usize = bundles.iter().map(|b| b.references().len()).sum();
    assert!(
        held(0).len() < named,
        "no payload is shared in this fixture"
    );

    rig.drain(limits(4), Some(4)).unwrap();

    let expected: Vec<_> = (0..4)
        .map(|index| (bundles[index].sequence(), held(index).len()))
        .collect();
    assert_eq!(*rig.repository.payloads.borrow(), expected);
}

/// Replay stands still from the batch's close to its last commit, and
/// housekeeping waits for that commit.
#[test]
fn nothing_prunes_until_the_batch_has_drained() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let at_close = rig.cursor();

    rig.repository.fault.set(Some((3, Fault::Upload)));
    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(rig.calls.pruned.get(), 0);
    assert_eq!(rig.cursor(), at_close);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    // Once for each batch that drained: this one, and the last boundary's.
    assert_eq!(rig.calls.pruned.get(), 2);
}

#[test]
fn each_commit_moves_the_published_frontier() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.drain(limits(4), Some(4)).unwrap();

    let reported: Vec<_> = rig
        .calls
        .frontiers
        .borrow()
        .iter()
        .map(|frontiers| frontiers.published)
        .collect();
    let first = reported.iter().position(|p| p.is_some()).unwrap();
    assert_eq!(reported[first..], [Some(1), Some(2), Some(3), Some(4)]);
}

/// A periodic run publishes what it prepared, and what the source offers next
/// starts another batch: nothing waits at the tip for a full one.
#[test]
fn a_run_at_the_tip_publishes_a_partial_batch_and_the_next_run_goes_on() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(5);

    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2]);
    assert_batch_is_gone(&rig);

    rig.restart_with_the_whole_source();
    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// An epoch in progress has no boundary: it stays out, however the batch
/// closed.
#[test]
fn a_run_ending_in_mid_epoch_publishes_only_captured_boundaries() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);

    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(latest_sequence(&rig), Some(2));
    assert_eq!(rig.calls.stagings.borrow().len(), 1);
    assert_batch_is_gone(&rig);
}

/// A capture that filled the volume closes the batch behind the stores.
/// Publishing frees its room, and the uncaptured boundary begins the next
/// batch.
#[test]
fn a_batch_closed_by_a_full_volume_publishes_and_goes_on() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.full_at.set(Some(3));

    assert_eq!(rig.drain(limits(9), None).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(*rig.calls.traversed.borrow(), [3], "the delta died with it");
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The first batch, its successor and every sequence past `until_epoch` left
/// alone.
#[test]
fn successive_batches_drain_in_turn_up_to_the_epoch_asked_for() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    assert_eq!(
        rig.drain(limits(2), Some(3)).unwrap(),
        Outcome::UntilEpoch { sequence: 3 }
    );
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3]);
    let stagings = rig.calls.stagings.borrow();
    assert_eq!(stagings.len(), 2, "one per batch");

    // The second batch spools only what its predecessor doesn't carry, not every
    // closed epoch up to its boundary.
    let whole = Rig::new(&chain);
    whole.drain(limits(3), Some(3)).unwrap();
    let closed_epochs = whole.calls.stagings.borrow()[0].bytes;
    assert!(stagings[1].bytes > 0, "{stagings:?}");
    assert!(
        stagings[1].bytes < closed_epochs,
        "{stagings:?} against {closed_epochs}"
    );
}

fn source_down(_: &Calls) -> backfill::Error {
    backfill::Error::Mithril {
        what: "listing mithril snapshots",
        reason: "the aggregator didn't answer".to_owned(),
    }
}

fn source_stalled(_: &Calls) -> backfill::Error {
    backfill::Error::StalledWindow {
        start: 0,
        end: 1,
        highest: None,
        after: None,
        cursor_slot: 0,
        contents: String::new(),
    }
}

/// A source that stays down publishes what a batch prepared, as a batch of one
/// already did, and the run still ends on that failure. The next run that can
/// reach the source goes on.
#[test]
fn a_failing_source_doesnt_hold_back_what_a_batch_prepared() {
    let chain = Chain::spanning(12);
    let one = Rig::new(&chain);
    one.source_ends_after(6);
    one.calls.source_fails.set(Some(source_down));
    let error = one.drain(Limits::ONE, None).unwrap_err();
    assert!(matches!(error, backfill::Error::Mithril { .. }), "{error}");
    assert_eq!(one.calls.published.get(), 2);

    let failures: [SourceFailure; 2] = [source_down, source_stalled];
    for failure in failures {
        let rig = Rig::new(&chain);
        rig.source_ends_after(6);
        rig.calls.source_fails.set(Some(failure));

        let error = rig.drain(limits(16), None).unwrap_err();
        assert_eq!(error.to_string(), failure(&rig.calls).to_string());
        assert_eq!(*rig.repository.uploaded.borrow(), [1, 2]);
        assert_batch_is_gone(&rig);

        rig.calls.source_fails.set(None);
        rig.restart_with_the_whole_source();
        assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
        assert_eq!(rig.repository.tags(), uninterrupted(&chain));
    }
}

#[test]
fn a_failing_source_with_nothing_prepared_only_fails() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(1);
    rig.calls.source_fails.set(Some(source_down));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Mithril { .. }), "{error}");
    assert_eq!(rig.calls.stage_attempts.get(), 0);
}

/// A shutdown is honoured first: a staging would outlast its grace period.
#[test]
fn a_shutdown_while_the_source_fails_publishes_nothing() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(|calls| {
        calls.cancel.get().cancel();
        source_down(calls)
    }));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Mithril { .. }), "{error}");
    assert_eq!(rig.calls.stage_attempts.get(), 0);
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None);
}

/// The operator has two things to act on, so the exit names both. The batch
/// stays closed for the rerun, at where the stores reached, and the rerun goes
/// on.
#[test]
fn a_flush_failing_after_the_source_did_reports_both() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(source_down));
    rig.repository.fault.set(Some((2, Fault::FullVolume)));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::SourceAndFlush { flush, failure }
                if matches!(**flush, backfill::Error::FlushNoRoom(_))
                    && matches!(**failure, backfill::Error::Mithril { .. })
        ),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("ran out of room"), "{message}");
    assert!(
        message.contains(&source_down(&rig.calls).to_string()),
        "{message}"
    );
    let backfill::Error::SourceAndFlush { failure, flush } = &error else {
        unreachable!()
    };
    assert_eq!(
        message,
        format!(
            "reading the source failed ({failure}), and publishing what the batch prepared then \
             failed ({flush}). Deal with both before the rerun"
        )
    );
    assert_eq!(*rig.repository.uploaded.borrow(), [1]);
    let batch = rig.batch();
    assert_eq!(batch.ready(), Some(Flush::EndOfSource));
    let replay = batch.frontiers().replay.unwrap();
    assert_eq!(replay.slot, rig.cursor().slot());
    assert!(replay.slot > batch.prepared().last().unwrap().point.slot);

    rig.calls.source_fails.set(None);
    rig.restart_with_the_whole_source();
    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The flush can't reopen the stores on the volume the fetch filled. The exit
/// keeps the fetch's failure beside that one, and the batch, which nothing
/// closed, goes on.
#[test]
fn a_flush_that_cant_reopen_the_stores_keeps_the_sources_failure() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(|calls| {
        calls.open_full.set(true);
        source_down(calls)
    }));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::SourceAndFlush { flush, failure }
                if matches!(**flush, backfill::Error::ReplayFilledVolume(_))
                    && matches!(**failure, backfill::Error::Mithril { .. })
        ),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains(&source_down(&rig.calls).to_string()),
        "{error}"
    );
    assert!(rig.repository.uploaded.borrow().is_empty());
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None);

    rig.calls.source_fails.set(None);
    rig.restart_with_the_whole_source();
    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The stores' shutdown fails with the source, so the run doesn't start a
/// flush. The exit names both and the batch stays open. The fixture's stores
/// did close, so its rerun goes on.
#[test]
fn a_shutdown_failing_with_the_source_leaves_the_batch_open() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(source_down));
    rig.calls.full_finish_at.set(Some(3));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::ReplayAndShutdown { replay, shutdown }
                if matches!(**replay, backfill::Error::Mithril { .. })
                    && matches!(**shutdown, backfill::Error::ReplayFilledVolume(_))
        ),
        "{error}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&source_down(&rig.calls).to_string()),
        "{message}"
    );
    assert!(message.contains("ran out of room"), "{message}");
    assert!(rig.repository.uploaded.borrow().is_empty());
    assert_eq!(rig.sequences(), [1, 2]);
    assert_eq!(rig.batch().ready(), None);

    rig.calls.source_fails.set(None);
    rig.restart_with_the_whole_source();
    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The stores the flush opened to read the cursor won't close, before the
/// journal records the close: the exit keeps the source's failure and the batch
/// stays open.
#[test]
fn a_store_close_failing_before_the_batch_closes_leaves_it_open() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(|calls| {
        calls.poison_workspace_close.set(true);
        source_down(calls)
    }));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::SourceAndFlush { flush, failure }
                if matches!(**flush, backfill::Error::StorePoisoned(_))
                    && matches!(**failure, backfill::Error::Mithril { .. })
        ),
        "{error}"
    );
    assert!(rig.repository.uploaded.borrow().is_empty());
    assert_eq!(rig.batch().ready(), None);
}

/// A reopen failing after the journal recorded the close: the same exit, but
/// the batch stays closed and the rerun publishes it whatever the controls say.
#[test]
fn a_reopen_failing_after_the_batch_closed_leaves_it_closed() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(|calls| {
        calls.open_full_after.set(Some(1));
        source_down(calls)
    }));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::SourceAndFlush { flush, failure }
                if matches!(**flush, backfill::Error::ReplayFilledVolume(_))
                    && matches!(**failure, backfill::Error::Mithril { .. })
        ),
        "{error}"
    );
    assert!(rig.repository.uploaded.borrow().is_empty());
    assert_eq!(rig.batch().ready(), Some(Flush::EndOfSource));

    rig.calls.source_fails.set(None);
    rig.restart_with_the_whole_source();
    assert_eq!(rig.drain(limits(16), None).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// A real fetch failing with a batch prepared, then a publication that runs out
/// of room as it would on the volume the download filled: the exit keeps the
/// free space the fetch read. Takes the retries' whole backoff.
#[test]
fn a_failed_download_is_still_named_when_the_flush_runs_out_of_room() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.beacon.set(100);
    rig.repository.fault.set(Some((1, Fault::FullVolume)));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(&error, backfill::Error::SourceAndFlush { .. }),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("ran out of room"), "{message}");
    assert!(
        message.contains("fetching a mithril immutable window"),
        "{message}"
    );
    assert!(message.contains("bytes free"), "{message}");
    assert!(rig.repository.uploaded.borrow().is_empty());
    assert_eq!(rig.batch().ready(), Some(Flush::EndOfSource));
}

/// A shutdown during that flush stops it, and the source's failure is still
/// told.
#[test]
fn a_shutdown_during_the_flush_keeps_the_sources_failure() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.source_ends_after(6);
    rig.calls.source_fails.set(Some(source_down));
    rig.repository.fault.set(Some((1, Fault::AfterAnswer)));

    let error = rig.drain(limits(16), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::SourceAndFlush { flush, .. }
                if matches!(**flush, backfill::Error::Interrupted)
        ),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains(&source_down(&rig.calls).to_string()),
        "{error}"
    );
    assert_eq!(*rig.repository.uploaded.borrow(), [1]);
}

/// A batch following a history a batch of one began, whose publish record is
/// the old way a publish resumed, keeps that history.
#[test]
fn a_batch_continues_a_history_a_batch_of_one_began() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.keep_singles.set(true);
    assert_eq!(
        rig.run(Limits::ONE, Some(1)).unwrap(),
        Outcome::UntilEpoch { sequence: 1 }
    );

    assert_eq!(rig.drain(limits(3), None).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

#[test]
fn a_kill_between_capture_and_journal_converges() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);

    // 2 is captured and its stores don't close, so nothing journals it.
    rig.calls.fail_finish_after.set(Some(2));
    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(rig.sequences(), [1]);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The markers survive the process, so the rerun walks nothing again.
#[test]
fn a_kill_after_the_staging_markers_reuses_them() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.die_after_staging.set(true);
    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(latest_sequence(&rig), None);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);

    let stagings = rig.calls.stagings.borrow();
    assert!(stagings[0].tag_traversals > 0, "{stagings:?}");
    assert_eq!(stagings[1].tag_traversals, 0, "{stagings:?}");
    assert_eq!(stagings[1].exact_traversals, 0, "{stagings:?}");
    assert_eq!(stagings[1].entity_traversals, 0, "{stagings:?}");
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// A family whose marker never landed is walked again, and only that family.
#[test]
fn a_kill_before_a_staging_marker_walks_that_family_again() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.die_after_staging.set(true);
    rig.drain(limits(4), None).unwrap_err();
    std::fs::remove_file(batch_dir(&rig).join("staging/indexes.json")).unwrap();

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);

    let stagings = rig.calls.stagings.borrow();
    assert_eq!(stagings[1].tag_traversals, 1, "{stagings:?}");
    assert_eq!(stagings[1].entity_traversals, 0, "{stagings:?}");
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Runs are disposable, so damage to a marker costs a rebuild from the stores.
#[test]
fn a_damaged_staging_marker_is_rebuilt_from_the_stores() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.die_after_staging.set(true);
    rig.drain(limits(4), None).unwrap_err();
    std::fs::write(batch_dir(&rig).join("staging/indexes.json"), b"{").unwrap();

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Runs of another frontier were staged for something else. Staged again, they
/// still have to stand on the chain the bundles were captured from.
#[test]
fn runs_of_another_batch_are_rebuilt_from_the_stores() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.die_after_staging.set(true);
    rig.drain(limits(4), None).unwrap_err();

    let marker = batch_dir(&rig).join("staging/indexes.json");
    let mut held: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    held["identity"]["frontier"][0] = 1.into();
    std::fs::write(&marker, serde_json::to_vec(&held).unwrap()).unwrap();

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

fn flip_a_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

/// A run the marker can't see is damaged only shows when it is read. It
/// produces no layer, and the flush stages again rather than stopping for good.
#[test]
fn a_damaged_run_is_found_at_finalization_and_staged_again() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.calls.die_after_staging.set(true);
    rig.drain(limits(4), None).unwrap_err();

    let runs = batch_dir(&rig).join("staging/indexes");
    let run = std::fs::read_dir(&runs)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .max_by_key(|path| std::fs::metadata(path).unwrap().len())
        .unwrap();
    flip_a_byte(&run);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    // The killed run, its rerun reusing the runs, and the rebuild.
    let stagings = rig.calls.stagings.borrow();
    assert_eq!(stagings[1].tag_traversals, 0, "{stagings:?}");
    assert_eq!(stagings[2].tag_traversals, 1, "{stagings:?}");
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Runs that keep failing stop after the one rebuild. Otherwise a flush nobody
/// watches would walk the stores over and over.
#[test]
fn runs_that_stay_unusable_stop_after_one_rebuild() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let attempts = Cell::new(0);
    let publisher = Around {
        inner: &rig.repository,
        finalize: &|_, _, _| {
            attempts.set(attempts.get() + 1);
            assert!(attempts.get() <= 2, "the flush kept staging again");
            let damaged = dolos_snapshot::staging::Refusal::Corrupt {
                path: "a-run".into(),
                reason: "damaged again".to_owned(),
            };
            Err(backfill::Error::Finalizing(Box::new(damaged.into())))
        },
    };

    let error = rig.drive(limits(4), None, true, &publisher).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::RunsUnusable { .. })
        ),
        "{error}"
    );
    assert_eq!(attempts.get(), 2);
    assert_eq!(rig.calls.stagings.borrow().len(), 2);
}

/// A registry failing a publication is asked again, and stays a registry's
/// failure: nothing tells the operator to clear a healthy batch. The second try
/// cancels, which spares the rest of the backoff.
#[test]
fn a_registry_that_wont_answer_is_asked_again_and_the_batch_stands() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let attempts = Cell::new(0);
    let publisher = Around {
        inner: &rig.repository,
        finalize: &|_, _, _| {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 2 {
                rig.calls.cancel.get().cancel();
            }
            let refused =
                stelae::Error::Registry(oci_client::errors::OciDistributionError::ServerError {
                    code: 503,
                    url: "https://registry.invalid/v2/x/blobs/uploads/".to_owned(),
                    message: "unavailable".to_owned(),
                });
            Err(backfill::Error::Finalizing(Box::new(refused.into())))
        },
    };

    let error = rig.drive(limits(4), None, true, &publisher).unwrap_err();
    assert!(matches!(error, backfill::Error::Finalizing(_)), "{error}");
    assert!(!error.to_string().contains("clear"), "{error}");
    assert_eq!(attempts.get(), 2);
    assert_eq!(rig.sequences(), [1, 2, 3, 4]);
    assert_eq!(rig.batch().frontiers().published, None);
}

#[test]
fn a_kill_during_an_upload_converges() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((2, Fault::Upload)));

    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(latest_sequence(&rig), Some(1));
    assert_eq!(rig.sequences(), [2, 3, 4]);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The stele landed and nothing said so: the rerun finds it, proves it is this
/// batch's by its identity, and doesn't upload it again.
#[test]
fn a_lost_acknowledgement_reconciles_without_another_upload() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((2, Fault::Acknowledgement)));

    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(latest_sequence(&rig), Some(2));
    assert_eq!(rig.sequences(), [2, 3, 4], "the journal hasn't heard");

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The same within one process, whose next attempt finds what the last one did.
#[test]
fn an_attempt_after_a_lost_acknowledgement_reconciles_in_place() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository
        .fault
        .set(Some((2, Fault::AcknowledgementLostAlive)));

    assert_eq!(rig.drain(limits(4), None).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The first sequence has no predecessor to read, which reconciling also
/// covers.
#[test]
fn a_lost_acknowledgement_of_a_first_stele_reconciles() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((1, Fault::Acknowledgement)));
    rig.drain(limits(4), None).unwrap_err();

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// A seal writes the immutable tag first. One left behind is checked to be this
/// batch's, and sealing again finishes it with the same bytes.
#[test]
fn a_seal_that_stopped_half_way_is_finished() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((2, Fault::SealHalfWay)));

    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(latest_sequence(&rig), Some(1));
    assert!(rig.repository.tags().contains_key(&2));

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The stele landed and its journal write failed, so the journal still names
/// it: the rerun reconciles rather than uploading it again.
#[cfg(unix)]
#[test]
fn a_journal_commit_that_failed_reconciles() {
    use std::os::unix::fs::PermissionsExt as _;

    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository
        .fault
        .set(Some((2, Fault::UnwritableJournal)));

    let error = rig.drain(limits(4), None).unwrap_err();
    std::fs::set_permissions(batch_dir(&rig), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        matches!(error, backfill::Error::Batch(batch::Error::Io { .. })),
        "{error}"
    );
    assert_eq!(latest_sequence(&rig), Some(2));
    assert_eq!(rig.sequences(), [2, 3, 4]);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// The journal moved and its bundle didn't go: nothing names it, so it does.
#[test]
fn a_kill_between_the_journal_and_the_release_sweeps_the_bundle() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let prepared = batch_dir(&rig).join("prepared");
    let kept = rig.node.path().join("kept-prepared");
    copy_tree(&prepared, &kept);

    rig.repository.fault.set(Some((2, Fault::AfterAnswer)));
    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Interrupted), "{error}");
    assert_eq!(rig.sequences(), [3, 4]);

    std::fs::remove_dir_all(&prepared).unwrap();
    copy_tree(&kept, &prepared);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
    assert_batch_is_gone(&rig);
}

/// Cancelling between sequences keeps what committed and what is left.
#[test]
fn a_cancelled_flush_goes_on_from_the_next_sequence() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((2, Fault::AfterAnswer)));

    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(matches!(error, backfill::Error::Interrupted), "{error}");
    assert_eq!(latest_sequence(&rig), Some(2));
    let batch = rig.batch();
    assert_eq!(batch.frontiers().published, Some(2));
    assert_eq!(batch.bundles().unwrap().len(), 2, "what is left stands");

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4, 5]);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Cleanup that stopped part way is only ever repeated. It holds nothing a
/// later sequence needs, so even a bundle it can't read doesn't stop it.
#[test]
fn a_cleanup_that_stopped_part_way_finishes() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let mut journal: serde_json::Value = serde_json::from_slice(&journal_bytes(&rig)).unwrap();
    rig.drain(limits(4), Some(4)).unwrap();

    let dir = batch_dir(&rig);
    let leave = |journal: &mut serde_json::Value| {
        journal["prepared"] = serde_json::json!([]);
        journal["source"]["dolos_revision"] = "another build".into();
        std::fs::write(
            dir.join("journal.json"),
            serde_json::to_vec(journal).unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(dir.join("staging/indexes")).unwrap();
        std::fs::write(dir.join("staging/indexes/1.run"), b"left").unwrap();
        std::fs::write(dir.join("prepared/bundles/9.json"), b"{").unwrap();
    };

    leave(&mut journal);
    assert_eq!(rig.drain(limits(4), None).unwrap(), Outcome::UpToDate);
    assert_batch_is_gone(&rig);
    assert_eq!(latest_sequence(&rig), Some(5));

    // A batch of one finishes it too, rather than leaving the runs on the volume.
    leave(&mut journal);
    rig.run(Limits::ONE, None).unwrap();
    assert_batch_is_gone(&rig);
}

/// Cleaning up only ever follows the last commit.
#[test]
fn clearing_a_batch_with_a_sequence_left_keeps_it() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(2), None));

    let mut batch = rig.batch();
    batch.clear().unwrap();
    batch::clear_if_drained(&rig.node.config.storage.path).unwrap();
    assert_eq!(rig.batch().bundles().unwrap().len(), 2);
}

/// Half way through, what is already published and what is left both stand.
fn stopped_after_the_first_commit(rig: &Rig<'_>) {
    rig.repository.fault.set(Some((1, Fault::AfterAnswer)));
    rig.drain(limits(4), None).unwrap_err();
    assert_eq!(latest_sequence(rig), Some(1));
    rig.calls.cancel.restart();
}

fn assert_nothing_moved(rig: &Rig<'_>, journal: &[u8], uploaded: &[u64]) {
    assert_eq!(journal_bytes(rig), journal);
    assert_eq!(*rig.repository.uploaded.borrow(), uploaded);
}

#[test]
fn another_source_refuses_a_batch_part_way_through() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    *rig.named.borrow_mut() = "oci://registry.invalid/cardano/other".to_owned();
    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::SourceChanged {
                what: "repository",
                ..
            })
        ),
        "{error}"
    );
    assert_nothing_moved(&rig, &journal, &[1]);

    *rig.named.borrow_mut() = REPOSITORY.to_owned();
    assert_eq!(rig.drain(limits(4), None).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Another writer's stele at the next sequence differs in its identity, and
/// nothing overwrites it or moves past it.
#[test]
fn another_writers_stele_at_the_next_sequence_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    let theirs = Committed {
        sequence: 2,
        identity: "sha256:another-writer".to_owned(),
    };
    *rig.repository.latest.borrow_mut() = Some(theirs.clone());
    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::Batch(batch::Error::Diverged { sequence: 2, found, .. })
                if *found == theirs.identity
        ),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("Nothing was overwritten"), "{message}");
    assert!(message.contains("clear the storage path"), "{message}");
    assert_nothing_moved(&rig, &journal, &[1]);
    assert_eq!(*rig.repository.latest.borrow(), Some(theirs));
}

/// A writer that left only its immutable tag is caught before a seal could
/// write over that tag. A half seal of this publisher's over stores since
/// replaced looks the same, so the refusal names both and a recovery for each.
#[test]
fn another_writers_immutable_tag_refuses_before_it_is_overwritten() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    let mut theirs = rig.repository.steles.borrow()[&1].clone();
    theirs.identity = stelae::Digest::compute(b"another writer");
    rig.repository.steles.borrow_mut().insert(2, theirs.clone());

    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            &error,
            backfill::Error::Batch(batch::Error::TagTaken { sequence: 2, found, expected, .. })
                if *found == theirs.identity.to_string() && expected != found
        ),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("another writer stopped"), "{message}");
    assert!(message.contains("these stores aren't"), "{message}");
    assert!(message.contains("delete the tag and rerun"), "{message}");
    assert!(message.contains("leave the tag"), "{message}");
    assert!(message.contains("clear the storage path"), "{message}");
    assert!(!message.contains("nothing needs clearing"), "{message}");
    let backfill::Error::Batch(batch::Error::TagTaken {
        found, expected, ..
    }) = &error
    else {
        unreachable!()
    };
    assert!(
        message.contains(&format!(
            "holds {found} where this batch would publish {expected}"
        )),
        "{message}"
    );
    // Each recovery stays with its cause: deleting the tag only for a tag known
    // to be another writer's, and the warning and the replay for everything else.
    assert!(
        message.contains(
            "If you know that tag is another writer's, stop that writer, delete the tag and \
             rerun. Otherwise leave the tag and treat the stores as wrong, because deleting it \
             publishes from them unchecked. Nothing else holds those boundaries"
        ),
        "{message}"
    );
    assert_nothing_moved(&rig, &journal, &[1]);
    assert_eq!(rig.repository.steles.borrow()[&2].identity, theirs.identity);
}

/// A failed read of the tag isn't an absent tag: the seal would push over
/// whatever that tag holds.
#[test]
fn an_immutable_tag_that_cant_be_read_isnt_sealed_over() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    let mut theirs = rig.repository.steles.borrow()[&1].clone();
    theirs.identity = stelae::Digest::compute(b"another writer");
    rig.repository.steles.borrow_mut().insert(2, theirs.clone());
    rig.repository.unreadable_tag.set(Some(2));

    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(error.to_string().contains(UNREADABLE_TAG), "{error}");
    assert_nothing_moved(&rig, &journal, &[1]);
    assert_eq!(rig.repository.steles.borrow()[&2].identity, theirs.identity);
}

/// The refusal's recovery works as written, and keeps the batch.
#[test]
fn deleting_another_writers_immutable_tag_lets_the_batch_go_on() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let mut theirs = rig.repository.steles.borrow()[&1].clone();
    theirs.identity = stelae::Digest::compute(b"another writer");
    rig.repository.steles.borrow_mut().insert(2, theirs);
    rig.drain(limits(4), None).unwrap_err();

    rig.repository.steles.borrow_mut().remove(&2);
    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Put another writer's stele under `latest` at `sequence`, as a rewrite of its
/// tags would.
fn rewrite_latest(repository: &Repository, sequence: u64) -> Committed {
    let mut theirs = repository.inscription(sequence).unwrap();
    theirs.parameters = serde_json::json!({ "another-writer": true });
    let document = Document {
        canonical: theirs.canonicalize().unwrap(),
        identity: theirs.digest().unwrap(),
        layers: theirs.layers.len(),
        uncompressed_size: theirs.uncompressed_size(),
    };
    let committed = Repository::committed(sequence, &document);
    *repository.latest.borrow_mut() = Some(committed.clone());
    repository.steles.borrow_mut().insert(sequence, document);
    committed
}

/// Run a batch whose repository moves once, as `at` goes to publish: past every
/// check the flush makes itself, and before the read it chains from.
fn drain_moving_before_chaining(
    rig: &Rig<'_>,
    limits: Limits,
    at: u64,
    move_it: &dyn Fn(&Repository),
) -> Result<Outcome, backfill::Error> {
    let fired = Cell::new(false);
    let publisher = Around {
        inner: &rig.repository,
        finalize: &|finalizing, predecessor, source| {
            if finalizing.bundle.sequence() == at && !fired.replace(true) {
                move_it(&rig.repository);
            }
            backfill::Publish::finalize(&rig.repository, finalizing, predecessor, source)
        },
    };
    rig.drive(limits, None, true, &publisher)
}

fn moved_to(error: backfill::Error) -> Option<Committed> {
    match error {
        backfill::Error::Batch(batch::Error::PredecessorMoved { found, .. }) => found,
        other => panic!("expected a moved repository, got {other}"),
    }
}

/// Stele 2 would otherwise attest a digest nothing checked, and the next read
/// would match it.
#[test]
fn a_predecessor_rewritten_before_the_chain_read_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let theirs = RefCell::new(None);
    let error = drain_moving_before_chaining(&rig, limits(4), 2, &|repository| {
        *theirs.borrow_mut() = Some(rewrite_latest(repository, 1));
    })
    .unwrap_err();

    assert_eq!(moved_to(error), theirs.into_inner());
    assert!(rig.repository.inscription(2).is_none());
    assert_eq!(*rig.repository.uploaded.borrow(), [1]);
    assert_eq!(rig.sequences(), [2, 3, 4]);
}

/// A tag deleted under a batch begun over an empty repository would otherwise
/// seal stele 2 with no history at all, and under a followed one lose the
/// chain.
#[test]
fn a_tag_deleted_before_the_chain_read_refuses() {
    let chain = Chain::spanning(12);
    let delete = |repository: &Repository| *repository.latest.borrow_mut() = None;

    let first = Rig::new(&chain);
    let error = drain_moving_before_chaining(&first, limits(4), 2, &delete).unwrap_err();
    assert_eq!(moved_to(error), None);
    assert!(first.repository.inscription(2).is_none());

    let followed = Rig::new(&chain);
    followed.repository.keep_singles.set(true);
    followed.run(Limits::ONE, Some(1)).unwrap();
    let error = drain_moving_before_chaining(&followed, limits(3), 3, &delete).unwrap_err();
    assert_eq!(moved_to(error), None);
    assert!(followed.repository.inscription(3).is_none());
}

/// A moved chain read is a refusal and not a failure worth asking again, even
/// if the tag reads back as the journal says by the next attempt.
#[test]
fn a_moved_chain_read_is_refused_at_once() {
    let chain = Chain::spanning(12);
    let at = |sequence| Committed {
        sequence,
        identity: "sha256:another-writer".to_owned(),
    };
    for moved in [None, Some(at(1)), Some(at(3))] {
        let rig = Rig::new(&chain);
        let attempts = Cell::new(0);
        let publisher = Around {
            inner: &rig.repository,
            finalize: &|finalizing, predecessor, source| {
                let publish = |finalizing: &Finalizing<'_>, source: &dyn SnapshotSource| {
                    backfill::Publish::finalize(&rig.repository, finalizing, predecessor, source)
                };
                if finalizing.bundle.sequence() != 2 {
                    return publish(finalizing, source);
                }
                attempts.set(attempts.get() + 1);
                let held = rig.repository.latest.replace(moved.clone());
                let answer = publish(finalizing, source);
                *rig.repository.latest.borrow_mut() = held;
                answer
            },
        };

        let error = rig.drive(limits(4), None, true, &publisher).unwrap_err();
        assert_eq!(moved_to(error), moved);
        assert_eq!(attempts.get(), 1);
    }
}

/// A seal's `latest` answers with an error and the registry applies it late,
/// during the retry's reproduction. The chain read then finds this batch's own
/// stele at the sequence being published, which the next try reconciles. Takes
/// two backoffs.
#[test]
fn our_own_late_latest_before_the_chain_read_reconciles() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let attempts = Cell::new(0);
    let late = RefCell::new(None);
    let publisher = Around {
        inner: &rig.repository,
        finalize: &|finalizing, predecessor, source| {
            let publish =
                || backfill::Publish::finalize(&rig.repository, finalizing, predecessor, source);
            if finalizing.bundle.sequence() != 2 {
                return publish();
            }
            attempts.set(attempts.get() + 1);
            match attempts.get() {
                1 => {
                    let ours = publish()?;
                    *rig.repository.latest.borrow_mut() = predecessor.cloned();
                    *late.borrow_mut() = Some(ours);
                    let refused = stelae::Error::Registry(
                        oci_client::errors::OciDistributionError::ServerError {
                            code: 504,
                            url: "https://registry.invalid/v2/x/manifests/latest".to_owned(),
                            message: "gateway timeout".to_owned(),
                        },
                    );
                    Err(backfill::Error::Finalizing(Box::new(refused.into())))
                }
                2 => {
                    *rig.repository.latest.borrow_mut() = late.borrow_mut().take();
                    publish()
                }
                attempt => panic!("attempt {attempt}: the third read should have reconciled"),
            }
        },
    };

    let outcome = rig.drive(limits(4), None, true, &publisher);
    assert_eq!(outcome.unwrap(), Outcome::UpToDate);
    assert_eq!(attempts.get(), 2);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// Another writer's stele landing at the same place isn't this batch's, and the
/// next try's compare is what says so. Takes one backoff.
#[test]
fn another_writers_stele_at_the_chain_read_ends_as_diverged() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let theirs = Committed {
        sequence: 2,
        identity: "sha256:another-writer".to_owned(),
    };
    let error = drain_moving_before_chaining(&rig, limits(4), 2, &|repository| {
        *repository.latest.borrow_mut() = Some(theirs.clone());
    })
    .unwrap_err();

    assert!(
        matches!(
            &error,
            backfill::Error::Batch(batch::Error::Diverged { sequence: 2, found, .. })
                if *found == theirs.identity
        ),
        "{error}"
    );
    assert!(rig.repository.inscription(2).is_none());
    assert_eq!(*rig.repository.uploaded.borrow(), [1]);
}

/// Each sequence reads the repository before it does anything else, so a writer
/// moving it after a commit stops the next one at that read.
#[test]
fn a_repository_moved_between_two_sequences_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    let published = RefCell::new(Vec::new());
    let theirs = RefCell::new(None);
    let publisher = Around {
        inner: &rig.repository,
        finalize: &|finalizing, predecessor, source| {
            published.borrow_mut().push(finalizing.bundle.sequence());
            let committed =
                backfill::Publish::finalize(&rig.repository, finalizing, predecessor, source)?;
            *theirs.borrow_mut() = Some(rewrite_latest(&rig.repository, committed.sequence));
            Ok(committed)
        },
    };

    let error = rig.drive(limits(4), None, true, &publisher).unwrap_err();
    assert_eq!(moved_to(error), theirs.into_inner());
    assert_eq!(*published.borrow(), [1], "refused before it could chain");
    assert_eq!(rig.sequences(), [2, 3, 4]);
}

#[test]
fn a_repository_past_the_next_sequence_or_behind_the_journal_refuses() {
    for found in [
        Some(Committed {
            sequence: 3,
            identity: "sha256:another-writer".to_owned(),
        }),
        None,
    ] {
        let chain = Chain::spanning(12);
        let rig = Rig::new(&chain);
        stopped_after_the_first_commit(&rig);
        let journal = journal_bytes(&rig);

        *rig.repository.latest.borrow_mut() = found.clone();
        let error = rig.drain(limits(4), None).unwrap_err();
        assert!(
            matches!(
                error,
                backfill::Error::Batch(batch::Error::PredecessorMoved { .. })
            ),
            "{found:?}: {error}"
        );
        assert_nothing_moved(&rig, &journal, &[1]);
    }
}

/// The predecessor is held to the digest the journal recorded, not to its
/// number.
#[test]
fn a_predecessor_rewritten_under_its_tag_refuses() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    let rewritten = stelae::Digest::compute(b"another writer");
    rig.repository
        .steles
        .borrow_mut()
        .get_mut(&1)
        .unwrap()
        .identity = rewritten;
    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::PredecessorMoved { .. })
        ),
        "{error}"
    );
    assert_nothing_moved(&rig, &journal, &[1]);
}

/// A lost bundle is a precise stop, and the frontier stays what the repository
/// really holds. The recovery restores from there, not from where the batch
/// began.
#[test]
fn a_bundle_lost_part_way_through_stops_with_its_recovery() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    stopped_after_the_first_commit(&rig);
    let journal = journal_bytes(&rig);

    std::fs::remove_file(batch_dir(&rig).join("prepared/bundles/3.json")).unwrap();
    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::BundleLost { sequence: 3, .. })
        ),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("clear the storage path"), "{message}");
    assert!(message.contains("latest stele"), "{message}");
    assert_nothing_moved(&rig, &journal, &[1]);
}

/// Damage a bundle's load can't see surfaces as its payload is read, with no
/// stele and no silent use of what the stores hold now.
#[test]
fn a_damaged_payload_stops_at_its_sequence_with_its_recovery() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    let bundles = rig.batch().bundles().unwrap();
    let earlier: BTreeSet<_> = bundles[..2].iter().flat_map(|b| b.references()).collect();
    let only_third: Vec<_> = bundles[2]
        .references()
        .into_iter()
        .filter(|digest| !earlier.contains(digest))
        .collect();
    let blobs = batch_dir(&rig).join("prepared/blobs/sha256");
    let path = only_third
        .iter()
        .map(|digest| blobs.join(digest.to_hex()))
        .max_by_key(|path| std::fs::metadata(path).unwrap().len())
        .unwrap();
    flip_a_byte(&path);

    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(
        matches!(
            error,
            backfill::Error::Batch(batch::Error::BundleLost { sequence: 3, .. })
        ),
        "{error}"
    );
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2]);
    assert_eq!(latest_sequence(&rig), Some(2));
    assert_eq!(rig.sequences(), [3, 4]);
}

/// Stores that can't serve the bundles are no fault of the runs. Staging again
/// wouldn't mend them, so it stops with what recovers.
#[test]
fn stores_that_cant_serve_the_batch_stop_with_its_recovery() {
    let refusals: [fn() -> dolos_snapshot::Error; 2] = [
        || {
            prepare::Refusal::HorizonLost {
                sequence: 2,
                horizon: 1,
                first: 9,
            }
            .into()
        },
        || {
            dolos_snapshot::staging::Refusal::NotAtFrontier {
                frontier: "here".to_owned(),
            }
            .into()
        },
    ];
    for refusal in refusals {
        let chain = Chain::spanning(6);
        let rig = Rig::new(&chain);
        ready(rig.run(limits(2), None));
        let journal = journal_bytes(&rig);

        rig.calls.stage_refusal.set(Some(refusal));
        let error = rig.drain(limits(2), None).unwrap_err();
        assert!(
            matches!(
                error,
                backfill::Error::Batch(batch::Error::BundleLost { sequence: 1, .. })
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains("clear the storage path"),
            "{error}"
        );
        assert_nothing_moved(&rig, &journal, &[]);
    }
}

/// A full volume at a commit moves nothing, and a rerun with room goes on.
#[test]
fn a_volume_filling_during_a_publish_stops_recoverably() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    rig.repository.fault.set(Some((2, Fault::FullVolume)));

    let error = rig.drain(limits(4), None).unwrap_err();
    assert!(matches!(error, backfill::Error::FlushNoRoom(_)), "{error}");
    assert!(error.to_string().contains("make room"), "{error}");
    assert_eq!(latest_sequence(&rig), Some(1));
    assert_eq!(rig.sequences(), [2, 3, 4]);

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// A staging past the budget stops with what to raise, and leaves no partial
/// runs to charge the rerun with. The closed batch stays closed under any
/// controls.
#[test]
fn a_staging_past_the_budget_stops_until_it_is_raised() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let journal = journal_bytes(&rig);

    let tight = Limits {
        scratch: 1,
        ..limits(4)
    };
    let error = rig.drain(tight, None).unwrap_err();
    assert!(matches!(error, backfill::Error::FlushNoRoom(_)), "{error}");
    assert!(error.to_string().contains("--batch-scratch-mib"), "{error}");
    assert!(!batch_dir(&rig).join("staging").exists());
    assert_nothing_moved(&rig, &journal, &[]);
    assert_eq!(rig.calls.stage_attempts.get(), 1, "started from nothing");

    assert_eq!(rig.restart_and_drain(limits(4)).unwrap(), Outcome::UpToDate);
    assert_eq!(rig.repository.tags(), uninterrupted(&chain));
}

/// A capture's overshoot closes the batch past its budget, and the reserve kept
/// for finishing it is what staging then has.
#[test]
fn a_batch_a_capture_overshot_stages_within_its_reserve() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    let overshot = Limits {
        scratch: 1,
        reserve_per_epoch: 64 * 1024 * 1024,
        ..limits(4)
    };
    assert_eq!(
        rig.drain(overshot, Some(4)).unwrap(),
        Outcome::UntilEpoch { sequence: 4 }
    );
    assert_eq!(*rig.repository.uploaded.borrow(), [1, 2, 3, 4]);
}

/// What a restarted staging reuses counts against its budget, so a family it
/// walks again can't take the batch past the limit. One byte short of what the
/// runs take, walking `indexes` alone would fit if the others went uncharged.
#[test]
fn a_restarted_staging_is_charged_for_the_runs_it_holds() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let bytes = stage_under(&rig, limits(4)).unwrap();
    assert!(tree_bytes(&batch_dir(&rig).join("staging/indexes")) < bytes - 1);

    std::fs::remove_file(batch_dir(&rig).join("staging/indexes.json")).unwrap();
    let one_short = Limits {
        scratch: tree_bytes(&batch_dir(&rig).join("prepared")) + bytes - 1,
        ..limits(4)
    };
    let error = stage_under(&rig, one_short).unwrap_err();
    assert!(
        matches!(
            &error,
            batch::Error::Staging { source, .. }
                if matches!(
                    **source,
                    dolos_snapshot::Error::Staging(
                        dolos_snapshot::staging::Refusal::ScratchExceeded { .. }
                    )
                )
        ),
        "{error}"
    );
    assert!(!batch_dir(&rig).join("staging").exists());
}

/// Runs a kill left are charged until their family is walked again, so a batch
/// that fits starts over rather than stopping on them and asking for more
/// budget.
#[test]
fn a_staging_that_fits_isnt_stopped_by_what_a_kill_left() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));
    let bytes = stage_under(&rig, limits(4)).unwrap();

    std::fs::remove_file(batch_dir(&rig).join("staging/indexes.json")).unwrap();
    let same = Limits {
        scratch: tree_bytes(&batch_dir(&rig)),
        ..limits(4)
    };
    assert_eq!(stage_under(&rig, same).unwrap(), bytes);
}

/// A batch's pending refusal names flags that really continue it, and they do.
#[test]
fn the_pending_refusal_names_how_to_continue() {
    let chain = Chain::spanning(12);
    let rig = Rig::new(&chain);
    ready(rig.run(limits(4), None));

    let error = rig.run(Limits::ONE, None).unwrap_err();
    assert!(
        error.to_string().contains("--batch-epochs above 1"),
        "{error}"
    );
    assert_eq!(rig.drain(limits(2), None).unwrap(), Outcome::UpToDate);
}

/// The real transport: `cargo test -p stelae-cardano --all-features --test
/// batch -- --ignored --test-threads=1`. `STELAE_TEST_REGISTRY_IMAGE` picks the
/// server.
mod over_a_registry {
    use dolos_snapshot::registry::{
        Auth, Inspected, Point, Repository, SnapshotRepository, Tuning,
    };
    use stelae::transport::WrittenLayer;
    use stelae_cardano::publisher::{BackfillPublisher, Publisher, RepositoryPublish};
    use stelae_driver::publish::{Chained, Origin, PublishRecord};

    use super::*;

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
                .expect("docker is required for the registry lane");
            let stderr = String::from_utf8_lossy(&run.stderr);
            assert!(run.status.success(), "docker run {image}: {stderr}");
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
                        let request = "GET /v2/ HTTP/1.0\r\nHost: localhost\r\n\r\n";
                        stream.write_all(request.as_bytes())?;
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

        fn repository(&self, name: &str) -> Repository {
            format!("oci://127.0.0.1:{}/cardano/{name}", self.port)
                .parse()
                .unwrap()
        }

        /// One request, and the status and body it got.
        fn request(&self, method: &str, path: &str, media: &str, body: &[u8]) -> (u16, Vec<u8>) {
            use std::io::{Read as _, Write as _};

            let mut stream = std::net::TcpStream::connect(("127.0.0.1", self.port)).unwrap();
            let head = format!(
                "{method} {path} HTTP/1.0\r\nHost: localhost\r\nAccept: {media}\r\nContent-Type: \
                 {media}\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
            let mut answer = Vec::new();
            stream.read_to_end(&mut answer).unwrap();
            let status = std::str::from_utf8(&answer[9..12])
                .unwrap()
                .parse()
                .unwrap();
            let start = answer
                .windows(4)
                .position(|end| end == b"\r\n\r\n")
                .unwrap();
            (status, answer[start + 4..].to_vec())
        }

        /// Point `to`'s `latest` at what `from`'s names, as another writer's
        /// push would: its blobs mounted, then its manifest. Nothing
        /// else in the transport writes a stele under a number the
        /// repository already holds.
        fn move_latest(&self, from: &str, to: &str) {
            const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

            let path = format!("/v2/cardano/{from}/manifests/latest");
            let (status, manifest) = self.request("GET", &path, MANIFEST, &[]);
            assert_eq!(status, 200);
            let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
            let layers = parsed["layers"].as_array().unwrap();
            for blob in layers.iter().chain([&parsed["config"]]) {
                let digest = blob["digest"].as_str().unwrap();
                let path =
                    format!("/v2/cardano/{to}/blobs/uploads/?mount={digest}&from=cardano/{from}");
                let (status, _) = self.request("POST", &path, MANIFEST, &[]);
                assert_eq!(status, 201, "{digest}");
            }
            let path = format!("/v2/cardano/{to}/manifests/latest");
            let (status, _) = self.request("PUT", &path, MANIFEST, &manifest);
            assert_eq!(status, 201);
        }
    }

    impl Drop for LocalRegistry {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.container])
                .output();
        }
    }

    fn settings(repo: &Repository) -> RepositoryPublish<'_> {
        RepositoryPublish {
            repo,
            auth: Auth::Anonymous,
            insecure: true,
            scratch_dir: None,
            rebuild: false,
            tuning: Tuning::default(),
        }
    }

    fn publish_through(
        rig: &Rig<'_>,
        repo: &Repository,
        limits: Limits,
        until: u64,
    ) -> Result<Outcome, backfill::Error> {
        let observer = Observer::silent();
        let publisher = BackfillPublisher {
            config: &rig.node.config,
            settings: settings(repo),
            observer: &observer,
        };
        rig.drive(limits, Some(until), true, &publisher)
    }

    fn inspect(repo: &Repository, point: Point) -> Inspected {
        let scratch = tempfile::tempdir().unwrap();
        SnapshotRepository::open(
            repo,
            true,
            Auth::Anonymous,
            scratch.path().to_owned(),
            Tuning::default(),
        )
        .unwrap()
        .inspect(point)
        .unwrap()
    }

    /// The whole chain one boundary at a time, to hold a batch against.
    fn one_at_a_time(chain: &Chain, repo: &Repository, until: u64) {
        one_at_a_time_on(&Rig::new(chain), repo, until);
    }

    fn one_at_a_time_on(rig: &Rig<'_>, repo: &Repository, until: u64) {
        assert_eq!(
            publish_through(rig, repo, Limits::ONE, until).unwrap(),
            Outcome::UntilEpoch { sequence: until }
        );
    }

    fn assert_same_steles(found: &Repository, expected: &Repository, through: u64) {
        for sequence in 1..=through {
            let (found, expected) = (
                inspect(found, Point::Epoch(sequence)),
                inspect(expected, Point::Epoch(sequence)),
            );
            assert_eq!(found.identity, expected.identity, "at sequence {sequence}");
            assert_eq!(
                found.inscription, expected.inscription,
                "at sequence {sequence}"
            );
            assert_eq!(
                found.total_compressed, expected.total_compressed,
                "at sequence {sequence}"
            );
        }
        let latest = inspect(found, Point::Latest);
        assert_eq!(latest.inscription.sequence, through);
    }

    /// A repository a batch filled holds what one filled a boundary at a time
    /// holds: the same steles, the same blobs and a `latest` standing on the
    /// last.
    #[test]
    #[ignore = "spawns a registry"]
    fn a_batch_publishes_what_one_boundary_at_a_time_publishes() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let (single, batched) = (registry.repository("one"), registry.repository("batch"));
        one_at_a_time(&chain, &single, 4);

        let rig = Rig::new(&chain);
        assert_eq!(
            publish_through(&rig, &batched, limits(2), 4).unwrap(),
            Outcome::UntilEpoch { sequence: 4 }
        );
        assert_batch_is_gone(&rig);
        assert_same_steles(&batched, &single, 4);

        let scratch = tempfile::tempdir().unwrap();
        SnapshotRepository::open(
            &batched,
            true,
            Auth::Anonymous,
            scratch.path().to_owned(),
            Tuning::default(),
        )
        .unwrap()
        .verify(Point::Latest)
        .unwrap();
    }

    /// Publishes through a real repository, and loses the answer once.
    struct LosesAnAnswer<'a> {
        inner: BackfillPublisher<'a>,
        at: u64,
        cancel: CancellationToken,
        uploads: RefCell<Vec<u64>>,
    }

    impl backfill::Publish for LosesAnAnswer<'_> {
        fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), backfill::Error> {
            self.inner.publish(plan, source)
        }

        fn latest(&self, plan: &Plan) -> Result<Option<Committed>, backfill::Error> {
            self.inner.latest(plan)
        }

        fn stele(&self, sequence: u64) -> Result<Option<Held>, backfill::Error> {
            self.inner.stele(sequence)
        }

        fn finalize(
            &self,
            finalizing: &Finalizing<'_>,
            predecessor: Option<&Committed>,
            source: &dyn SnapshotSource,
        ) -> Result<Committed, backfill::Error> {
            let committed = self.inner.finalize(finalizing, predecessor, source)?;
            self.uploads.borrow_mut().push(committed.sequence);
            if committed.sequence == self.at && !self.cancel.is_cancelled() {
                self.cancel.cancel();
                return Err(backfill::Error::caller(io::Error::other(
                    "the process died",
                )));
            }
            Ok(committed)
        }
    }

    #[test]
    #[ignore = "spawns a registry"]
    fn a_lost_answer_reconciles_against_the_repository() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let (single, batched) = (registry.repository("one"), registry.repository("batch"));
        one_at_a_time(&chain, &single, 4);

        let rig = Rig::new(&chain);
        let observer = Observer::silent();
        let publisher = LosesAnAnswer {
            inner: BackfillPublisher {
                config: &rig.node.config,
                settings: settings(&batched),
                observer: &observer,
            },
            at: 2,
            cancel: rig.calls.cancel.get(),
            uploads: RefCell::default(),
        };
        rig.drive(limits(4), Some(4), true, &publisher).unwrap_err();
        assert_eq!(rig.sequences(), [2, 3, 4], "the journal hasn't heard");
        assert_eq!(inspect(&batched, Point::Latest).inscription.sequence, 2);

        rig.calls.cancel.restart();
        assert_eq!(
            rig.drive(limits(4), Some(4), true, &publisher).unwrap(),
            Outcome::UntilEpoch { sequence: 4 }
        );
        assert_eq!(*publisher.uploads.borrow(), [1, 2, 3, 4]);
        assert_same_steles(&batched, &single, 4);
    }

    /// Another writer's stele is told by its identity, and it stays.
    #[test]
    #[ignore = "spawns a registry"]
    fn another_writers_stele_refuses_and_stays() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let repo = registry.repository("shared");

        let rig = Rig::new(&chain);
        ready(rig.run(limits(2), None));

        // The same chain under another retained list, which its identity states.
        let theirs = Rig::new(&chain);
        *theirs.retained.borrow_mut() = dolos_snapshot::planning::retained_epochs(&[3]).unwrap();
        one_at_a_time_on(&theirs, &repo, 1);
        let before = inspect(&repo, Point::Epoch(1)).identity;

        let error = publish_through(&rig, &repo, limits(2), 2).unwrap_err();
        assert!(
            matches!(
                error,
                backfill::Error::Batch(batch::Error::Diverged { sequence: 1, .. })
            ),
            "{error}"
        );
        assert_eq!(inspect(&repo, Point::Epoch(1)).identity, before);
        assert_eq!(inspect(&repo, Point::Latest).identity, before);
        let batch = rig.batch();
        assert_eq!(batch.frontiers().published, None);
        assert_eq!(rig.sequences(), [1, 2]);
    }

    /// The chain read itself, on the real transport: `latest` moves past every
    /// check the flush makes, and nothing is sealed on it. Sequence 1 reads
    /// another writer's stele at the sequence it publishes, which the next
    /// try's compare refuses, and sequence 2 reads another stele under the
    /// number it follows, which refuses at once. In both, the chain read's own
    /// compare gives the first answer, because at sequence 1 the retry alone
    /// would end the same way.
    #[test]
    #[ignore = "spawns a registry"]
    fn a_latest_moved_before_the_chain_read_refuses_and_seals_nothing() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let theirs = registry.repository("theirs");
        let other = Rig::new(&chain);
        *other.retained.borrow_mut() = dolos_snapshot::planning::retained_epochs(&[3]).unwrap();
        one_at_a_time_on(&other, &theirs, 1);
        let their_identity = inspect(&theirs, Point::Latest).identity;

        for at in [1, 2] {
            let name = format!("moved-at-{at}");
            let repo = registry.repository(&name);
            let rig = Rig::new(&chain);
            let observer = Observer::silent();
            let inner = BackfillPublisher {
                config: &rig.node.config,
                settings: settings(&repo),
                observer: &observer,
            };
            let fired = Cell::new(false);
            let publisher = Around {
                inner: &inner,
                finalize: &|finalizing, predecessor, source| {
                    let moved = finalizing.bundle.sequence() == at && !fired.replace(true);
                    if moved {
                        registry.move_latest("theirs", &name);
                    }
                    let answer =
                        backfill::Publish::finalize(&inner, finalizing, predecessor, source);
                    if moved {
                        let compared = matches!(
                            &answer,
                            Err(backfill::Error::Finalizing(error)) if matches!(
                                &**error,
                                dolos_snapshot::Error::Stelae(stelae::Error::PredecessorMoved {
                                    expected,
                                    found: Some((1, found)),
                                }) if expected.is_some() == (at == 2)
                                    && *found == their_identity.to_string()
                            )
                        );
                        let told = answer.as_ref().err().map(ToString::to_string);
                        assert!(compared, "at {at}: the chain read answered {told:?}");
                    }
                    answer
                },
            };

            let error = rig.drive(limits(4), Some(4), true, &publisher).unwrap_err();
            let expected = Committed {
                sequence: 1,
                identity: their_identity.to_string(),
            };
            match (at, &error) {
                (
                    1,
                    backfill::Error::Batch(batch::Error::Diverged {
                        sequence: 1, found, ..
                    }),
                ) => assert_eq!(*found, expected.identity),
                (2, backfill::Error::Batch(batch::Error::PredecessorMoved { found, .. })) => {
                    assert_eq!(found.as_ref(), Some(&expected))
                }
                _ => panic!("at {at}: {error}"),
            }
            assert_eq!(inspect(&repo, Point::Latest).identity, their_identity);
            assert_eq!(rig.sequences().first(), Some(&at), "the journal stands");
            assert!(
                held_at(&repo, at).is_none(),
                "at {at}: another stele was sealed on what it read"
            );
        }
    }

    /// A seal of this batch's own applied late, on the real transport: the
    /// chain read finds it at the sequence being published, and the next
    /// try reconciles it without another upload. Another publisher over the
    /// same chain stands in for the late write, since it seals the same
    /// stele. Takes one backoff.
    #[test]
    #[ignore = "spawns a registry"]
    fn our_own_late_seal_found_by_the_chain_read_reconciles() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let (single, batched) = (registry.repository("one"), registry.repository("batch"));
        one_at_a_time(&chain, &single, 2);

        let rig = Rig::new(&chain);
        let late = Rig::new(&chain);
        let observer = Observer::silent();
        let inner = BackfillPublisher {
            config: &rig.node.config,
            settings: settings(&batched),
            observer: &observer,
        };
        let attempts = Cell::new(0);
        let publisher = Around {
            inner: &inner,
            finalize: &|finalizing, predecessor, source| {
                if finalizing.bundle.sequence() == 1 {
                    attempts.set(attempts.get() + 1);
                    one_at_a_time_on(&late, &batched, 1);
                }
                backfill::Publish::finalize(&inner, finalizing, predecessor, source)
            },
        };

        assert_eq!(
            rig.drive(limits(2), Some(2), true, &publisher).unwrap(),
            Outcome::UntilEpoch { sequence: 2 }
        );
        assert_eq!(attempts.get(), 1, "reconciled, not published again");
        assert_same_steles(&batched, &single, 2);
    }

    /// What the immutable tag holds, reading no further than a publish does.
    fn held_at(repo: &Repository, sequence: u64) -> Option<Held> {
        let scratch = tempfile::tempdir().unwrap();
        Publisher::open_explicit(
            repo,
            true,
            Auth::Anonymous,
            scratch.path().join("scratch"),
            scratch.path().join("record"),
            false,
            Tuning::default(),
        )
        .unwrap()
        .stele(sequence)
        .unwrap()
    }

    /// `adopt` keeps a layer that is already there from being built and
    /// uploaded again. Dropping it still produces the same bytes, so only
    /// the count tells.
    #[test]
    #[ignore = "spawns a registry"]
    fn a_stele_that_inherits_carries_its_layers_forward() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let repo = registry.repository("reuse");
        let rig = Rig::new(&chain);
        let observer = Observer::silent();
        let inner = BackfillPublisher {
            config: &rig.node.config,
            settings: settings(&repo),
            observer: &observer,
        };
        let reused = RefCell::new(BTreeMap::new());
        let publisher = Around {
            inner: &inner,
            finalize: &|finalizing, predecessor, source| {
                let published = Publisher::open(&rig.node.config, &settings(&repo))
                    .and_then(|publisher| {
                        publisher.finalize(finalizing, predecessor, source, &observer)
                    })
                    .map_err(|error| backfill::Error::Finalizing(Box::new(error)))?;
                let sequence = published.inscription.sequence;
                reused
                    .borrow_mut()
                    .insert(sequence, published.layers_reused);
                Ok(Committed {
                    sequence,
                    identity: published.identity.to_string(),
                })
            },
        };

        rig.drive(limits(4), Some(4), true, &publisher).unwrap();
        let reused = reused.into_inner();
        assert_eq!(reused[&1], 0, "{reused:?}");
        for sequence in 2..=4 {
            assert!(reused[&sequence] > 0, "{reused:?}");
        }
    }

    /// A port nothing listens on.
    fn dead_registry() -> Repository {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("oci://127.0.0.1:{port}/cardano/dead")
            .parse()
            .unwrap()
    }

    /// Read as no tag, a registry that didn't answer would let a seal push over
    /// whatever that tag holds. Needs no server.
    #[test]
    fn a_tag_the_registry_cant_serve_isnt_read_as_absent() {
        let dead = dead_registry();
        let scratch = tempfile::tempdir().unwrap();
        let open = |repo: &Repository| {
            Publisher::open_explicit(
                repo,
                true,
                Auth::Anonymous,
                scratch.path().join("scratch"),
                scratch.path().join("record"),
                false,
                Tuning::default(),
            )
            .unwrap()
        };

        let empty: Repository = empty_registry().parse().unwrap();
        assert!(open(&empty).stele(2).unwrap().is_none());
        assert!(open(&dead).stele(2).is_err());
    }

    /// A real publication's failure is told apart from a damaged bundle by its
    /// shape, so the real publisher has to keep it. Needs no server: the
    /// repository the flush reads is the fake, and only the publication goes
    /// out. Cancelling spares the backoff.
    #[test]
    fn a_dead_registry_surfaces_as_a_publication_failure() {
        let chain = Chain::spanning(12);
        let dead = dead_registry();
        let rig = Rig::new(&chain);
        let observer = Observer::silent();
        let real = BackfillPublisher {
            config: &rig.node.config,
            settings: settings(&dead),
            observer: &observer,
        };
        let publisher = Around {
            inner: &rig.repository,
            finalize: &|finalizing, predecessor, source| {
                let answer = backfill::Publish::finalize(&real, finalizing, predecessor, source);
                rig.calls.cancel.get().cancel();
                answer
            },
        };

        let error = rig.drive(limits(4), None, true, &publisher).unwrap_err();
        assert!(matches!(error, backfill::Error::Finalizing(_)), "{error}");
        assert_eq!(rig.sequences(), [1, 2, 3, 4]);
    }

    /// The driver's half of a deleted tag, which a real registry here can't do:
    /// nothing under `latest` refuses a publish held to a stele, and only that.
    #[test]
    fn a_publish_held_to_a_stele_refuses_a_repository_holding_nothing() {
        let dead = dead_registry();
        let scratch = tempfile::tempdir().unwrap();
        let registry = stelae_driver::publish::open(
            &dead,
            true,
            Auth::Anonymous,
            scratch.path().to_owned(),
            Tuning::default(),
        )
        .unwrap();
        let origin = Origin {
            repository: dead.to_string(),
            dataset_id: MAGIC.into(),
            parameters: dolos_snapshot::parameters(&retained()),
            compression: dolos_snapshot::compression(),
        };
        let chained = |publishing| {
            Chained::new(
                &dolos_snapshot::DolosProfile,
                publishing,
                None,
                2,
                &serde_json::json!({}),
                origin.clone(),
            )
            .err()
        };

        let held = (1, stelae::Digest::compute(b"the stele it follows"));
        let refused = chained(Publishing::new(&registry).following(Some(held)));
        assert!(
            matches!(
                refused,
                Some(stelae_driver::Error::Stelae(
                    stelae::Error::PredecessorMoved {
                        expected: Some(_),
                        found: None,
                    }
                ))
            ),
            "{refused:?}"
        );
        assert!(chained(Publishing::new(&registry)).is_none());
        assert!(chained(Publishing::new(&registry).following(None)).is_none());
    }

    /// A publish record a batch of one left resumes under a batch: a recorded
    /// layer whose blob isn't there is rebuilt, the same stele results, and
    /// the record goes with its seal.
    #[test]
    #[ignore = "spawns a registry"]
    fn a_record_a_single_publish_left_resumes_under_a_batch() {
        let chain = Chain::spanning(12);
        let registry = LocalRegistry::spawn();
        let (single, batched) = (registry.repository("one"), registry.repository("batch"));
        one_at_a_time(&chain, &single, 3);

        let rig = Rig::new(&chain);
        publish_through(&rig, &batched, Limits::ONE, 1).unwrap();

        let storage = &rig.node.config.storage.path;
        let record = dolos_snapshot::registry::record_path_in(storage);
        let closed = inspect(&single, Point::Epoch(2))
            .inscription
            .layers
            .into_iter()
            .find(|layer| layer.kind == "blocks" && layer.scope["epoch"] == 1)
            .unwrap();
        PublishRecord {
            origin: Origin {
                repository: batched.to_string(),
                dataset_id: MAGIC.into(),
                parameters: dolos_snapshot::parameters(&retained()),
                compression: dolos_snapshot::compression(),
            },
            layers: vec![WrittenLayer {
                digests: stelae::digest::LayerDigests {
                    diff_id: closed.diff_id,
                    blob_digest: stelae::Digest::compute(b"a blob nothing uploaded"),
                    compressed_size: 1,
                    uncompressed_size: closed.uncompressed_size,
                },
                descriptor: closed,
            }],
        }
        .save(&record)
        .unwrap();

        assert_eq!(
            publish_through(&rig, &batched, limits(2), 3).unwrap(),
            Outcome::UntilEpoch { sequence: 3 }
        );
        assert!(!record.exists());
        assert_same_steles(&batched, &single, 3);
    }
}
