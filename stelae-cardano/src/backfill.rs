//! Stelae-owned Cardano backfill state machine.
//!
//! A batch of one publishes the committed boundary before it advances and
//! prunes only after that publish succeeds. A larger batch prepares each
//! boundary durably instead and replays on until [`crate::batch`] closes it, so
//! the cursor may pass the published sequence only over journaled work. Then it
//! stages once and publishes every prepared sequence in order before replay
//! moves again. Both download bounded Mithril windows and close the replay
//! engine on every result.
//!
//! Ordering at a prepared boundary: capture while the importing session still
//! holds the block's index delta, finish that session so the stores are
//! durable, journal the bundle, and only then remove consumed immutable files
//! or start replaying. Nothing pending reads an immutable file: bundles and the
//! archive hold it all, and a replay after a lost bundle downloads them again,
//! once a window left from the lost replay is out of the way.
//!
//! Ordering at a commit: read what the repository really holds, hold it to the
//! journal, publish, and journal the snapshot before its bundle goes. A
//! snapshot the repository already holds at the next sequence is an upload
//! whose acknowledgement was lost, or another writer's, and only its identity
//! tells them apart: it is reproduced and compared, and a difference refuses.
//!
//! Every Dolos snapshot names the blocks snapshot of its own sequence, which
//! the host publishes into the blocks repository first ([`crate::blocks`]): at
//! a batch's capture, or before a batch of one publishes, and in either case
//! before the closed epoch's immutable files are removed. A batch of one
//! therefore removes them only once the snapshot is published. A restart that
//! needs a blocks snapshot after its files are gone downloads them again beside
//! the window.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::{
    config::{RootConfig, StorageConfig, StoreType},
    ArchiveIndexDelta, BlockSlot, ChainPoint, Genesis, RawBlock, ReplayProgress,
};
use dolos_mithril as mithril;
use dolos_mithril::MithrilSource;
use dolos_snapshot::{
    export::{Following, Plan, Predecessor},
    node,
    prepare::{self, Bundle, Finalizing, IndexTail},
    registry::Auth,
    source::SnapshotSource,
    staging::Staged,
};
use itertools::Itertools as _;
use stelae::{inscription::Inscription, progress::Observer, Digest};
use stelae_blocks::EpochScope;
use stelae_driver::{First, Standing};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    batch::{
        self, Admission, Batch, Committed, Flush, Frontiers, Held, Limits, Position, START_OVER,
    },
    blocks::{self, EpochFiles},
    publisher::{BackfillPublisher, PolicyError, RepositoryPublish},
};

const IMPORT_CHUNK: usize = 100;
const IMMUTABLE_FILE_MARGIN: u64 = 2;
const SLOTS_PER_SECURITY_PARAM: u64 = 10;
const FALLBACK_SLOTS_PER_IMMUTABLE_FILE: u64 = 21_600;
/// Under the download directory: an epoch's immutable files downloaded again
/// for its blocks snapshot, apart from the replay's window.
const REFETCH_DIR: &str = "blocks-refetch";
/// In the refetch directory once its download finished: the files it holds.
const REFETCHED_FILE: &str = "downloaded";

const IF_THE_STATE_PASSED: &str =
    "If the state passed the archive on the way, the rerun refuses and says how to recover";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Caller(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("planning the publish")]
    Planning(#[source] dolos_snapshot::Error),
    #[error("replay failed ({replay}) and shutting down also failed ({shutdown})")]
    ReplayAndShutdown {
        replay: Box<Error>,
        shutdown: Box<Error>,
    },
    /// A source failure stopped the replay and publishing what the batch
    /// prepared failed too. A download that filled the volume does both, and
    /// each needs its own fix.
    #[error(
        "reading the source failed ({failure}), and publishing what the batch prepared then \
         failed ({flush}). Deal with both before the rerun"
    )]
    SourceAndFlush {
        failure: Box<Error>,
        flush: Box<Error>,
    },
    #[error("iterating the local immutable db: {0}")]
    ImmutableDb(String),
    #[error(
        "the immutable directory {dir} holds {contents}, and not where the stores stand ({at}). A \
         window left from another replay can't continue this one: empty it and rerun, and what \
         this replay needs downloads again"
    )]
    StaleWindow {
        dir: PathBuf,
        contents: String,
        at: String,
    },
    #[error("reading block data: {0}")]
    BlockData(String),
    #[error("{what}: {reason}")]
    Mithril { what: &'static str, reason: String },
    #[error("removing the consumed immutable file {name}")]
    RemoveConsumed {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "the state store's cursor at slot {slot} has no block hash: a stop between an epoch's \
         opening and its first block leaves this, and nothing anchors it again. {START_OVER}"
    )]
    UnanchoredCursor { slot: BlockSlot },
    #[error(
        "the fetched immutable window {start:05}..={end:05} did not advance the downloaded files \
         (highest was {highest:?}, still {after:?}); mithril returned nothing new for the state \
         cursor at slot {cursor_slot} — the immutable dir holds {contents}"
    )]
    StalledWindow {
        start: u64,
        end: u64,
        highest: Option<u64>,
        after: Option<u64>,
        cursor_slot: BlockSlot,
        contents: String,
    },
    #[error("the download window must be at least 1 immutable file")]
    EmptyWindow,
    #[error("backfill::run must be called outside an entered Tokio runtime")]
    AsyncRuntimeContext,
    #[error(
        "interrupted by a shutdown signal; the stores are consistent and a rerun resumes here"
    )]
    Interrupted,
    #[error(transparent)]
    Batch(#[from] batch::Error),
    #[error(
        "a batch can't hold one boundary: it holds {used} bytes and needs {needed} more within \
         its scratch budget and on the volume. Nothing is lost: make room or raise the budget \
         and rerun"
    )]
    NoRoom { needed: u64, used: u64 },
    #[error("the stores ran out of room: {0}. Make room and rerun. {IF_THE_STATE_PASSED}")]
    ReplayFilledVolume(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(
        "a store stopped taking writes: {0}. A write or a background worker failed, which a full \
         volume or a failing disk causes, and this log holds its error, such as `Worker #N \
         crashed` or `persist failed`. Check both, then rerun. {IF_THE_STATE_PASSED}"
    )]
    StorePoisoned(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(
        "the state store stands at slot {cursor} and {}: the state passed blocks the archive \
         never got, and replaying on would leave them out of every snapshot. {START_OVER}",
        archive_end(.tip)
    )]
    ArchiveBehind {
        cursor: BlockSlot,
        tip: Option<BlockSlot>,
    },
    #[error("publishing a prepared boundary: {0}")]
    Finalizing(#[source] Box<dolos_snapshot::Error>),
    #[error(
        "publishing the batch ran out of room: {0}. All it holds stands: make room, or raise \
         --batch-scratch-mib where that budget is what ran out, and rerun"
    )]
    FlushNoRoom(#[source] Box<dolos_snapshot::Error>),
    #[error(transparent)]
    Blocks(#[from] blocks::Error),
}

fn archive_end(tip: &Option<BlockSlot>) -> String {
    match tip {
        Some(slot) => format!("the archive's last block is at slot {slot}"),
        None => "the archive holds no block".to_owned(),
    }
}

impl Error {
    pub fn caller(source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        Self::Caller(source.into())
    }

    /// Name the stop when a store failed on a full volume or a failed worker.
    /// `Poisoned` also covers a real fsync failure, so it isn't read as full.
    fn in_replay(self) -> Self {
        match self {
            Self::Caller(source) if batch::reports_full_volume(&*source) => {
                Self::ReplayFilledVolume(source)
            }
            Self::Caller(source) if batch::reports_poisoned(&*source) => {
                Self::StorePoisoned(source)
            }
            other => other,
        }
    }

    fn fills_stores(&self) -> bool {
        matches!(self, Self::ReplayFilledVolume(_) | Self::StorePoisoned(_))
    }

    /// What publishing a sequence can't get past by trying again.
    fn is_refusal(&self) -> bool {
        match self {
            Self::Batch(_) | Self::FlushNoRoom(_) => true,
            Self::Blocks(error) => error.is_refusal(),
            _ => false,
        }
    }

    /// The source stopped answering past its retries, or a fetch brought
    /// nothing new. A rerun may find it back.
    fn is_source_failure(&self) -> bool {
        matches!(self, Self::Mithril { .. } | Self::StalledWindow { .. })
    }
}

/// Refuse stores whose state has passed the archive. Dolos commits a block's
/// state first and an import keeps no WAL to catch the archive up from, so
/// replaying on would close the difference and leave those blocks out of every
/// snapshot. Only a block's commit moves a hashed cursor, and an archive ahead
/// of it heals as the same blocks import again, up to the next boundary.
///
/// This can't see a gap later imports already closed.
pub fn refuse_archive_behind(config: &RootConfig, genesis: Arc<Genesis>) -> Result<(), Error> {
    use dolos_core::{ArchiveStore as _, StateStore as _};

    // What the engine refuses comes first, and this creates what it would.
    ReplayWorkspace::open(config, genesis)
        .map_err(Error::caller)?
        .finish()
        .map_err(Error::caller)?;
    if !config.keeps_archive() || config.storage.archive.kind == StoreType::Ephemeral {
        return Ok(());
    }

    let state = dolos::storage::open_state_store(config).map_err(Error::caller)?;
    let cursor = state.read_cursor().map_err(Error::caller);
    let closed = state.shutdown().map_err(Error::caller);
    drop(state);
    closed?;
    // An unanchored cursor has its own refusal, and Origin has no block.
    let Some(ChainPoint::Specific(cursor, _)) = cursor? else {
        return Ok(());
    };

    let archive = dolos::storage::open_archive_store(config).map_err(Error::caller)?;
    let tip = archive.get_tip().map_err(Error::caller);
    let closed = archive.shutdown().map_err(Error::caller);
    drop(archive);
    closed?;
    match tip?.map(|(slot, _)| slot) {
        Some(tip) if tip >= cursor => Ok(()),
        tip => Err(Error::ArchiveBehind { cursor, tip }),
    }
}

pub trait Workspace {
    type Session: Session;
    fn snapshot(&self) -> impl SnapshotSource + '_;
    fn start(self, target_epoch: u64) -> Result<Self::Session, Error>;
    fn finish(self) -> Result<(), Error>;
}

pub trait Session {
    fn snapshot(&self) -> impl SnapshotSource + '_;
    fn boundary_index_delta(&self) -> Option<&ArchiveIndexDelta>;
    fn committed_position(&self) -> Result<Option<ChainPoint>, Error>;
    fn import_blocks(&mut self, blocks: Vec<RawBlock>) -> Result<ReplayProgress, Error>;
    fn prune_history(&mut self) -> Result<u64, Error>;
    fn finish(self) -> Result<(), Error>;
}

impl Workspace for ReplayWorkspace<'_> {
    type Session = BulkReplaySession;

    fn snapshot(&self) -> impl SnapshotSource + '_ {
        self.snapshot()
    }

    fn start(self, target_epoch: u64) -> Result<Self::Session, Error> {
        self.start(Some(target_epoch)).map_err(Error::caller)
    }

    fn finish(self) -> Result<(), Error> {
        self.finish().map_err(Error::caller)
    }
}

impl Session for BulkReplaySession {
    fn snapshot(&self) -> impl SnapshotSource + '_ {
        self.snapshot()
    }

    fn boundary_index_delta(&self) -> Option<&ArchiveIndexDelta> {
        self.boundary_index_delta()
    }

    fn committed_position(&self) -> Result<Option<ChainPoint>, Error> {
        self.committed_position().map_err(Error::caller)
    }

    fn import_blocks(&mut self, blocks: Vec<RawBlock>) -> Result<ReplayProgress, Error> {
        self.import_blocks(blocks).map_err(Error::caller)
    }

    fn prune_history(&mut self) -> Result<u64, Error> {
        self.prune_history().map_err(Error::caller)
    }

    fn finish(self) -> Result<(), Error> {
        self.finish().map_err(Error::caller)
    }
}

pub trait Replay {
    fn round_started(&self) {}
    fn reached(&self, slot: BlockSlot) {
        let _ = slot;
    }
    fn round_finished(&self) {}
    fn frontiers(&self, frontiers: &Frontiers) {
        let _ = frontiers;
    }
}

impl Replay for () {}

pub trait Publish {
    fn announce(&self, plan: &Plan) -> Result<(), Error> {
        let _ = plan;
        Ok(())
    }
    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), Error>;
    /// The repository's latest snapshot as `plan` finds it: what a batch
    /// follows.
    fn latest(&self, plan: &Plan) -> Result<Option<Committed>, Error>;
    /// The snapshot the repository holds under `sequence`'s immutable tag.
    fn snapshot(&self, sequence: u64) -> Result<Option<Held>, Error>;
    /// The inscription digest of the blocks snapshot at `plan`'s sequence,
    /// which the Dolos snapshot there names: the one the blocks repository
    /// holds, once reproduced from `files`, or else one published from them.
    fn blocks(
        &self,
        plan: &Plan,
        files: &dyn Fn() -> Result<EpochFiles, Error>,
    ) -> Result<Digest, Error>;
    /// Publish a prepared boundary, chained to the repository's latest
    /// snapshot, which has to be `predecessor`. Failures of the publication
    /// itself are [`Error::Finalizing`], which is what tells a damaged
    /// bundle from a registry that didn't answer.
    fn finalize(
        &self,
        finalizing: &Finalizing<'_>,
        predecessor: Option<&Committed>,
        source: &dyn SnapshotSource,
    ) -> Result<Committed, Error>;
}

/// What a publication of `plan` following `previous` links to, without a
/// transport. It resumes nothing, so it serves staging and a reproduction,
/// never a real upload.
pub fn following(
    previous: Option<&Inscription>,
    plan: &Plan,
) -> Result<Box<dyn Predecessor>, Error> {
    match previous {
        None => Ok(Box::new(First)),
        Some(previous) => Ok(Box::new(
            Following::new(previous, plan).map_err(Error::Planning)?,
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    UntilEpoch {
        sequence: u64,
    },
    UpToDate,
    /// Every boundary of a closed batch is prepared and journaled, and what
    /// isn't published yet is what [`Driver::prepare`] leaves to the flush.
    /// [`Driver::run`] publishes it and never returns this.
    BatchReady {
        flush: Flush,
        frontiers: Frontiers,
    },
}

enum Step {
    Extend { target: u64, prune: bool },
    Done { sequence: u64 },
    Ready(Flush),
}
enum Advance {
    Boundary {
        cursor_slot: u64,
        capture: Option<Capture>,
    },
    MithrilExhausted {
        cursor: Option<ChainPoint>,
    },
    Cancelled,
}
enum Capture {
    Bundle(Box<Bundle>),
    AlreadyPublished,
    OutOfSpace,
}
enum Import {
    Boundary,
    Exhausted,
    Cancelled,
}

pub(crate) fn slots_per_immutable_file(genesis: &Genesis) -> u64 {
    genesis
        .shelley
        .security_param
        .map_or(FALLBACK_SLOTS_PER_IMMUTABLE_FILE, |k| {
            u64::from(k) * SLOTS_PER_SECURITY_PARAM
        })
}

fn target_epoch(cursor_epoch: Option<u64>) -> u64 {
    cursor_epoch.map_or(1, |epoch| epoch + 1)
}

fn resume_file(highest: Option<u64>, cursor_slot: Option<u64>, width: u64) -> Option<u64> {
    highest.or_else(|| cursor_slot.map(|slot| (slot / width).saturating_sub(IMMUTABLE_FILE_MARGIN)))
}

fn resume_lag(resume: Option<u64>, cursor_slot: Option<u64>, width: u64) -> Option<u64> {
    let expected = (cursor_slot? / width).saturating_sub(IMMUTABLE_FILE_MARGIN);
    match expected.saturating_sub(resume.unwrap_or(0)) {
        0 => None,
        lag => Some(lag),
    }
}

fn next_window(resume: Option<u64>, window: u64, beacon: u64) -> Option<(Option<u64>, u64)> {
    match resume {
        Some(resume) if beacon <= resume => None,
        Some(resume) => Some((Some(resume), beacon.min(resume.saturating_add(window)))),
        None => Some((None, beacon.min(window))),
    }
}

fn fetch_advanced(before: Option<u64>, after: Option<u64>) -> bool {
    after > before
}

fn consumed_below(cursor_slot: u64, width: u64) -> u64 {
    (cursor_slot / width).saturating_sub(IMMUTABLE_FILE_MARGIN)
}

/// Why a fetch failed, and what its volume holds. The client names only the
/// locations it tried, and a full volume's error goes to a logger nobody reads.
fn fetch_failure(error: &dyn std::fmt::Display, download_dir: &Path) -> String {
    let dir = download_dir.display();
    match stelae_driver::preflight::available(download_dir) {
        Some(free) => {
            format!("{error}. {dir} has {free} bytes free: a low figure can mean a full volume")
        }
        None => format!(
            "{error}. The free space at {dir} can't be read, so a full volume can't be ruled out"
        ),
    }
}

fn dir_contents(immutable_dir: &Path) -> String {
    let Ok(entries) = std::fs::read_dir(immutable_dir) else {
        return "an unreadable immutable dir".to_owned();
    };
    let mut numbers: Vec<u64> = entries
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .split('.')
                .next()?
                .parse()
                .ok()
        })
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    match (numbers.first(), numbers.last()) {
        (Some(first), Some(last)) => {
            format!("files {first:05}..={last:05} ({} of them)", numbers.len())
        }
        _ => "no immutable files".to_owned(),
    }
}

/// The first immutable file of the epoch after the one `plan` closes: the
/// epoch the stores stand in, whose files the next blocks snapshot reads.
fn epoch_first_file(plan: &Plan, width: u64) -> Result<u64, Error> {
    let closed = blocks::closed_epoch(plan).map_err(Error::caller)?;
    Ok((closed.end_slot + 1) / width)
}

fn cleanup_consumed(immutable_dir: &Path, cursor_slot: u64, width: u64) -> Result<(), Error> {
    cleanup_below(immutable_dir, consumed_below(cursor_slot, width))
}

/// Remove the numbered immutable files below `threshold`.
fn cleanup_below(immutable_dir: &Path, threshold: u64) -> Result<(), Error> {
    if threshold == 0 {
        return Ok(());
    }
    let Ok(entries) = std::fs::read_dir(immutable_dir) else {
        return Ok(());
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(number) = name.split('.').next().and_then(|s| s.parse::<u64>().ok()) else {
            continue;
        };
        if number < threshold {
            std::fs::remove_file(entry.path())
                .map_err(|source| Error::RemoveConsumed { name, source })?;
            removed += 1;
        }
    }
    if removed > 0 {
        info!(removed, threshold, "removed consumed immutable files");
    }
    Ok(())
}

/// Download a source's immutable files `start..=end`, from its first when
/// `start` is `None`, into a directory's `immutable` subdirectory, retrying
/// what may pass; `None` when cancelled.
pub type Fetch<'a> = dyn Fn(&Path, Option<u64>, u64) -> Result<Option<()>, Error> + 'a;

pub struct Driver<'a, W: Workspace> {
    pub genesis: &'a Genesis,
    pub download_dir: PathBuf,
    /// Where an export spools its index runs.
    pub scratch: PathBuf,
    pub window: u64,
    pub until_epoch: Option<u64>,
    pub cancel: CancellationToken,
    /// The highest immutable file the source offers; `None` when cancelled.
    pub beacon: &'a dyn Fn() -> Result<Option<u64>, Error>,
    pub fetch: &'a Fetch<'a>,
    pub replay: &'a dyn Replay,
    pub open_workspace: &'a dyn Fn() -> Result<W, Error>,
    /// Refuse stores no run may continue. It opens them itself, so it runs with
    /// no workspace holding their locks.
    pub check_stores: &'a dyn Fn() -> Result<(), Error>,
    pub publish: &'a dyn Publish,
    pub batching: Batching<'a>,
}

/// How a run batches, and what it needs to beyond a batch of one.
pub struct Batching<'a> {
    pub limits: Limits,
    pub storage_path: PathBuf,
    /// The Dolos repository, which a batch is bound to.
    pub repository: String,
    /// Whether the batch's volume has this many bytes free.
    pub room: &'a dyn Fn(&Path, u64) -> bool,
    pub observer: &'a Observer,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub direct_s3: Option<Box<stelae::oci::s3_upload::Config>>,
    /// The network root, which holds the Dolos and the blocks repositories.
    pub repository: dolos_snapshot::registry::Repository,
    pub auth: Auth,
    pub insecure: bool,
    pub scratch_dir: Option<PathBuf>,
    pub concurrency: Option<std::num::NonZeroUsize>,
    /// The largest layer, in bytes, pushed as one request; the transport's
    /// default when `None`.
    pub monolithic_max: Option<u64>,
    pub download_dir: Option<PathBuf>,
    pub window: u64,
    pub until_epoch: Option<u64>,
    pub skip_validation: bool,
    pub mithril: MithrilSource,
    pub limits: Limits,
}

impl Options {
    /// Where a run spools its index runs.
    fn spool(&self, storage: &StorageConfig) -> PathBuf {
        node::scratch_dir(storage, self.scratch_dir.as_deref())
    }
}

fn require_synchronous_context() -> Result<(), Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        Err(Error::AsyncRuntimeContext)
    } else {
        Ok(())
    }
}

/// Run backfill on a synchronous process thread.
///
/// OCI publication stays on this thread while a private Tokio runtime drives
/// Mithril. Calling this entry point from an entered Tokio runtime is rejected.
pub fn run(
    config: &RootConfig,
    genesis: &Genesis,
    options: &Options,
    cancel: CancellationToken,
    observer: &stelae::progress::Observer,
) -> Result<Outcome, Error> {
    crate::config::require_full_history(config).map_err(Error::caller)?;
    require_synchronous_context()?;
    let download_dir = options
        .download_dir
        .clone()
        .unwrap_or_else(|| config.storage.path.join("mithril"));
    std::fs::create_dir_all(&download_dir).map_err(Error::caller)?;
    let runtime = tokio::runtime::Runtime::new().map_err(Error::caller)?;
    let dolos_repository =
        crate::publisher::dolos_repository(&options.repository).map_err(Error::caller)?;
    let settings = RepositoryPublish {
        repo: &options.repository,
        auth: options.auth.clone(),
        insecure: options.insecure,
        scratch_dir: options.scratch_dir.as_deref(),
        tuning: dolos_snapshot::registry::Tuning {
            direct_s3: options.direct_s3.clone(),
            concurrency: options.concurrency,
            monolithic_max: options.monolithic_max,
        },
    };
    let publisher = BackfillPublisher {
        config,
        settings,
        observer,
    };
    let genesis = Arc::new(genesis.clone());
    let aborting = cancel.clone();
    let handle = runtime.handle().clone();
    let beacon = || {
        stelae_driver::retry::transient(
            "listing mithril snapshots",
            &|| aborting.is_cancelled(),
            || {
                handle.block_on(async {
                    tokio::select! {
                        result = mithril::latest_immutable_file(&options.mithril) => result.map(Some),
                        _ = aborting.cancelled() => Ok(None),
                    }
                })
            },
        )
        .map_err(|error| Error::Mithril {
            what: "listing mithril snapshots",
            reason: error.to_string(),
        })
    };
    let fetch = mithril_fetch(
        &options.mithril,
        options.skip_validation,
        runtime.handle().clone(),
        cancel.clone(),
    );
    let driver = Driver::<ReplayWorkspace> {
        genesis: &genesis,
        download_dir,
        scratch: options.spool(&config.storage),
        window: options.window,
        until_epoch: options.until_epoch,
        cancel,
        beacon: &beacon,
        fetch: &fetch,
        replay: &(),
        open_workspace: &|| ReplayWorkspace::open(config, genesis.clone()).map_err(Error::caller),
        check_stores: &|| refuse_archive_behind(config, genesis.clone()),
        publish: &publisher,
        batching: Batching {
            limits: options.limits,
            storage_path: config.storage.path.clone(),
            repository: dolos_repository.to_string(),
            room: &batch::volume_has_room,
            observer,
        },
    };
    driver.run()
}

/// Download from `source` as a run does: retried, given up on cancellation,
/// and on failure saying what the download's volume holds.
pub fn mithril_fetch(
    source: &MithrilSource,
    skip_validation: bool,
    runtime: tokio::runtime::Handle,
    cancel: CancellationToken,
) -> impl Fn(&Path, Option<u64>, u64) -> Result<Option<()>, Error> + '_ {
    move |dir, start, end| {
        let fetch = mithril::Fetch {
            download_dir: dir,
            skip_validation,
            download_start: start,
            download_end: Some(end),
        };
        stelae_driver::retry::transient(
            "fetching a mithril immutable window",
            &|| cancel.is_cancelled(),
            || {
                runtime.block_on(async {
                    tokio::select! {
                        result = mithril::fetch_snapshot(&fetch, source, None) => result.map(Some),
                        _ = cancel.cancelled() => Ok(None),
                    }
                })
            },
        )
        .map_err(|error| Error::Mithril {
            what: "fetching a mithril immutable window",
            reason: fetch_failure(&error, dir),
        })
    }
}

/// What the stores showed when a batched run looked at them.
struct Reconciled {
    cursor: Option<ChainPoint>,
    epoch: Option<u64>,
    /// The repository already holds the sequence the stores stand in.
    published: bool,
    capture: Option<Capture>,
}

/// The repository's latest snapshot, once a batch may follow it.
struct Latest(Option<Committed>);

fn finish_both<T>(work: Result<T, Error>, finish: Result<(), Error>) -> Result<T, Error> {
    match (work, finish) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        // A store that hit a full volume stays poisoned, so its close fails too.
        (Err(replay), Err(shutdown)) if replay.fills_stores() => {
            warn!(%shutdown, "closing the stores after a store stopped the replay");
            Err(replay)
        }
        (Err(replay), Err(shutdown)) => Err(Error::ReplayAndShutdown {
            replay: Box::new(replay),
            shutdown: Box::new(shutdown),
        }),
    }
}

/// Say what a failed publication means for the batch: no room, a bundle or runs
/// that can't serve it, or else a repository worth asking again.
fn finalizing_failure(batch: &Batch, sequence: u64, error: Error) -> Error {
    use dolos_snapshot::Error as E;

    let Error::Finalizing(source) = error else {
        return error;
    };
    match &*source {
        // At this very sequence it can be this batch's own seal, applied after
        // its answer was lost, so the next try's first read compares it.
        E::Stelae(stelae::Error::PredecessorMoved { found, .. })
            if found.as_ref().is_none_or(|(held, _)| *held != sequence) =>
        {
            let found = found
                .clone()
                .map(|(sequence, identity)| Committed { sequence, identity });
            batch.moved(found).into()
        }
        error if batch::ran_out_of_room(error) => Error::FlushNoRoom(source),
        error if batch::runs_are_stale(error) => batch.runs_unusable(source).into(),
        E::Preparation(_) => batch.unusable(sequence, *source).into(),
        _ => Error::Finalizing(source),
    }
}

/// The epoch the stores stand in, which a cursor with no hash can't give.
fn anchored_epoch(source: &dyn SnapshotSource, slot: BlockSlot) -> Result<u64, Error> {
    match source.epoch() {
        Ok(Some(epoch)) => Ok(epoch),
        Ok(None) | Err(dolos_snapshot::Error::UnanchoredPoint(_)) => {
            Err(Error::UnanchoredCursor { slot })
        }
        Err(error) => Err(Error::Planning(error)),
    }
}

/// A capture that ran out of room still closes its batch when the stores' close
/// fails the same way: the journal names only what earlier closes made durable.
fn close_after_capture(capture: Option<&Capture>, finish: Result<(), Error>) -> Result<(), Error> {
    match finish {
        Err(error) if matches!(capture, Some(Capture::OutOfSpace)) && error.fills_stores() => {
            warn!(%error, "closing the stores after a capture filled the volume");
            Ok(())
        }
        other => other,
    }
}

/// Put a finished download's files and the directories naming them on disk,
/// then write the `marker` that vouches for them, so a host crash leaves the
/// marker only beside files that are whole. Mithril syncs nothing it unpacks.
fn keep_download(files: &Path, marker: &Path, range: &str) -> std::io::Result<()> {
    for entry in std::fs::read_dir(files)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            std::fs::File::open(entry.path())?.sync_all()?;
        }
    }
    batch::sync_dir(files)?;
    if let Some(download) = files.parent() {
        batch::sync_dir(download)?;
    }
    batch::write_file_durably(marker, range.as_bytes())
}

impl<W: Workspace> Driver<'_, W> {
    fn immutable_dir(&self) -> PathBuf {
        self.download_dir.join("immutable")
    }
    fn aborted(&self) -> bool {
        self.cancel.is_cancelled()
    }

    fn open(&self) -> Result<W, Error> {
        (self.open_workspace)().map_err(Error::in_replay)
    }

    /// Replay and publish until the source runs out or `until_epoch` is
    /// published. A periodic run therefore drains what it prepared, however few
    /// boundaries the source offered, and leaves nothing waiting for a full
    /// batch. A failing source closes the batch too, unless something fails
    /// before the journal records that, and the run still ends on that failure.
    pub fn run(&self) -> Result<Outcome, Error> {
        if self.batching.limits.is_one() {
            return self.run_one();
        }
        loop {
            match self.prepare() {
                Ok(Outcome::BatchReady { .. }) => self.flush()?,
                Ok(outcome) => return Ok(outcome),
                Err(failure) if failure.is_source_failure() && !self.aborted() => {
                    warn!(error = %failure, "reading the source failed");
                    return Err(match self.flush_open_batch() {
                        Ok(()) => failure,
                        Err(flush) => Error::SourceAndFlush {
                            failure: Box::new(failure),
                            flush: Box::new(flush),
                        },
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Close what the batch prepared at the frontier the stores reached, and
    /// publish it.
    fn flush_open_batch(&self) -> Result<(), Error> {
        let mut batch = self.open_batch()?;
        if batch.is_empty() {
            return Ok(());
        }
        warn!("publishing what the batch prepared before the run ends");
        let workspace = self.open()?;
        let cursor = workspace
            .snapshot()
            .committed_position()
            .map_err(Error::Planning);
        let cursor = finish_both(cursor, workspace.finish().map_err(Error::in_replay))?;
        batch.close(Flush::EndOfSource, cursor.as_ref())?;
        self.flush()
    }

    /// Prepare until a batch closes, which publishes nothing.
    pub fn prepare(&self) -> Result<Outcome, Error> {
        let width = self.begin()?;
        self.run_batched(width)
    }

    /// What every run asks first, and the geometry it replays by.
    fn begin(&self) -> Result<u64, Error> {
        if self.window == 0 {
            return Err(Error::EmptyWindow);
        }
        (self.check_stores)().map_err(Error::in_replay)?;
        let width = slots_per_immutable_file(self.genesis);
        info!(
            slots_per_immutable_file = width,
            "derived immutable chunk size from genesis"
        );
        Ok(width)
    }

    fn run_one(&self) -> Result<Outcome, Error> {
        let width = self.begin()?;
        batch::clear_if_drained(&self.batching.storage_path)?;
        batch::refuse_pending(&self.batching.storage_path)?;

        loop {
            if self.aborted() {
                return Err(Error::Interrupted);
            }
            let (target, prune) = match self.publish_pending(width)? {
                Step::Done { sequence } => return Ok(Outcome::UntilEpoch { sequence }),
                Step::Ready(_) => unreachable!("a batch of one has nothing to close"),
                Step::Extend { target, prune } => (target, prune),
            };
            match self.extend(target, prune, width, None)? {
                // Its blocks snapshot reads the files the boundary closed, so
                // they go once its snapshot is published, at the next turn.
                Advance::Boundary { .. } => {}
                Advance::MithrilExhausted { .. } => return Ok(Outcome::UpToDate),
                Advance::Cancelled => return Err(Error::Interrupted),
            }
        }
    }

    fn run_batched(&self, width: u64) -> Result<Outcome, Error> {
        let immutable_dir = self.immutable_dir();
        let mut batch = self.open_batch()?;
        let mut unchecked = !batch.is_empty();
        // Before replay moves: past here a lost bundle is only found at the end.
        batch.bundles()?;

        loop {
            if self.aborted() {
                return Err(Error::Interrupted);
            }
            let (target, prune) = match self.settle(&mut batch, &mut unchecked)? {
                Step::Done { sequence } => return Ok(Outcome::UntilEpoch { sequence }),
                Step::Ready(flush) => return Ok(self.ready(&batch, flush)),
                Step::Extend { target, prune } => (target, prune),
            };
            match self.extend(target, prune, width, Some(&mut batch))? {
                Advance::Boundary {
                    cursor_slot,
                    capture,
                } => {
                    if let Some(flush) = self.settle_capture(&mut batch, capture)? {
                        return Ok(self.ready(&batch, flush));
                    }
                    cleanup_consumed(&immutable_dir, cursor_slot, width)?;
                }
                Advance::MithrilExhausted { cursor } => {
                    if batch.is_empty() {
                        return Ok(Outcome::UpToDate);
                    }
                    // The epoch in progress stays unpublished: it has no boundary.
                    batch.close(Flush::EndOfSource, cursor.as_ref())?;
                    return Ok(self.ready(&batch, Flush::EndOfSource));
                }
                Advance::Cancelled => return Err(Error::Interrupted),
            }
        }
    }

    fn open_batch(&self) -> Result<Batch, Error> {
        let source = batch::Source {
            repository: self.batching.repository.clone(),
            network_magic: u64::from(self.genesis.network_magic()),
            store_contract: dolos_core::compat::SUPPORTED_CONTRACTS[0].to_owned(),
            mutable_slots: dolos_cardano::mutable_slots(self.genesis),
            dolos_revision: crate::DOLOS_REVISION.to_owned(),
        };
        let dir = batch::dir_in(&self.batching.storage_path);
        let mut batch = Batch::open(dir, self.batching.limits, source)?;
        batch.clear()?;
        Ok(batch)
    }

    /// Stage a closed batch once and publish it in order, reading the stores
    /// and writing none, so replay stays where the batch closed until the last
    /// sequence commits.
    fn flush(&self) -> Result<(), Error> {
        let mut batch = self.open_batch()?;
        let workspace = self.open()?;
        let result = self.publish_batch(&workspace.snapshot(), &mut batch);
        finish_both(result, workspace.finish().map_err(Error::in_replay))?;
        Ok(batch.clear()?)
    }

    fn publish_batch(&self, source: &dyn SnapshotSource, batch: &mut Batch) -> Result<(), Error> {
        // A crash after the journal moved left that bundle behind.
        batch.sweep()?;
        let mut rebuilt = false;
        loop {
            let staged = self.stage(source, batch)?;
            match self.publish_staged(source, batch, &staged) {
                Err(Error::Batch(batch::Error::RunsUnusable { source, .. })) if !rebuilt => {
                    warn!(error = %source, "the staged runs can't finish a publication: staging them again");
                    rebuilt = true;
                    batch.drop_staging()?;
                }
                outcome => return outcome,
            }
        }
    }

    fn stage(&self, source: &dyn SnapshotSource, batch: &mut Batch) -> Result<Staged, Error> {
        let next = batch.prepared().first().map_or(0, |next| next.sequence);
        let followed = self.followed(batch)?;
        let previous = match batch.bundles()?.first() {
            Some(first) => following(followed.as_ref(), &first.plan())?,
            None => Box::new(First),
        };
        let staged = batch.stage(source, &*previous, self.batching.observer);
        self.report(batch);
        staged.map_err(|error| match error {
            batch::Error::Staging { source, .. } if batch::ran_out_of_room(&source) => {
                Error::FlushNoRoom(source)
            }
            batch::Error::Staging { source, .. } if batch::stores_cant_serve(&source) => {
                batch.unusable(next, *source).into()
            }
            other => other.into(),
        })
    }

    /// The document the journal says the next publication follows, held to the
    /// identity the journal recorded.
    fn followed(&self, batch: &Batch) -> Result<Option<Inscription>, Error> {
        let Some(planned) = batch.predecessor() else {
            return Ok(None);
        };
        let held = stelae_driver::retry::transient(
            "reading the snapshot a batch follows",
            &|| self.aborted(),
            || self.publish.snapshot(planned.sequence),
        )?;
        match held {
            Some(held) if held.committed == *planned => Ok(Some(held.inscription)),
            held => Err(batch.moved(held.map(|held| held.committed)).into()),
        }
    }

    fn publish_staged(
        &self,
        source: &dyn SnapshotSource,
        batch: &mut Batch,
        staged: &Staged,
    ) -> Result<(), Error> {
        for bundle in batch.bundles()? {
            if self.aborted() {
                return Err(Error::Interrupted);
            }
            // Each try reads the repository again, so one whose acknowledgement
            // was lost reconciles rather than publishing over itself.
            let committed = stelae_driver::retry::transient(
                "publishing a prepared sequence",
                &|| self.aborted(),
                || match self.commit_next(source, batch, &bundle, staged) {
                    Err(error) if error.is_refusal() => Ok(Err(error)),
                    other => other.map(Ok),
                },
            )??;
            info!(sequence = committed.sequence, identity = %committed.identity, "committed");
            batch.commit(committed)?;
            self.report(batch);
        }
        Ok(())
    }

    /// Bring the repository to hold `bundle`'s sequence on exactly what the
    /// journal says it follows.
    ///
    /// A seal writes three tags in turn: the sequence's tip, the sequence, and
    /// `latest`. A seal stopped after the first leaves the sequence absent, and
    /// sealing again writes the same tip. One stopped after the second leaves
    /// the sequence held under a `latest` that still follows the journal, and
    /// one stopped after the third leaves `latest` at the sequence: each of
    /// those is reproduced and compared before it counts as this batch's.
    fn commit_next(
        &self,
        source: &dyn SnapshotSource,
        batch: &Batch,
        bundle: &Bundle,
        staged: &Staged,
    ) -> Result<Committed, Error> {
        let sequence = bundle.sequence();
        let plan = bundle.plan();
        let finalizing = Finalizing {
            store: batch.store(),
            bundle,
            staged,
            producers: plan.producers,
        };
        let latest = self.publish.latest(&plan)?;
        if latest.as_ref() != batch.predecessor() {
            return match latest {
                Some(found) if found.sequence == sequence => {
                    self.confirm(source, batch, &finalizing, &found)?;
                    Ok(found)
                }
                found => Err(batch.moved(found).into()),
            };
        }
        // A seal writes the sequence's tags before it moves the latest one, so
        // a tag here is a seal that stopped half way, or another writer's.
        // Sealing again only repeats what this batch would write.
        if let Some(held) = self.publish.snapshot(sequence)? {
            let expected = self.reproduce(source, batch, &finalizing, sequence)?;
            if expected != held.committed.identity {
                let found = held.committed.identity;
                return Err(batch.tag_taken(sequence, found, expected).into());
            }
        }
        self.publish.announce(&plan)?;
        self.publish
            .finalize(&finalizing, batch.predecessor(), source)
            .map_err(|error| finalizing_failure(batch, sequence, error))
    }

    /// Hold a `latest` snapshot already at the next sequence to what this batch
    /// publishes there.
    fn confirm(
        &self,
        source: &dyn SnapshotSource,
        batch: &Batch,
        finalizing: &Finalizing<'_>,
        found: &Committed,
    ) -> Result<(), Error> {
        let sequence = found.sequence;
        let expected = self.reproduce(source, batch, finalizing, sequence)?;
        if expected == found.identity {
            return Ok(());
        }
        Err(batch
            .diverged(sequence, found.identity.clone(), expected)
            .into())
    }

    /// The identity this batch publishes at `sequence`, reproduced from the
    /// same bundle, runs and predecessor.
    fn reproduce(
        &self,
        source: &dyn SnapshotSource,
        batch: &Batch,
        finalizing: &Finalizing<'_>,
        sequence: u64,
    ) -> Result<String, Error> {
        let followed = self.followed(batch)?;
        let previous = following(followed.as_ref(), &finalizing.bundle.plan())?;
        warn!(
            sequence,
            "the repository already holds this sequence: reproducing it to compare"
        );
        let document = source
            .finalize_document(finalizing, &*previous)
            .map_err(|error| {
                finalizing_failure(batch, sequence, Error::Finalizing(error.into()))
            })?;
        Ok(document.identity.to_string())
    }

    fn ready(&self, batch: &Batch, flush: Flush) -> Outcome {
        self.report(batch);
        Outcome::BatchReady {
            flush,
            frontiers: batch.frontiers(),
        }
    }

    fn report(&self, batch: &Batch) {
        let frontiers = batch.frontiers();
        info!(
            published = ?frontiers.published,
            prepared = ?frontiers.prepared,
            staged = ?frontiers.staged,
            replay = ?frontiers.replay,
            "batch frontiers"
        );
        self.replay.frontiers(&frontiers);
    }

    fn plan(&self, source: &dyn SnapshotSource) -> Result<Plan, Error> {
        source
            .plan(u64::from(self.genesis.network_magic()))
            .map_err(Error::Planning)
    }

    /// `plan` naming the blocks snapshot at its sequence, which the blocks
    /// repository holds or this publishes there first, from the files of the
    /// epoch its cut closes. A refusal is not tried again, and neither is a
    /// download that failed: it has had its own retries.
    fn name_blocks(&self, plan: Plan) -> Result<Plan, Error> {
        let scope = blocks::closed_epoch(&plan).map_err(Error::caller)?;
        let files = || self.epoch_files(scope);
        let named = stelae_driver::retry::transient(
            "publishing the blocks snapshot a Dolos snapshot names",
            &|| self.aborted(),
            || match self.publish.blocks(&plan, &files) {
                Err(error) if error.is_refusal() || error.is_source_failure() => Ok(Err(error)),
                other => other.map(Ok),
            },
        )??;
        self.remove_refetched()?;
        Ok(plan.with_blocks(named))
    }

    /// The immutable files of the epoch `scope` covers: the window's while it
    /// holds them, or else downloaded again apart from it, which a restart
    /// after they were removed needs.
    ///
    /// A download is used only once it finished and its files were checked and
    /// put on disk, which a marker beside them records. One that failed or
    /// stopped is removed.
    fn epoch_files(&self, scope: EpochScope) -> Result<EpochFiles, Error> {
        let own = EpochFiles {
            dir: self.immutable_dir(),
            scope,
            slots_per_file: slots_per_immutable_file(self.genesis),
        };
        if own.epoch().check_files().is_ok() {
            return Ok(own);
        }
        let refused = |dir: &Path, source| blocks::Error::Files {
            epoch: scope.epoch,
            dir: dir.to_owned(),
            source: Box::new(source),
        };
        let files = own
            .epoch()
            .files()
            .map_err(|error| refused(&own.dir, error))?;
        // One file past the epoch, which shows its last one closed.
        let (start, end) = (*files.start(), files.end() + 1);
        let refetch = self.download_dir.join(REFETCH_DIR);
        let again = EpochFiles {
            dir: refetch.join("immutable"),
            ..own.clone()
        };
        let marker = refetch.join(REFETCHED_FILE);
        let downloaded = format!("{start}..={end}");
        if std::fs::read_to_string(&marker).is_ok_and(|held| held == downloaded)
            && again.epoch().check_files().is_ok()
        {
            return Ok(again);
        }
        warn!(
            epoch = scope.epoch,
            start, end, "the epoch's immutable files are gone: downloading them again"
        );
        self.remove_refetched()?;
        // Mithril unpacks only into a directory that is already there.
        let fetched = std::fs::create_dir_all(&refetch)
            .map_err(Error::caller)
            .and_then(|()| (self.fetch)(&refetch, Some(start), end))
            .and_then(|done| match done {
                Some(()) => again
                    .epoch()
                    .check_files()
                    .map_err(|error| refused(&again.dir, error).into())
                    .and_then(|()| {
                        keep_download(&again.dir, &marker, &downloaded).map_err(Error::caller)
                    }),
                None => Err(Error::Interrupted),
            });
        if let Err(error) = fetched {
            if let Err(removing) = self.remove_refetched() {
                warn!(%removing, "removing a download that did not finish");
            }
            return Err(error);
        }
        Ok(again)
    }

    fn remove_refetched(&self) -> Result<(), Error> {
        let dir = self.download_dir.join(REFETCH_DIR);
        match std::fs::remove_dir_all(&dir) {
            Err(source) if source.kind() != std::io::ErrorKind::NotFound => {
                Err(Error::RemoveConsumed {
                    name: dir.display().to_string(),
                    source,
                })
            }
            _ => Ok(()),
        }
    }

    fn latest(&self, plan: &Plan) -> Result<Option<Committed>, Error> {
        stelae_driver::retry::transient(
            "reading the repository's latest snapshot",
            &|| self.aborted(),
            || self.publish.latest(plan),
        )
    }

    /// Hold the stores to the journal and capture a boundary they stand on with
    /// no bundle yet, which only a restart reaches. Then decide, from what the
    /// batch holds, whether replay may advance.
    fn settle(&self, batch: &mut Batch, unchecked: &mut bool) -> Result<Step, Error> {
        let workspace = self.open()?;
        let result = self.reconcile(&workspace.snapshot(), batch, unchecked);
        let capture = result.as_ref().ok().and_then(|r| r.capture.as_ref());
        let finish = close_after_capture(capture, workspace.finish().map_err(Error::in_replay));
        let reconciled = finish_both(result, finish)?;

        if let Some(flush) = self.settle_capture(batch, reconciled.capture)? {
            return Ok(Step::Ready(flush));
        }
        if let Some(flush) = batch.ready() {
            return Ok(Step::Ready(flush));
        }

        let sequence = reconciled.epoch.unwrap_or(0);
        let until = self.until_epoch;
        if batch.is_empty() && reconciled.published && until.is_some_and(|until| sequence >= until)
        {
            return Ok(Step::Done { sequence });
        }

        let last = batch.last_sequence();
        let until_reached = last.is_some_and(|last| until.is_some_and(|until| last >= until));
        let dir = batch::dir_in(&self.batching.storage_path);
        let room = |bytes| (self.batching.room)(&dir, bytes);
        match batch.admit(until_reached, &room)? {
            Admission::Advance => Ok(Step::Extend {
                target: target_epoch(reconciled.epoch),
                prune: batch.is_empty() && reconciled.published,
            }),
            Admission::Flush(flush) => {
                batch.close(flush, reconciled.cursor.as_ref())?;
                Ok(Step::Ready(flush))
            }
            Admission::NoRoom { needed, used } => Err(Error::NoRoom { needed, used }),
        }
    }

    fn reconcile(
        &self,
        source: &dyn SnapshotSource,
        batch: &mut Batch,
        unchecked: &mut bool,
    ) -> Result<Reconciled, Error> {
        let cursor = source.committed_position().map_err(Error::Planning)?;
        let epoch = match &cursor {
            None => None,
            Some(cursor) => Some(anchored_epoch(source, cursor.slot())?),
        };
        let position = batch.position(cursor.as_ref(), epoch.unwrap_or(0))?;
        let mut reconciled = Reconciled {
            cursor,
            epoch,
            published: false,
            capture: None,
        };
        let Some(sequence) = epoch.filter(|epoch| *epoch > 0) else {
            return Ok(reconciled);
        };
        let plan = self.plan(source)?;

        if *unchecked {
            batch.check_predecessor(self.latest(&plan)?)?;
            *unchecked = false;
        }
        if batch.ready().is_some() {
            return Ok(reconciled);
        }

        let capture = match (batch.is_empty(), position) {
            (false, Position::Within) => false,
            (false, Position::NextEpoch) => match source.boundary(&plan) {
                Ok(_) => true,
                Err(dolos_snapshot::Error::NotABoundary { .. }) => {
                    return Err(batch.unrepresented(sequence).into())
                }
                Err(error) => return Err(Error::Planning(error)),
            },
            (true, _) => {
                reconciled.published = self.unpublished(&plan, sequence)?.is_none();
                !reconciled.published
            }
        };
        if capture {
            warn!(
                sequence,
                "capturing a boundary this process did not import: the index tail takes a \
                 whole-index traversal"
            );
            reconciled.capture = Some(self.capture(batch, source, IndexTail::Traverse)?);
        }
        Ok(reconciled)
    }

    /// What a batch beginning at `sequence` would follow, or `None` when the
    /// repository already holds that sequence.
    fn unpublished(&self, plan: &Plan, sequence: u64) -> Result<Option<Latest>, Error> {
        let latest = self.latest(plan)?;
        match Standing::read(latest.as_ref().map(|latest| latest.sequence), sequence) {
            Standing::UpToDate { .. } => Ok(None),
            Standing::Empty | Standing::Next { .. } => Ok(Some(Latest(latest))),
            Standing::Ahead { latest, distance } => {
                Err(Error::caller(PolicyError::PublishWouldGap {
                    latest,
                    sequence,
                    distance,
                }))
            }
        }
    }

    /// Capture the boundary `source` stands on. The bundle is complete when
    /// this returns, and the caller journals it once the stores are durable.
    fn capture(
        &self,
        batch: &mut Batch,
        source: &dyn SnapshotSource,
        tail: IndexTail<'_>,
    ) -> Result<Capture, Error> {
        let plan = self.plan(source)?;
        let begins = match batch.is_empty() {
            true => match self.unpublished(&plan, plan.sequence)? {
                None => return Ok(Capture::AlreadyPublished),
                Some(Latest(latest)) => Some(latest),
            },
            false => None,
        };
        // Before anything is downloaded or written for either repository.
        if let Some(None) = begins {
            batch::refuse_beginning(&self.batching.storage_path, plan.sequence)?;
        }
        // Before anything is written for the boundary, so a blocks snapshot
        // that can't be named leaves the batch as it was.
        let plan = self.name_blocks(plan)?;
        if let Some(latest) = begins {
            batch.begin(&self.batching.storage_path, plan.sequence, latest)?;
        }
        let request = prepare::Request {
            store_contract: batch.source().store_contract.clone(),
            mutable_slots: batch.source().mutable_slots,
            tail,
        };
        info!(sequence = plan.sequence, cursor = %plan.cursor, "preparing boundary");
        let captured =
            source.prepare_boundary(batch.store(), &plan, &request, self.batching.observer);
        match captured {
            Ok(bundle) => Ok(Capture::Bundle(Box::new(bundle))),
            Err(error) if batch::out_of_space(&error) => {
                warn!(%error, sequence = plan.sequence, "a capture ran out of room, and what it wrote goes");
                Ok(Capture::OutOfSpace)
            }
            Err(error) => Err(batch::Error::Preparing(Box::new(error)).into()),
        }
    }

    /// Journal a capture, or close the batch on a full volume. An empty batch
    /// has nothing to flush, and its boundary is captured again on a rerun.
    fn settle_capture(
        &self,
        batch: &mut Batch,
        capture: Option<Capture>,
    ) -> Result<Option<Flush>, Error> {
        match capture {
            Some(Capture::Bundle(bundle)) => {
                batch.record(&bundle)?;
                self.report(batch);
                Ok(None)
            }
            Some(Capture::AlreadyPublished) | None => Ok(None),
            Some(Capture::OutOfSpace) => {
                batch.sweep()?;
                if batch.is_empty() {
                    return Err(Error::NoRoom {
                        needed: batch.limits().reserve_per_epoch,
                        used: batch.used()?,
                    });
                }
                batch.close(Flush::DiskPressure, None)?;
                Ok(Some(Flush::DiskPressure))
            }
        }
    }

    fn publish_pending(&self, width: u64) -> Result<Step, Error> {
        let workspace = self.open()?;
        let result = self.plan_pending(&workspace.snapshot(), width);
        finish_both(result, workspace.finish().map_err(Error::in_replay))
    }

    /// Publish the boundary the stores stand on, unless the repository holds
    /// it, then remove the immutable files it consumed. The files of the epoch
    /// the stores stand in stay, for the next boundary's blocks snapshot.
    fn plan_pending(&self, source: &dyn SnapshotSource, width: u64) -> Result<Step, Error> {
        let Some(cursor) = source.committed_position().map_err(Error::Planning)? else {
            return Ok(Step::Extend {
                target: target_epoch(None),
                prune: false,
            });
        };
        let epoch = anchored_epoch(source, cursor.slot())?;
        if epoch == 0 {
            return Ok(Step::Extend {
                target: target_epoch(Some(epoch)),
                prune: false,
            });
        }
        let plan = self.plan(source)?.with_scratch(self.scratch.clone());
        let sequence = plan.sequence;
        let consumed = consumed_below(cursor.slot(), width).min(epoch_first_file(&plan, width)?);
        // Standing first: a sequence the repository holds or can't take asks
        // nothing of the blocks repository.
        if let Some(Latest(latest)) = self.unpublished(&plan, sequence)? {
            if latest.is_none() {
                batch::refuse_beginning(&self.batching.storage_path, sequence)?;
            }
            let plan = self.name_blocks(plan)?;
            self.publish.announce(&plan)?;
            stelae_driver::retry::transient(
                "publishing the pending sequence",
                &|| self.aborted(),
                || match self.publish.publish(&plan, source) {
                    Err(error) if error.is_refusal() => Ok(Err(error)),
                    other => other.map(Ok),
                },
            )??;
        }
        cleanup_below(&self.immutable_dir(), consumed)?;
        if self.until_epoch.is_some_and(|until| sequence >= until) {
            return Ok(Step::Done { sequence });
        }
        Ok(Step::Extend {
            target: target_epoch(Some(epoch)),
            prune: true,
        })
    }

    fn extend(
        &self,
        target: u64,
        prune: bool,
        width: u64,
        batch: Option<&mut Batch>,
    ) -> Result<Advance, Error> {
        let workspace = self.open()?;
        let mut session = workspace.start(target).map_err(Error::in_replay)?;
        let result = self.advance_session(&mut session, prune, width, batch);
        let capture = match &result {
            Ok(Advance::Boundary { capture, .. }) => capture.as_ref(),
            _ => None,
        };
        let finish = close_after_capture(capture, session.finish().map_err(Error::in_replay));
        finish_both(result, finish)
    }

    fn advance_session(
        &self,
        session: &mut W::Session,
        prune: bool,
        width: u64,
        batch: Option<&mut Batch>,
    ) -> Result<Advance, Error> {
        let immutable_dir = self.immutable_dir();
        if prune {
            let rounds = session.prune_history().map_err(Error::in_replay)?;
            info!(rounds, "housekeeping drained after publication");
        }
        self.replay.round_started();
        let result = self.advance_loop(session, &immutable_dir, width);
        self.replay.round_finished();
        match (result?, batch) {
            (Advance::Boundary { .. }, Some(_)) if self.aborted() => Ok(Advance::Cancelled),
            (Advance::Boundary { cursor_slot, .. }, Some(batch)) => {
                let tail = match session.boundary_index_delta() {
                    Some(delta) => IndexTail::Delta(delta),
                    None => IndexTail::Traverse,
                };
                let capture = self.capture(batch, &session.snapshot(), tail)?;
                Ok(Advance::Boundary {
                    cursor_slot,
                    capture: Some(capture),
                })
            }
            (advance, _) => Ok(advance),
        }
    }

    fn advance_loop(
        &self,
        session: &mut W::Session,
        immutable_dir: &Path,
        width: u64,
    ) -> Result<Advance, Error> {
        loop {
            if self.aborted() {
                return Ok(Advance::Cancelled);
            }
            match self.import_available(session, immutable_dir)? {
                Import::Boundary => {
                    let cursor_slot = session
                        .committed_position()?
                        .map(|point| point.slot())
                        .unwrap_or_default();
                    return Ok(Advance::Boundary {
                        cursor_slot,
                        capture: None,
                    });
                }
                Import::Cancelled => return Ok(Advance::Cancelled),
                Import::Exhausted => {}
            }

            let Some(beacon) = (self.beacon)()? else {
                return Ok(Advance::Cancelled);
            };

            let highest = mithril::highest_existing_immutable(immutable_dir);
            let cursor = session.committed_position()?;
            let cursor_slot = cursor.as_ref().map(ChainPoint::slot);
            let resume = resume_file(highest, cursor_slot, width);
            if let Some(lag) =
                resume_lag(resume, cursor_slot, width).filter(|lag| *lag > self.window)
            {
                warn!(
                    lag,
                    ?resume,
                    width,
                    "immutable download resumes more than one window behind the cursor"
                );
            }
            let Some((start, end)) = next_window(resume, self.window, beacon) else {
                return Ok(Advance::MithrilExhausted { cursor });
            };
            let fetched = (self.fetch)(&self.download_dir, start, end)?;
            if fetched.is_none() {
                return Ok(Advance::Cancelled);
            }
            let after = mithril::highest_existing_immutable(immutable_dir);
            if !fetch_advanced(highest, after) {
                return Err(Error::StalledWindow {
                    start: start.unwrap_or(0),
                    end,
                    highest,
                    after,
                    cursor_slot: cursor_slot.unwrap_or_default(),
                    contents: dir_contents(immutable_dir),
                });
            }
        }
    }

    fn import_available(
        &self,
        session: &mut W::Session,
        immutable_dir: &Path,
    ) -> Result<Import, Error> {
        use pallas::{
            interop::hardano::storage::immutable::{self, Error as Reader},
            network::miniprotocols::Point,
        };

        if !immutable_dir.is_dir() || mithril::highest_existing_immutable(immutable_dir).is_none() {
            return Ok(Import::Exhausted);
        }
        let cursor = session.committed_position()?;
        let point: Point = match cursor {
            None => Point::Origin,
            Some(cursor) => {
                let slot = cursor.slot();
                cursor
                    .try_into()
                    .map_err(|_| Error::UnanchoredCursor { slot })?
            }
        };
        let mut blocks =
            immutable::read_blocks_from_point(immutable_dir, point.clone()).map_err(|error| {
                match error {
                    Reader::CannotFindBlock(_) | Reader::OriginMissing => Error::StaleWindow {
                        dir: immutable_dir.to_owned(),
                        contents: dir_contents(immutable_dir),
                        at: match &point {
                            Point::Origin => "the chain's first block".to_owned(),
                            Point::Specific(slot, _) => format!("slot {slot}"),
                        },
                    },
                    other => Error::ImmutableDb(other.to_string()),
                }
            })?;
        if point != Point::Origin {
            blocks.next();
        }
        for batch in blocks.chunks(IMPORT_CHUNK).into_iter() {
            let batch: Vec<_> = batch
                .try_collect()
                .map_err(|error| Error::BlockData(error.to_string()))?;
            let progress = session
                .import_blocks(batch.into_iter().map(Arc::new).collect())
                .map_err(Error::in_replay)?;
            self.replay.reached(progress.position().slot());
            if progress.is_boundary() {
                return Ok(Import::Boundary);
            }
            if self.aborted() {
                return Ok(Import::Cancelled);
            }
        }
        Ok(Import::Exhausted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAINNET_WIDTH: u64 = 21_600;
    const PREVIEW_CURSOR: u64 = 22_118_504;

    #[test]
    fn targets_follow_the_cursor() {
        assert_eq!(target_epoch(None), 1);
        assert_eq!(target_epoch(Some(0)), 1);
        assert_eq!(target_epoch(Some(499)), 500);
    }

    #[test]
    fn chunk_width_is_network_specific() {
        use dolos_cardano::include;
        assert_eq!(
            slots_per_immutable_file(&include::mainnet::load()),
            MAINNET_WIDTH
        );
        assert_eq!(
            slots_per_immutable_file(&include::preprod::load()),
            MAINNET_WIDTH
        );
        assert_eq!(slots_per_immutable_file(&include::preview::load()), 4_320);
    }

    #[test]
    fn cold_start_resumes_from_cursor_margin() {
        assert_eq!(resume_file(None, Some(PREVIEW_CURSOR), 4_320), Some(5_118));
        assert_eq!(
            resume_file(None, Some(MAINNET_WIDTH), MAINNET_WIDTH),
            Some(0)
        );
        assert_eq!(resume_file(None, None, MAINNET_WIDTH), None);
        assert_eq!(
            resume_file(Some(120), Some(MAINNET_WIDTH * 6000), MAINNET_WIDTH),
            Some(120)
        );
    }

    #[test]
    fn resume_lag_reports_only_distance_behind_margin() {
        assert_eq!(resume_lag(Some(5_118), Some(PREVIEW_CURSOR), 4_320), None);
        assert_eq!(
            resume_lag(Some(1_022), Some(PREVIEW_CURSOR), 4_320),
            Some(4_096)
        );
        assert_eq!(resume_lag(Some(9_000), Some(PREVIEW_CURSOR), 4_320), None);
    }

    #[test]
    fn window_clamps_and_refetches_resume_file() {
        assert_eq!(next_window(None, 40, 1_000), Some((None, 40)));
        assert_eq!(next_window(Some(100), 40, 1_000), Some((Some(100), 140)));
        assert_eq!(next_window(Some(990), 40, 1_000), Some((Some(990), 1_000)));
        assert_eq!(next_window(Some(1_000), 40, 1_000), None);
    }

    #[test]
    fn only_a_new_highest_file_counts_as_fetch_progress() {
        assert!(fetch_advanced(None, Some(0)));
        assert!(fetch_advanced(Some(5), Some(6)));
        assert!(!fetch_advanced(Some(5), Some(5)));
        assert!(!fetch_advanced(None, None));
    }

    #[test]
    fn cleanup_threshold_keeps_two_files_of_margin() {
        assert_eq!(consumed_below(0, MAINNET_WIDTH), 0);
        assert_eq!(consumed_below(MAINNET_WIDTH * 3, MAINNET_WIDTH), 1);
        assert_eq!(consumed_below(MAINNET_WIDTH * 10 + 5, MAINNET_WIDTH), 8);
    }

    #[test]
    fn cleanup_removes_only_consumed_numbered_files() {
        let dir = tempfile::tempdir().unwrap();
        for n in 0..6u64 {
            for ext in ["chunk", "primary", "secondary"] {
                std::fs::write(dir.path().join(format!("{n:05}.{ext}")), []).unwrap();
            }
        }
        std::fs::write(dir.path().join("lock"), []).unwrap();
        cleanup_consumed(dir.path(), MAINNET_WIDTH * 5, MAINNET_WIDTH).unwrap();
        let mut remaining: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        remaining.sort();
        assert_eq!(
            remaining,
            [
                "00003.chunk",
                "00003.primary",
                "00003.secondary",
                "00004.chunk",
                "00004.primary",
                "00004.secondary",
                "00005.chunk",
                "00005.primary",
                "00005.secondary",
                "lock",
            ]
        );
    }

    #[test]
    fn only_a_replay_on_a_full_volume_is_a_recoverable_stop() {
        let full = Error::caller(std::io::Error::other(
            "state error: IO: No space left on device (os error 28)",
        ));
        assert!(matches!(full.in_replay(), Error::ReplayFilledVolume(_)));

        let broken = Error::caller(std::io::Error::other("broken invariant"));
        assert!(matches!(broken.in_replay(), Error::Caller(_)));
    }

    #[test]
    fn a_close_failing_after_replay_filled_the_volume_is_the_same_stop() {
        let filled = Error::ReplayFilledVolume(std::io::Error::other("full").into());
        let error = finish_both::<()>(Err(filled), Err(Error::EmptyWindow)).unwrap_err();
        assert!(matches!(error, Error::ReplayFilledVolume(_)), "{error}");

        let poisoned = Error::StorePoisoned(std::io::Error::other("poisoned").into());
        let error = finish_both::<()>(Err(poisoned), Err(Error::EmptyWindow)).unwrap_err();
        assert!(matches!(error, Error::StorePoisoned(_)), "{error}");
    }

    #[test]
    fn a_failed_worker_is_named_without_calling_the_volume_full() {
        let poisoned = Error::caller(std::io::Error::other(
            "state error: internal store error: fjall error: FjallError: Poisoned",
        ));
        let stop = poisoned.in_replay();
        assert!(matches!(stop, Error::StorePoisoned(_)), "{stop}");
        let message = stop.to_string();
        assert!(
            message.contains("a full volume or a failing disk"),
            "{message}"
        );
        assert!(!message.contains("ran out of room"), "{message}");
    }

    #[test]
    fn a_gap_says_where_the_archive_ends() {
        let text = |tip| Error::ArchiveBehind { cursor: 200, tip }.to_string();
        assert!(
            text(Some(101)).contains("the archive's last block is at slot 101:"),
            "{}",
            text(Some(101))
        );
        assert!(
            text(None).contains("the archive holds no block:"),
            "{}",
            text(None)
        );
    }

    /// fjall's words name no cause, so the stop says where the cause is.
    #[test]
    fn a_failed_worker_says_where_its_cause_is_logged() {
        let message = Error::StorePoisoned(std::io::Error::other("poisoned").into()).to_string();
        assert!(message.contains("this log holds its error"), "{message}");
        assert!(message.contains("Worker #N crashed"), "{message}");
        assert!(message.contains("persist failed"), "{message}");
        assert!(message.contains("Check both, then rerun"), "{message}");
        assert!(message.contains(IF_THE_STATE_PASSED), "{message}");
    }

    #[test]
    fn a_failed_fetch_says_what_its_volume_holds() {
        let dir = tempfile::tempdir().unwrap();
        let reason = fetch_failure(&"All locations failed", dir.path());
        assert!(reason.starts_with("All locations failed. "), "{reason}");
        assert!(
            reason.contains(&dir.path().display().to_string()),
            "{reason}"
        );
        assert!(reason.contains("bytes free"), "{reason}");
    }

    /// Most failed fetches have nothing to do with the disk, so the words leave
    /// that to the figure.
    #[test]
    fn a_failed_fetch_doesnt_blame_a_volume_it_cant_see_is_full() {
        let dir = tempfile::tempdir().unwrap();
        let reason = fetch_failure(&"certificate does not match", dir.path());
        assert!(
            reason.ends_with("bytes free: a low figure can mean a full volume"),
            "{reason}"
        );
    }

    /// With no figure to show, the words can't rule a full volume out, and the
    /// client's own error still leads.
    #[test]
    fn a_failed_fetch_says_when_it_cant_read_the_free_space() {
        let dir = Path::new("relative-with-no-ancestor");
        let reason = fetch_failure(&"All locations failed", dir);
        assert!(reason.starts_with("All locations failed. "), "{reason}");
        assert!(
            reason.ends_with("so a full volume can't be ruled out"),
            "{reason}"
        );
    }

    #[test]
    fn a_run_spools_under_the_scratch_directory_it_is_given() {
        let storage = StorageConfig {
            path: PathBuf::from("/data"),
            ..Default::default()
        };
        let storage = &storage;
        let options = |scratch_dir: Option<&str>| Options {
            direct_s3: None,
            repository: "oci://registry.invalid/cardano/preview".parse().unwrap(),
            auth: Auth::Anonymous,
            insecure: false,
            scratch_dir: scratch_dir.map(PathBuf::from),
            concurrency: None,
            monolithic_max: None,
            download_dir: None,
            window: 1,
            until_epoch: None,
            skip_validation: true,
            mithril: MithrilSource {
                aggregator: String::new(),
                genesis_vkey: String::new(),
            },
            limits: Limits::ONE,
        };
        assert_eq!(options(Some("/spool")).spool(storage), Path::new("/spool"));
        assert_eq!(options(None).spool(storage), storage.path.join("scratch"));
    }

    #[test]
    fn simultaneous_work_and_finish_failures_are_preserved() {
        let error =
            finish_both::<()>(Err(Error::Interrupted), Err(Error::EmptyWindow)).unwrap_err();
        assert!(matches!(error, Error::ReplayAndShutdown { .. }));
    }

    #[test]
    fn synchronous_entrypoint_refuses_an_entered_tokio_runtime() {
        assert!(require_synchronous_context().is_ok());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            assert!(matches!(
                require_synchronous_context(),
                Err(Error::AsyncRuntimeContext)
            ));
        });
    }
}
