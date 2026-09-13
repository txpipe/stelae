//! Stelae-owned Cardano backfill state machine.
//!
//! Every iteration publishes the committed boundary before it advances,
//! prunes only after that publish succeeds, downloads bounded Mithril windows,
//! and closes the replay engine on every result. This is intentionally a
//! sequential relocation of the Dolos publisher loop at `DOLOS_REVISION`.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::{
    config::{MithrilConfig, RootConfig},
    BlockSlot, ChainPoint, Genesis, RawBlock, ReplayProgress,
};
use dolos_mithril as mithril;
use dolos_mithril::mithril_client::feedback::FeedbackReceiver;
use dolos_snapshot::{export::Plan, planning, source::SnapshotSource};
use itertools::Itertools as _;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::publisher::{BackfillPublisher, RepositoryPublish};

const IMPORT_CHUNK: usize = 100;
const IMMUTABLE_FILE_MARGIN: u64 = 2;
const SLOTS_PER_SECURITY_PARAM: u64 = 10;
const FALLBACK_SLOTS_PER_IMMUTABLE_FILE: u64 = 21_600;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Caller(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("reading snapshot.state_epochs")]
    RetainedEpochs(#[source] dolos_snapshot::Error),
    #[error("planning the publish")]
    Planning(#[source] dolos_snapshot::Error),
    #[error("replay failed ({replay}) and shutting down also failed ({shutdown})")]
    ReplayAndShutdown {
        replay: Box<Error>,
        shutdown: Box<Error>,
    },
    #[error("iterating the local immutable db: {0}")]
    ImmutableDb(String),
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
    #[error("state cursor at slot {slot} has no block hash, cannot walk the immutable db from it")]
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
}

impl Error {
    pub fn caller(source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
        Self::Caller(source.into())
    }
}

pub trait Workspace {
    type Session: Session;
    fn snapshot(&self) -> impl SnapshotSource + '_;
    fn start(self, target_epoch: u64) -> Result<Self::Session, Error>;
    fn finish(self) -> Result<(), Error>;
}

pub trait Session {
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
}

impl Replay for () {}

pub trait Publish {
    fn announce(&self, plan: &Plan) -> Result<(), Error> {
        let _ = plan;
        Ok(())
    }
    fn publish(&self, plan: &Plan, source: &dyn SnapshotSource) -> Result<(), Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    UntilEpoch { sequence: u64 },
    UpToDate,
}

enum Step {
    Extend { target: u64, prune: bool },
    Done { sequence: u64 },
}
enum Advance {
    Boundary { cursor_slot: u64 },
    MithrilExhausted,
    Cancelled,
}
enum Import {
    Boundary,
    Exhausted,
    Cancelled,
}

fn slots_per_immutable_file(genesis: &Genesis) -> u64 {
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

fn cleanup_consumed(immutable_dir: &Path, cursor_slot: u64, width: u64) -> Result<(), Error> {
    let threshold = consumed_below(cursor_slot, width);
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

pub struct Driver<'a, W: Workspace> {
    pub config: &'a RootConfig,
    pub genesis: &'a Genesis,
    pub mithril: &'a MithrilConfig,
    pub download_dir: PathBuf,
    pub window: u64,
    pub until_epoch: Option<u64>,
    pub skip_validation: bool,
    pub runtime: tokio::runtime::Handle,
    pub cancel: CancellationToken,
    pub mithril_feedback: &'a dyn Fn() -> Option<Arc<dyn FeedbackReceiver>>,
    pub replay: &'a dyn Replay,
    pub open_workspace: &'a dyn Fn() -> Result<W, Error>,
    pub publish: &'a dyn Publish,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub repository: dolos_snapshot::registry::Repository,
    pub insecure: bool,
    pub scratch_dir: Option<PathBuf>,
    pub concurrency: Option<std::num::NonZeroUsize>,
    pub verify_carried: bool,
    pub download_dir: Option<PathBuf>,
    pub window: u64,
    pub until_epoch: Option<u64>,
    pub skip_validation: bool,
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
    require_synchronous_context()?;
    let mithril = config.mithril.as_ref().ok_or_else(|| {
        Error::caller(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "missing mithril config",
        ))
    })?;
    let download_dir = options
        .download_dir
        .clone()
        .unwrap_or_else(|| config.storage.path.join("mithril"));
    std::fs::create_dir_all(&download_dir).map_err(Error::caller)?;
    let runtime = tokio::runtime::Runtime::new().map_err(Error::caller)?;
    let settings = RepositoryPublish {
        repo: &options.repository,
        insecure: options.insecure,
        scratch_dir: options.scratch_dir.as_deref(),
        rebuild: false,
        tuning: dolos_snapshot::registry::Tuning {
            concurrency: options.concurrency,
            verify_adopted: options.verify_carried,
        },
    };
    let publisher = BackfillPublisher {
        config,
        settings,
        observer,
    };
    let genesis = Arc::new(genesis.clone());
    let driver = Driver::<ReplayWorkspace> {
        config,
        genesis: &genesis,
        mithril,
        download_dir,
        window: options.window,
        until_epoch: options.until_epoch,
        skip_validation: options.skip_validation,
        runtime: runtime.handle().clone(),
        cancel,
        mithril_feedback: &|| None,
        replay: &(),
        open_workspace: &|| ReplayWorkspace::open(config, genesis.clone()).map_err(Error::caller),
        publish: &publisher,
    };
    driver.run()
}

fn finish_both<T>(work: Result<T, Error>, finish: Result<(), Error>) -> Result<T, Error> {
    match (work, finish) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(replay), Err(shutdown)) => Err(Error::ReplayAndShutdown {
            replay: Box::new(replay),
            shutdown: Box::new(shutdown),
        }),
    }
}

impl<W: Workspace> Driver<'_, W> {
    fn immutable_dir(&self) -> PathBuf {
        self.download_dir.join("immutable")
    }
    fn aborted(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn run(&self) -> Result<Outcome, Error> {
        if self.window == 0 {
            return Err(Error::EmptyWindow);
        }
        let width = slots_per_immutable_file(self.genesis);
        let immutable_dir = self.immutable_dir();
        info!(
            slots_per_immutable_file = width,
            "derived immutable chunk size from genesis"
        );

        loop {
            if self.aborted() {
                return Err(Error::Interrupted);
            }
            let (target, prune) = match self.publish_pending()? {
                Step::Done { sequence } => return Ok(Outcome::UntilEpoch { sequence }),
                Step::Extend { target, prune } => (target, prune),
            };
            match self.extend(target, prune, width)? {
                Advance::Boundary { cursor_slot } => {
                    cleanup_consumed(&immutable_dir, cursor_slot, width)?
                }
                Advance::MithrilExhausted => return Ok(Outcome::UpToDate),
                Advance::Cancelled => return Err(Error::Interrupted),
            }
        }
    }

    fn publish_pending(&self) -> Result<Step, Error> {
        let workspace = (self.open_workspace)()?;
        let result = self.plan_pending(&workspace.snapshot());
        finish_both(result, workspace.finish())
    }

    fn plan_pending(&self, source: &dyn SnapshotSource) -> Result<Step, Error> {
        let Some(cursor) = source.committed_position().map_err(Error::Planning)? else {
            return Ok(Step::Extend {
                target: target_epoch(None),
                prune: false,
            });
        };
        let epoch = source
            .epoch()
            .map_err(Error::Planning)?
            .ok_or(Error::UnanchoredCursor {
                slot: cursor.slot(),
            })?;
        if epoch == 0 {
            return Ok(Step::Extend {
                target: target_epoch(Some(epoch)),
                prune: false,
            });
        }
        let retained = planning::retained_epochs(self.config).map_err(Error::RetainedEpochs)?;
        let plan = source
            .plan(u64::from(self.genesis.network_magic()), retained)
            .map_err(Error::Planning)?;
        self.publish.announce(&plan)?;
        stelae_driver::retry::transient(
            "publishing the pending sequence",
            &|| self.aborted(),
            || self.publish.publish(&plan, source),
        )?;
        if self.until_epoch.is_some_and(|until| plan.sequence >= until) {
            return Ok(Step::Done {
                sequence: plan.sequence,
            });
        }
        Ok(Step::Extend {
            target: target_epoch(Some(epoch)),
            prune: true,
        })
    }

    fn extend(&self, target: u64, prune: bool, width: u64) -> Result<Advance, Error> {
        let workspace = (self.open_workspace)()?;
        let mut session = workspace.start(target)?;
        let result = self.advance_session(&mut session, prune, width);
        finish_both(result, session.finish())
    }

    fn advance_session(
        &self,
        session: &mut W::Session,
        prune: bool,
        width: u64,
    ) -> Result<Advance, Error> {
        let immutable_dir = self.immutable_dir();
        if prune {
            let rounds = session.prune_history()?;
            info!(rounds, "housekeeping drained after publication");
        }
        self.replay.round_started();
        let result = self.advance_loop(session, &immutable_dir, width);
        self.replay.round_finished();
        result
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
                    return Ok(Advance::Boundary { cursor_slot });
                }
                Import::Cancelled => return Ok(Advance::Cancelled),
                Import::Exhausted => {}
            }

            let Some(beacon) = stelae_driver::retry::transient(
                "listing mithril snapshots",
                &|| self.aborted(),
                || {
                    self.runtime.block_on(async {
                    tokio::select! {
                        result = mithril::latest_immutable_file(self.mithril) => result.map(Some),
                        _ = self.cancel.cancelled() => Ok(None),
                    }
                })
                },
            )
            .map_err(|error| Error::Mithril {
                what: "listing mithril snapshots",
                reason: error.to_string(),
            })?
            else {
                return Ok(Advance::Cancelled);
            };

            let highest = mithril::highest_existing_immutable(immutable_dir);
            let cursor_slot = session.committed_position()?.map(|point| point.slot());
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
                return Ok(Advance::MithrilExhausted);
            };
            let fetch = mithril::Fetch {
                download_dir: &self.download_dir,
                skip_validation: self.skip_validation,
                download_start: start,
                download_end: Some(end),
            };
            let fetched = stelae_driver::retry::transient(
                "fetching a mithril immutable window",
                &|| self.aborted(),
                || self.runtime.block_on(async {
                    tokio::select! {
                        result = mithril::fetch_snapshot(&fetch, self.mithril, (self.mithril_feedback)()) => result.map(Some),
                        _ = self.cancel.cancelled() => Ok(None),
                    }
                }),
            ).map_err(|error| Error::Mithril {
                what: "fetching a mithril immutable window",
                reason: error.to_string(),
            })?;
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
        use pallas::network::miniprotocols::Point;

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
        let mut blocks = pallas::interop::hardano::storage::immutable::read_blocks_from_point(
            immutable_dir,
            point.clone(),
        )
        .map_err(|error| Error::ImmutableDb(error.to_string()))?;
        if point != Point::Origin {
            blocks.next();
        }
        for batch in blocks.chunks(IMPORT_CHUNK).into_iter() {
            let batch: Vec<_> = batch
                .try_collect()
                .map_err(|error| Error::BlockData(error.to_string()))?;
            let progress = session.import_blocks(batch.into_iter().map(Arc::new).collect())?;
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
