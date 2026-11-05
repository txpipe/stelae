//! The durable record of a multi-boundary batch.
//!
//! Replay may stand past the repository's latest stele only while every
//! sequence in between is held here: a complete bundle and a journal entry
//! naming it. The journal is written after the stores that reached a boundary
//! closed, so the frontier it names is one the stores really hold, and a cursor
//! behind it means they were rebuilt or rolled back.

use std::{
    fs::{self, File},
    io::{self, Write as _},
    num::NonZeroU64,
    path::{Path, PathBuf},
};

use dolos_core::{BlockSlot, ChainPoint};
use dolos_snapshot::{
    export::Predecessor,
    facade::SnapshotSource,
    prepare::{self, Bundle, Store},
    staging::{Budget, Staged},
};
use serde::{Deserialize, Serialize};
use stelae::{progress::Observer, Digest};
use stelae_driver::preflight::Need;

/// Version of the journal's layout.
pub const JOURNAL_FORMAT: u32 = 1;

const BATCH_DIR: &str = ".stelae-batch";
const JOURNAL_FILE: &str = "journal.json";
const PREPARED_DIR: &str = "prepared";
const STAGING_DIR: &str = "staging";

/// Where a publisher keeps its batch. Inside the storage path because it is
/// recovery data for those stores, and clearing them has to take it too.
pub fn dir_in(storage_path: &Path) -> PathBuf {
    storage_path.join(BATCH_DIR)
}

/// The controls bounding a batch. The epoch count and the scratch budget are
/// independent: whichever is met first closes the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_epochs: NonZeroU64,
    /// Bytes this batch may hold on disk: its bundles and its staged runs.
    pub scratch: u64,
    /// Bytes kept free per prepared boundary for staging and finalizing it.
    pub reserve_per_epoch: u64,
}

impl Limits {
    /// A batch of one captures nothing: it publishes before it advances.
    pub const ONE: Self = Self {
        max_epochs: NonZeroU64::MIN,
        scratch: 0,
        reserve_per_epoch: 0,
    };

    pub fn is_one(&self) -> bool {
        self.max_epochs == NonZeroU64::MIN
    }
}

/// What a batch's bundles were prepared from. A later run differing in any of
/// this would continue the batch from something else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub repository: String,
    pub network_magic: u64,
    pub store_contract: String,
    pub mutable_slots: u64,
    pub retained: Vec<u64>,
    pub dolos_revision: String,
}

/// A stele the repository really holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Committed {
    pub sequence: u64,
    pub identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Point {
    pub slot: BlockSlot,
    pub hash: Option<String>,
}

impl From<&ChainPoint> for Point {
    fn from(point: &ChainPoint) -> Self {
        Self {
            slot: point.slot(),
            hash: point.hash().map(|hash| hash.to_string()),
        }
    }
}

/// One prepared boundary, as far as the journal vouches for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub sequence: u64,
    pub point: Point,
    /// What the bundle holds, stable across a recapture of the same rows.
    pub bundle: String,
    pub bytes: u64,
    pub horizon: Option<BlockSlot>,
}

/// Why a batch stopped growing. Only captured boundaries are flushed: none of
/// these manufactures one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Flush {
    EpochLimit,
    UntilEpoch,
    ScratchBudget,
    DiskPressure,
    EndOfSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    format: u32,
    source: Source,
    /// The repository's latest stele when the batch began: `None` for an empty
    /// repository. What the first publication has to follow.
    predecessor: Option<Committed>,
    prepared: Vec<Prepared>,
    replay: Point,
    staged: Option<Point>,
    /// The oldest slot any bundle needs the archive to keep.
    horizon: Option<BlockSlot>,
    ready: Option<Flush>,
}

/// The three frontiers an operator tells a batch's progress by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frontiers {
    pub published: Option<u64>,
    pub prepared: Option<u64>,
    pub staged: Option<Point>,
    pub replay: Option<Point>,
    /// The oldest slot the archive still has to hold for this batch.
    pub horizon: Option<BlockSlot>,
}

/// What may happen next, before replay advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Advance,
    Flush(Flush),
    /// Not even one boundary fits. Nothing is lost, and a rerun with room
    /// resumes.
    NoRoom {
        needed: u64,
        used: u64,
    },
}

/// Where the stores stand against the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// In the epoch of the last prepared boundary, at or past it.
    Within,
    /// In the epoch after it, so on a boundary that is not captured yet if
    /// anywhere.
    NextEpoch,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("batch journal at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("batch journal at {path} is unreadable ({reason}). {REPLAY}")]
    Corrupt { path: PathBuf, reason: String },
    #[error(
        "the batch at {path} was prepared from another {what}: run the build and configuration \
         that prepared it. {REPLAY}"
    )]
    SourceChanged { path: PathBuf, what: &'static str },
    #[error("sequence {sequence} of the batch at {path} can't be used: {source}. {REPLAY}")]
    BundleLost {
        sequence: u64,
        path: PathBuf,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },
    #[error(
        "sequence {sequence} of the batch at {path} holds other layers than it recorded. {REPLAY}"
    )]
    BundleChanged { sequence: u64, path: PathBuf },
    #[error(
        "the batch at {path} reached slot {frontier}, and the stores stand at {cursor:?}: they \
         were rebuilt or rolled back. {REPLAY}"
    )]
    StoresBehind {
        frontier: BlockSlot,
        cursor: Option<BlockSlot>,
        path: PathBuf,
    },
    #[error(
        "the stores stand in epoch {epoch}, and the batch at {path} last prepared sequence \
         {last}: nothing holds the sequences between them. {REPLAY}"
    )]
    Unrepresented {
        epoch: u64,
        last: u64,
        path: PathBuf,
    },
    #[error(
        "the repository's latest stele is {found:?}, and the batch at {path} follows {planned:?}: \
         another writer moved it. Stop that writer. {REPLAY}"
    )]
    PredecessorMoved {
        planned: Option<Committed>,
        found: Option<Committed>,
        path: PathBuf,
    },
    #[error(
        "a batch of {prepared} prepared boundaries is pending at {path}: rerun with the \
         --batch-epochs that prepared it"
    )]
    Pending { prepared: usize, path: PathBuf },
    #[error("preparing a boundary")]
    Preparing(#[source] Box<dolos_snapshot::Error>),
}

/// The one recovery from losing what only a batch holds.
const REPLAY: &str = "Nothing else holds those boundaries: restore the stores from the \
                      repository's latest stele and delete the batch to replay them";

fn io_at(path: &Path) -> impl FnOnce(io::Error) -> Error + '_ {
    move |source| Error::Io {
        path: path.to_owned(),
        source,
    }
}

fn read_journal(path: &Path) -> Result<Option<Journal>, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_at(path)(error)),
    };
    let corrupt = |reason: String| Error::Corrupt {
        path: path.to_owned(),
        reason,
    };
    let journal: Journal =
        serde_json::from_slice(&bytes).map_err(|error| corrupt(error.to_string()))?;
    if journal.format != JOURNAL_FORMAT {
        return Err(corrupt(format!(
            "format {}, this build reads {JOURNAL_FORMAT}",
            journal.format
        )));
    }
    Ok(Some(journal))
}

/// Refuse to run or publish past a pending batch. A one-boundary path can't
/// consume it, and the advanced cursor is no permission to skip its sequences.
pub fn refuse_pending(storage_path: &Path) -> Result<(), Error> {
    let path = dir_in(storage_path).join(JOURNAL_FILE);
    match read_journal(&path)? {
        Some(journal) if !journal.prepared.is_empty() => Err(Error::Pending {
            prepared: journal.prepared.len(),
            path,
        }),
        _ => Ok(()),
    }
}

/// What a bundle holds: every layer's descriptor, and the boundary it stands
/// on.
fn bundle_identity(bundle: &Bundle) -> String {
    let layers: Vec<_> = bundle.layers().collect();
    // Plain data with string keys, which always serializes.
    let bytes = serde_json::to_vec(&(Point::from(bundle.cursor()), layers)).unwrap_or_default();
    Digest::compute(bytes).to_hex()
}

fn tree_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0;
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;
        total += match meta.is_dir() {
            true => tree_bytes(&entry.path())?,
            false => meta.len(),
        };
    }
    Ok(total)
}

/// One publisher's batch: its journal and the bundles it vouches for.
pub struct Batch {
    dir: PathBuf,
    limits: Limits,
    source: Source,
    store: Store,
    journal: Option<Journal>,
    /// Pinned when a batch begins, and journaled with its first boundary.
    predecessor: Option<Committed>,
}

impl Batch {
    /// Open the batch under `dir`, refusing one prepared from another source.
    pub fn open(dir: PathBuf, limits: Limits, source: Source) -> Result<Self, Error> {
        let journal = read_journal(&dir.join(JOURNAL_FILE))?;
        if let Some(journal) = &journal {
            let what = differing(&journal.source, &source);
            if let Some(what) = what {
                return Err(Error::SourceChanged { path: dir, what });
            }
        }
        let store = Store::open(dir.join(PREPARED_DIR), source.repository.clone())
            .map_err(|error| Error::Preparing(Box::new(error)))?;
        Ok(Self {
            predecessor: journal.as_ref().and_then(|j| j.predecessor.clone()),
            dir,
            limits,
            source,
            store,
            journal,
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn source(&self) -> &Source {
        &self.source
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn is_empty(&self) -> bool {
        self.journal.as_ref().is_none_or(|j| j.prepared.is_empty())
    }

    pub fn ready(&self) -> Option<Flush> {
        self.journal.as_ref().and_then(|journal| journal.ready)
    }

    pub fn last_sequence(&self) -> Option<u64> {
        let journal = self.journal.as_ref()?;
        journal.prepared.last().map(|prepared| prepared.sequence)
    }

    pub fn prepared(&self) -> &[Prepared] {
        self.journal.as_ref().map_or(&[], |j| &j.prepared)
    }

    /// What the batch holds on disk, staged runs included.
    pub fn used(&self) -> Result<u64, Error> {
        tree_bytes(&self.dir).map_err(io_at(&self.dir))
    }

    pub fn frontiers(&self) -> Frontiers {
        let journal = self.journal.as_ref();
        Frontiers {
            published: self.predecessor.as_ref().map(|c| c.sequence),
            prepared: self.last_sequence(),
            staged: journal.and_then(|journal| journal.staged.clone()),
            replay: journal.map(|journal| journal.replay.clone()),
            horizon: journal.and_then(|journal| journal.horizon),
        }
    }

    /// Pin what the batch about to begin follows.
    pub fn begin(&mut self, predecessor: Option<Committed>) {
        self.predecessor = predecessor;
    }

    /// Refuse a repository that moved since the batch began.
    pub fn check_predecessor(&self, latest: Option<Committed>) -> Result<(), Error> {
        if latest == self.predecessor {
            return Ok(());
        }
        Err(Error::PredecessorMoved {
            planned: self.predecessor.clone(),
            found: latest,
            path: self.dir.clone(),
        })
    }

    /// Read where the stores stand against the journal.
    pub fn position(&self, cursor: Option<&ChainPoint>, epoch: u64) -> Result<Position, Error> {
        let (Some(journal), Some(last)) = (&self.journal, self.last_sequence()) else {
            return Ok(Position::Within);
        };
        let slot = cursor.map(ChainPoint::slot);
        // Slots only: a Byron main block shares its slot with the boundary block
        // before it, and finalization holds the bundle's block to the archive.
        if slot.is_none_or(|slot| slot < journal.replay.slot) || epoch < last {
            return Err(Error::StoresBehind {
                frontier: journal.replay.slot,
                cursor: slot,
                path: self.dir.clone(),
            });
        }
        match epoch - last {
            0 => Ok(Position::Within),
            1 => Ok(Position::NextEpoch),
            _ => Err(self.unrepresented(epoch)),
        }
    }

    /// The gap the cursor stands across, for a caller that found no boundary
    /// there either.
    pub fn unrepresented(&self, epoch: u64) -> Error {
        Error::Unrepresented {
            epoch,
            last: self.last_sequence().unwrap_or_default(),
            path: self.dir.clone(),
        }
    }

    /// Hold every journaled bundle to its record, as read from disk. Payload
    /// contents are checked again as they are read.
    pub fn bundles(&self) -> Result<Vec<Bundle>, Error> {
        self.prepared()
            .iter()
            .map(|prepared| {
                let sequence = prepared.sequence;
                let bundle = self
                    .store
                    .load(sequence)
                    .map_err(|source| Error::BundleLost {
                        sequence,
                        path: self.dir.clone(),
                        source: Box::new(source),
                    })?;
                let found = bundle_identity(&bundle);
                if found != prepared.bundle {
                    return Err(Error::BundleChanged {
                        sequence,
                        path: self.dir.clone(),
                    });
                }
                Ok(bundle)
            })
            .collect()
    }

    fn save(&self) -> Result<(), Error> {
        let path = self.dir.join(JOURNAL_FILE);
        let partial = self.dir.join(format!("{JOURNAL_FILE}.partial"));
        let bytes =
            serde_json::to_vec_pretty(&self.journal).map_err(|error| io_at(&path)(error.into()))?;
        let write = || -> io::Result<()> {
            let mut file = File::create(&partial)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&partial, &path)?;
            sync_dir(&self.dir)
        };
        write().map_err(io_at(&path))
    }

    /// Journal a complete bundle, once the stores that reached it are durable.
    /// Replay advances only after this returns.
    pub fn record(&mut self, bundle: &Bundle) -> Result<(), Error> {
        if let Some(last) = self
            .last_sequence()
            .filter(|last| last + 1 != bundle.sequence())
        {
            return Err(Error::Unrepresented {
                epoch: bundle.sequence(),
                last,
                path: self.dir.clone(),
            });
        }
        let prepared = Prepared {
            sequence: bundle.sequence(),
            point: bundle.cursor().into(),
            bundle: bundle_identity(bundle),
            bytes: bundle.bytes(),
            horizon: bundle.horizon(),
        };
        let journal = self.journal.get_or_insert_with(|| Journal {
            format: JOURNAL_FORMAT,
            source: self.source.clone(),
            predecessor: self.predecessor.clone(),
            prepared: Vec::new(),
            replay: prepared.point.clone(),
            staged: None,
            horizon: None,
            ready: None,
        });
        journal.replay = prepared.point.clone();
        journal.horizon = match (journal.horizon, prepared.horizon) {
            (Some(held), Some(new)) => Some(held.min(new)),
            (held, new) => held.or(new),
        };
        journal.prepared.push(prepared);
        self.save()
    }

    /// Close the batch at the frontier the stores really reached. Durable, so a
    /// restart finds it closed rather than growing it past what was decided.
    pub fn close(&mut self, flush: Flush, cursor: Option<&ChainPoint>) -> Result<(), Error> {
        let Some(journal) = self.journal.as_mut() else {
            return Ok(());
        };
        journal.ready = Some(flush);
        if let Some(cursor) = cursor {
            journal.replay = cursor.into();
        }
        self.save()
    }

    /// What may happen before replay leaves the current boundary, given what
    /// the batch holds on disk and `room`, whether the volume has that much
    /// free.
    pub fn admit(
        &self,
        until_reached: bool,
        room: &dyn Fn(u64) -> bool,
    ) -> Result<Admission, Error> {
        let held = self.prepared().len() as u64;
        if held >= self.limits.max_epochs.get() {
            return Ok(Admission::Flush(Flush::EpochLimit));
        }
        if until_reached {
            return Ok(Admission::Flush(Flush::UntilEpoch));
        }
        let used = self.used()?;
        let reserve = self.limits.reserve_per_epoch;
        let growth = self.prepared().iter().map(|p| p.bytes).max().unwrap_or(0);
        let refused = |needed: u64, last_resort: u64| -> Option<Admission> {
            let budget = used.saturating_add(needed) > self.limits.scratch;
            if !budget && room(needed) {
                return None;
            }
            Some(match (held <= last_resort, budget) {
                (true, _) => Admission::NoRoom { needed, used },
                (false, true) => Admission::Flush(Flush::ScratchBudget),
                (false, false) => Admission::Flush(Flush::DiskPressure),
            })
        };
        // What it holds has to finish, and then the next boundary has to fit.
        let standing = reserve.saturating_mul(held);
        let next = reserve.saturating_mul(held + 1).saturating_add(growth);
        Ok(refused(standing, 1)
            .or_else(|| refused(next, 0))
            .unwrap_or(Admission::Advance))
    }

    /// Stage what the batch's closed windows need, once, from a frozen view,
    /// inside what the budget has left.
    pub fn stage(
        &mut self,
        source: &dyn SnapshotSource,
        previous: &dyn Predecessor,
        observer: &Observer,
    ) -> Result<Staged, Error> {
        let bundles = self.bundles()?;
        let root = self.dir.join(STAGING_DIR);
        let used = tree_bytes(&self.dir.join(PREPARED_DIR)).map_err(io_at(&self.dir))?;
        let budget = Budget::new(self.limits.scratch.saturating_sub(used));
        let staged = source
            .stage_batch(&root, &bundles, previous, budget, observer)
            .map_err(|error| Error::Preparing(Box::new(error)))?;
        if let Some(journal) = self.journal.as_mut() {
            journal.staged = Some(staged.frontier().into());
        }
        self.save()?;
        Ok(staged)
    }
}

/// The first thing `found` differs from `held` in, for a refusal to name.
fn differing(held: &Source, found: &Source) -> Option<&'static str> {
    [
        ("repository", held.repository != found.repository),
        ("network", held.network_magic != found.network_magic),
        (
            "store contract",
            held.store_contract != found.store_contract,
        ),
        (
            "mutable slot count",
            held.mutable_slots != found.mutable_slots,
        ),
        ("retained list", held.retained != found.retained),
        (
            "Dolos revision",
            held.dolos_revision != found.dolos_revision,
        ),
    ]
    .into_iter()
    .find_map(|(what, differs)| differs.then_some(what))
}

/// Whether the volume holding `path` has `bytes` free, by the one free-space
/// policy: what can't be measured warns and admits.
pub fn volume_has_room(path: &Path, bytes: u64) -> bool {
    stelae_driver::preflight::check(&[Need::of("the next prepared boundary", path, bytes)]).is_ok()
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Windows cannot open a directory to sync it, so a crash may lose the rename:
/// the boundary is then captured again.
#[cfg(not(unix))]
fn sync_dir(_: &Path) -> io::Result<()> {
    Ok(())
}

/// Whether a capture failed because the volume is full, which is a reason to
/// flush what it holds rather than a failed run.
pub fn out_of_space(error: &dolos_snapshot::Error) -> bool {
    use dolos_snapshot::Error as E;

    let source = match error {
        E::Preparation(prepare::Refusal::Io(source)) => source,
        E::Stelae(stelae::Error::Io(source)) => source,
        _ => return false,
    };
    matches!(
        source.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Source {
        Source {
            repository: "oci://registry.invalid/cardano/preview".to_owned(),
            network_magic: 2,
            store_contract: "contract".to_owned(),
            mutable_slots: 129_600,
            retained: vec![1],
            dolos_revision: "revision".to_owned(),
        }
    }

    fn limits(max_epochs: u64, scratch: u64, reserve_per_epoch: u64) -> Limits {
        Limits {
            max_epochs: NonZeroU64::new(max_epochs).unwrap(),
            scratch,
            reserve_per_epoch,
        }
    }

    fn prepared(sequence: u64, bytes: u64) -> Prepared {
        Prepared {
            sequence,
            point: Point {
                slot: sequence * 100,
                hash: Some("aa".to_owned()),
            },
            bundle: format!("bundle-{sequence}"),
            bytes,
            horizon: Some(0),
        }
    }

    fn batch_holding(dir: &Path, limits: Limits, held: &[Prepared], used: u64) -> Batch {
        let mut batch = Batch::open(dir.to_owned(), limits, source()).unwrap();
        batch.journal = (!held.is_empty()).then(|| Journal {
            format: JOURNAL_FORMAT,
            source: source(),
            predecessor: None,
            prepared: held.to_vec(),
            replay: held[held.len() - 1].point.clone(),
            staged: None,
            horizon: Some(0),
            ready: None,
        });
        fs::write(dir.join("payload"), vec![0; used as usize]).unwrap();
        batch
    }

    #[test]
    fn the_epoch_limit_is_independent_of_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let held = [prepared(1, 1), prepared(2, 1)];
        let batch = batch_holding(dir.path(), limits(2, u64::MAX, 0), &held, 0);
        assert_eq!(
            batch.admit(false, &|_| true).unwrap(),
            Admission::Flush(Flush::EpochLimit)
        );
    }

    #[test]
    fn until_epoch_closes_a_batch_with_room_left() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch_holding(dir.path(), limits(9, u64::MAX, 0), &[prepared(1, 1)], 0);
        assert_eq!(
            batch.admit(true, &|_| true).unwrap(),
            Admission::Flush(Flush::UntilEpoch)
        );
    }

    #[test]
    fn the_budget_flushes_before_the_next_boundary_would_cross_it() {
        let dir = tempfile::tempdir().unwrap();
        let held = [prepared(1, 100), prepared(2, 100)];
        // Used 200; the next boundary is estimated at the largest yet, 100, and
        // three reserves of 10: 330 in all.
        let at = |scratch| {
            batch_holding(dir.path(), limits(9, scratch, 10), &held, 200)
                .admit(false, &|_| true)
                .unwrap()
        };
        assert_eq!(at(329), Admission::Flush(Flush::ScratchBudget));
        assert_eq!(at(330), Admission::Advance);
    }

    #[test]
    fn free_space_flushes_on_its_own_measure() {
        let dir = tempfile::tempdir().unwrap();
        let held = [prepared(1, 100), prepared(2, 100)];
        let batch = batch_holding(dir.path(), limits(9, u64::MAX, 10), &held, 200);
        // Two reserves to finish what it holds pass; 130 for the next do not.
        assert_eq!(
            batch.admit(false, &|needed| needed < 130).unwrap(),
            Admission::Flush(Flush::DiskPressure)
        );
        assert_eq!(
            batch.admit(false, &|needed| needed <= 130).unwrap(),
            Admission::Advance
        );
    }

    #[test]
    fn a_batch_that_cannot_hold_one_boundary_stops_instead_of_flushing() {
        let dir = tempfile::tempdir().unwrap();
        let empty = batch_holding(dir.path(), limits(9, 5, 10), &[], 0);
        assert!(matches!(
            empty.admit(false, &|_| true).unwrap(),
            Admission::NoRoom { needed: 10, .. }
        ));

        // One boundary already past the budget has nothing smaller to flush to.
        let dir = tempfile::tempdir().unwrap();
        let one = batch_holding(dir.path(), limits(9, 50, 10), &[prepared(1, 100)], 100);
        assert!(matches!(
            one.admit(false, &|_| true).unwrap(),
            Admission::NoRoom {
                needed: 10,
                used: 100
            }
        ));

        // One that fits but leaves no room for a second flushes to itself.
        let dir = tempfile::tempdir().unwrap();
        let fits = batch_holding(dir.path(), limits(9, 120, 10), &[prepared(1, 100)], 100);
        assert_eq!(
            fits.admit(false, &|_| true).unwrap(),
            Admission::Flush(Flush::ScratchBudget)
        );
    }

    #[test]
    fn a_shared_slot_past_the_boundary_is_within_its_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch_holding(dir.path(), limits(9, u64::MAX, 0), &[prepared(4, 1)], 0);
        // A Byron main block on the boundary block's slot: another hash, not
        // another chain.
        let main_block = ChainPoint::Specific(400, [0xbb; 32].into());
        assert_eq!(
            batch.position(Some(&main_block), 4).unwrap(),
            Position::Within
        );
    }

    #[test]
    fn the_cursor_is_held_to_the_journal() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch_holding(dir.path(), limits(9, u64::MAX, 0), &[prepared(4, 1)], 0);
        let point = |slot| ChainPoint::Specific(slot, [0xcc; 32].into());
        assert!(matches!(
            batch.position(Some(&point(399)), 4),
            Err(Error::StoresBehind { .. })
        ));
        assert!(matches!(
            batch.position(None, 4),
            Err(Error::StoresBehind { .. })
        ));
        assert!(matches!(
            batch.position(Some(&point(500)), 3),
            Err(Error::StoresBehind { .. })
        ));
        assert_eq!(
            batch.position(Some(&point(410)), 4).unwrap(),
            Position::Within
        );
        assert_eq!(
            batch.position(Some(&point(500)), 5).unwrap(),
            Position::NextEpoch
        );
        assert!(matches!(
            batch.position(Some(&point(600)), 6),
            Err(Error::Unrepresented {
                epoch: 6,
                last: 4,
                ..
            })
        ));
    }

    #[test]
    fn a_journal_round_trips_and_binds_its_source() {
        let dir = tempfile::tempdir().unwrap();
        let held = [prepared(1, 7)];
        let mut batch = batch_holding(dir.path(), limits(9, u64::MAX, 0), &held, 0);
        batch.close(Flush::EndOfSource, None).unwrap();

        let reopened = Batch::open(dir.path().to_owned(), Limits::ONE, source()).unwrap();
        assert_eq!(reopened.prepared(), &held);
        assert_eq!(reopened.ready(), Some(Flush::EndOfSource));

        for (what, change) in [
            (
                "repository",
                (|s| s.repository = "oci://other/x".to_owned()) as fn(&mut Source),
            ),
            ("network", |s| s.network_magic = 1),
            ("store contract", |s| s.store_contract = "other".to_owned()),
            ("mutable slot count", |s| s.mutable_slots = 1),
            ("retained list", |s| s.retained = vec![]),
            ("Dolos revision", |s| s.dolos_revision = "other".to_owned()),
        ] {
            let mut other = source();
            change(&mut other);
            let error = Batch::open(dir.path().to_owned(), Limits::ONE, other)
                .err()
                .unwrap();
            assert!(
                matches!(error, Error::SourceChanged { what: found, .. } if found == what),
                "{what}"
            );
        }
    }

    #[test]
    fn a_damaged_journal_is_refused_rather_than_read_as_none() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(JOURNAL_FILE), b"{\"format\":1").unwrap();
        assert!(matches!(
            Batch::open(dir.path().to_owned(), Limits::ONE, source()),
            Err(Error::Corrupt { .. })
        ));

        let dir = tempfile::tempdir().unwrap();
        let mut held = batch_holding(dir.path(), Limits::ONE, &[], 0);
        held.journal = Some(Journal {
            format: JOURNAL_FORMAT + 1,
            source: source(),
            predecessor: None,
            prepared: vec![],
            replay: Point {
                slot: 0,
                hash: None,
            },
            staged: None,
            horizon: None,
            ready: None,
        });
        held.save().unwrap();
        assert!(matches!(
            Batch::open(dir.path().to_owned(), Limits::ONE, source()),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn a_moved_repository_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut batch = batch_holding(dir.path(), limits(9, u64::MAX, 0), &[], 0);
        let at = |sequence| Committed {
            sequence,
            identity: format!("sha256:{sequence}"),
        };
        batch.begin(Some(at(3)));
        batch.check_predecessor(Some(at(3))).unwrap();
        for found in [
            Some(at(4)),
            None,
            Some(Committed {
                sequence: 3,
                identity: "other".to_owned(),
            }),
        ] {
            assert!(matches!(
                batch.check_predecessor(found),
                Err(Error::PredecessorMoved { .. })
            ));
        }
    }

    #[test]
    fn only_a_full_volume_counts_as_out_of_space() {
        use dolos_snapshot::Error as E;
        let refusal = |kind: io::ErrorKind| E::Preparation(prepare::Refusal::Io(kind.into()));
        assert!(out_of_space(&refusal(io::ErrorKind::StorageFull)));
        assert!(out_of_space(&refusal(io::ErrorKind::QuotaExceeded)));
        assert!(out_of_space(&E::Stelae(stelae::Error::Io(
            io::ErrorKind::StorageFull.into()
        ))));
        assert!(!out_of_space(&refusal(io::ErrorKind::PermissionDenied)));
        assert!(!out_of_space(&E::Preparation(prepare::Refusal::EmptyBatch)));
    }
}
