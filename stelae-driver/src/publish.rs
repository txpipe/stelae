//! The chained-publish lifecycle against a repository.
//!
//! Everything a publisher does once it is standing in front of a *repository*
//! rather than a transport: what the new snapshot says about the one before it,
//! and what an interrupted attempt of the same publish left behind. Generic
//! over [`DriverProfile`][crate::DriverProfile] throughout — every document
//! that crosses this module is composed by a profile and only compared here.
//!
//! What is *not* here is where any of it lives on a host: a resumption record's
//! path is handed in, never derived. A node's storage layout is the profile's.
//!
//! A publish never re-lists a layer of an earlier snapshot: it uploads and
//! lists the layers its profile assigns to its own sequence. See the
//! profile-side module that wraps this for the chain-or-refuse contract and
//! what an interrupted publish leaves behind.

use std::{
    collections::BTreeMap,
    io::Write as _,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use serde::{Deserialize, Serialize};
use stelae::{
    inscription::{Compression, LayerDescriptor, Link},
    oci::{Auth, Options, Registry, Repository, Snapshot, DEFAULT_CONCURRENCY},
    transport::WrittenLayer,
    Digest, SnapshotReader as _,
};

use crate::{scope_key, DriverProfile, Error, Predecessor, Role, Standing};

/// Open a repository in a registry.
///
/// Here rather than at the call site so a node's binary keeps never naming the
/// protocol crate — the same property a profile's own publish and restore entry
/// points hold for a directory.
///
/// `insecure` speaks plaintext HTTP. It is for a registry on a loopback address
/// or a mirror inside a cluster, and for nothing that is reachable from outside
/// one.
///
/// `auth` is who to authenticate as, decided by the caller. A node resolves it
/// from its own configuration and environment; nothing here goes looking, for
/// the reason `stelae::oci` states one layer down and this crate has no more
/// standing to override than that one does.
///
/// `scratch_dir` is where layers are staged, in both directions. The transport
/// creates it when the first layer needs it, so it need not exist yet.
///
/// `tuning` is how the publish path moves its layers; [`Tuning::default`] is
/// what every caller that is not publishing wants.
///
/// **Never call any of this from inside an async context.** The transport owns
/// a runtime and enters it with `block_on`; `stelae::oci`'s module
/// documentation states the rule and the reason.
pub fn open(
    repository: &Repository,
    insecure: bool,
    auth: Auth,
    scratch_dir: PathBuf,
    tuning: Tuning,
) -> Result<Registry, Error> {
    Ok(Registry::open(
        repository,
        options(insecure, auth, scratch_dir, tuning),
    )?)
}

/// The transport options [`open`] passes, apart so a test can read them.
fn options(insecure: bool, auth: Auth, scratch_dir: PathBuf, tuning: Tuning) -> Options {
    Options {
        direct_s3: tuning.direct_s3,
        insecure,
        scratch_dir: Some(scratch_dir),
        auth,
        concurrency: tuning.concurrency.map_or(DEFAULT_CONCURRENCY, Into::into),
        // Not an operator's knob and deliberately not one: the number that
        // absorbs a registry's transient `5xx` is the transport's own
        // measurement, and an outage longer than it is answered a level up,
        // where `stelae-publisher backfill` re-runs the whole publish rather than
        // dying into a pod restart.
        attempts: stelae::oci::DEFAULT_ATTEMPTS,
        // The read budget, for the same reason: it is sized to the
        // transfers a restore loses, not to how fast it should go.
        read_attempts: stelae::oci::DEFAULT_READ_ATTEMPTS,
        read_delay: stelae::oci::DEFAULT_READ_DELAY,
        // Nor these, for a related reason: both are facts about the
        // registry this publishes to and about the pod the publisher runs
        // in, measured rather than chosen, and neither moves when an
        // operator changes how fast a publish goes. `upload_memory` in
        // particular is what keeps `--concurrency` from being a claim on
        // memory — raising one does not raise the other.
        monolithic_max: stelae::oci::DEFAULT_MONOLITHIC_MAX,
        upload_memory: stelae::oci::DEFAULT_UPLOAD_MEMORY,
    }
}

/// What an operator may set about *how* a publish moves, as against where it
/// goes.
///
/// Separated from [`open`]'s other arguments because it is the only one of them
/// a caller can leave alone: a repository, a credential and a staging directory
/// are facts a publish cannot be run without, and these two are a default and
/// an escape hatch. A restore or an inspection passes [`Tuning::default`] and
/// means it.
#[derive(Debug, Clone, Default)]
pub struct Tuning {
    /// Direct S3 uploads for Zot-compatible blob storage.
    pub direct_s3: Option<Box<stelae::oci::s3_upload::Config>>,
    /// How many layer round trips run at once; `None` is
    /// [`DEFAULT_CONCURRENCY`].
    pub concurrency: Option<NonZeroUsize>,
}

/// Where a publish is going, and what the host running it knows about itself.
///
/// The publish-side counterpart of a restore's node-side facts, and it holds
/// the transport for the same reason that one holds a storage path: these are
/// the facts a publish is *given*, as against the ones it derives. Threading
/// them separately is what took [`publish`] to the edge of its signature, and
/// they have never been supplied from different places.
///
/// The record's path is owned rather than borrowed, and the type is `Clone`
/// rather than `Copy` because of it. Both are deliberate: a builder that took a
/// `&Path` would accept a node's *storage directory* as readily as its record,
/// silently, and the two are one `join` apart.
#[derive(Clone)]
pub struct Publishing<'a> {
    /// The repository being published into, already opened.
    pub registry: &'a Registry,

    /// Where the resumption record is kept, handed in rather than derived:
    /// a node's storage layout is the profile's and this crate never composes
    /// one. `None` for a caller with no node behind it, which records nothing
    /// and resumes nothing.
    pub record_path: Option<PathBuf>,

    /// The operator's `--rebuild`: build every layer, resume none, and start
    /// the record over.
    pub rebuild: bool,

    /// What the caller already holds this publish to follow: a snapshot, or
    /// inside it `None` for an empty repository. Outer `None` follows
    /// whatever is there.
    pub follows: Option<Option<(u64, Digest)>>,
}

impl<'a> Publishing<'a> {
    /// A publish into `registry` that keeps no record and rebuilds nothing.
    pub fn new(registry: &'a Registry) -> Self {
        Self {
            registry,
            record_path: None,
            rebuild: false,
            follows: None,
        }
    }

    /// The same publish, refused before anything is uploaded unless the
    /// repository's latest snapshot is `predecessor`.
    pub fn following(self, predecessor: Option<(u64, Digest)>) -> Self {
        Self {
            follows: Some(predecessor),
            ..self
        }
    }

    /// The same publish, recording what it finishes at `record_path`.
    pub fn recording_in(self, record_path: impl Into<PathBuf>) -> Self {
        Self {
            record_path: Some(record_path.into()),
            ..self
        }
    }

    /// The same publish, with the operator's `--rebuild`.
    pub fn rebuilding(self, rebuild: bool) -> Self {
        Self { rebuild, ..self }
    }
}

/// What a record has to agree with before a single layer in it is resumed.
///
/// Everything a recorded layer's bytes and address depend on that is *not* in
/// the layer's own key. The key is the kind plus the descriptor scope, and that
/// scope names an epoch and a slot window and nothing else — so:
///
/// - **the repository**, because a blob digest is an address in one repository
///   and means nothing in another;
/// - **the dataset**, because one sequence of two different datasets is two
///   different sets of bytes under one key. A descriptor scope names a window
///   and not a dataset — the layer's own header record carries that — so this
///   is the only place the record can hold it;
/// - **the parameters and the compression**, which are the inscription's own
///   statement of how its layers were built. A binary that changed either would
///   rebuild a recorded layer into different bytes, and the record would be
///   offering an answer to a question nobody is asking any more.
///
/// A mismatch in any of them makes the record a fresh one for the origin at
/// hand. Nothing is repaired and nothing is merged: the layers it named are
/// still in the registry, and the publish that wants them will build them
/// again and find them there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Origin {
    /// The repository the layers were uploaded to, as the transport names it.
    pub repository: String,
    /// The dataset the layers were built from, as the profile identifies it.
    ///
    /// Opaque here: this crate compares it and never reads it. A profile that
    /// numbers its datasets one way and a profile that numbers them another
    /// both fit, which is the whole reason the field is not named after
    /// either.
    pub dataset_id: u64,
    /// The inscription parameters the layers were built under.
    pub parameters: serde_json::Value,
    /// The compression they were built with.
    pub compression: Compression,
}

impl Origin {
    /// What a publish into `registry` of the dataset `dataset_id` identifies,
    /// under `parameters` and `compression`, records under.
    ///
    /// Rebuilt from parts rather than read off a plan: every one of them is the
    /// profile's own statement about the publish.
    pub fn of(
        registry: &Registry,
        dataset_id: u64,
        parameters: serde_json::Value,
        compression: Compression,
    ) -> Self {
        Self {
            repository: registry.repository().to_string(),
            dataset_id,
            parameters,
            compression,
        }
    }

    /// The fields `self` and `other` disagree on, in the order [`Origin`]
    /// states them.
    ///
    /// One of the four ways to differ changes the repository, so a refusal
    /// that reported the two repository names alone would read, in the other
    /// three, as though the two publishes matched. Names rather than values:
    /// `parameters` is arbitrary JSON, and what an operator needs from the
    /// event is which knob moved between the two runs.
    pub fn differences_from(&self, other: &Self) -> Vec<&'static str> {
        [
            (self.repository != other.repository, "repository"),
            (self.dataset_id != other.dataset_id, "dataset id"),
            (self.parameters != other.parameters, "parameters"),
            (self.compression != other.compression, "compression"),
        ]
        .into_iter()
        .filter_map(|(differs, field)| differs.then_some(field))
        .collect()
    }
}

/// The `diff` layers an interrupted publish got as far as uploading.
///
/// Written after each layer's upload succeeds and deleted once the snapshot is
/// sealed, so a record that exists describes a publish that did not finish. See
/// the module documentation for what it is and — more to the point — what it is
/// not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PublishRecord {
    pub origin: Origin,

    /// The layers whose blobs are up, each as the transport measured it.
    ///
    /// The whole [`WrittenLayer`] rather than the digest pair the resumption
    /// needs: the descriptor is what the new manifest has to state about the
    /// layer, and a record that held only its identity would have to invent the
    /// rest. In the canonical order of their keys, so the same progress is the
    /// same bytes.
    pub layers: Vec<WrittenLayer>,
}

impl PublishRecord {
    /// Read the record at `path`, or `None` if there is none.
    ///
    /// **Only absence is `None`**, on a restore's progress file's reasoning
    /// turned around: a file that exists and does not parse is an
    /// error rather than an empty resume, because reading it as "nothing has
    /// been uploaded" silently costs the rebuild this file exists to avoid.
    /// `--rebuild` is how an operator asks for that outcome on purpose.
    pub fn load(path: &Path) -> Result<Option<Self>, Error> {
        let raw = match read_file(path) {
            Ok(raw) => raw,
            Err(e) => return Err(Error::Stelae(e)),
        };

        let Some(raw) = raw else {
            return Ok(None);
        };

        Ok(Some(
            serde_json::from_slice(&raw).map_err(|e| Error::Stelae(e.into()))?,
        ))
    }

    /// Delete the record at `path`. A file that is not there is not an error.
    pub fn remove(path: &Path) -> Result<(), Error> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Stelae(e.into())),
        }
    }

    /// Write the record at `path`, atomically.
    ///
    /// Through a temporary sibling and a rename, for the reason
    /// `stelae::plan::RestoreProgress::save` states on its side: the failure
    /// this file exists to survive is a process that stops mid-write, and a
    /// half-written record would be refused by [`PublishRecord::load`] —
    /// correctly, and uselessly, since the publish it described would then have
    /// to start over.
    pub fn save(&self, path: &Path) -> Result<(), Error> {
        save_atomically(
            path,
            &serde_json::to_vec(self).map_err(stelae::Error::from)?,
        )
        .map_err(Error::Stelae)
    }

    /// The layers this record offers a publish at `sequence`, keyed the way
    /// [`Chained`] looks them up.
    ///
    /// An [`Origin`] the caller is not publishing under offers nothing: see
    /// [`Origin`] for why a mismatch is a fresh start rather than a merge.
    pub fn table(
        &self,
        origin: &Origin,
        profile: &dyn DriverProfile,
        sequence: u64,
    ) -> Result<BTreeMap<(String, String), WrittenLayer>, Error> {
        if &self.origin != origin {
            tracing::info!(
                differs = %self.origin.differences_from(origin).join(", "),
                recorded = %self.origin.repository,
                publishing = %origin.repository,
                "a resumption record was left by a publish this one does not continue; \
                 every layer will be rebuilt"
            );

            return Ok(BTreeMap::new());
        }

        let mut table = BTreeMap::new();

        for layer in &self.layers {
            // The filter `Chained::landed` applies on the way out, applied
            // again on the way in: a record naming a tip layer is a record
            // nothing wrote, and honouring one would carry a stale tip into a
            // manifest.
            let role = profile.role(&layer.descriptor.kind, &layer.descriptor.scope, sequence)?;

            if role != Role::Diff {
                continue;
            }

            table.insert(
                scope_key(&layer.descriptor.kind, &layer.descriptor.scope)?,
                layer.clone(),
            );
        }

        Ok(table)
    }
}

fn read_file(path: &Path) -> Result<Option<Vec<u8>>, stelae::Error> {
    match std::fs::read(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn save_atomically(path: &Path, bytes: &[u8]) -> Result<(), stelae::Error> {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    let staging = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));

    // Scoped so the handle is closed before the rename: Windows refuses to
    // rename a file that is still open.
    {
        let mut file = std::fs::File::create(&staging)?;

        file.write_all(bytes)?;

        // Before the rename, not after: a rename that lands pointing at bytes
        // the page cache has not written yet is the same truncated file by
        // another route.
        file.sync_all()?;
    }

    std::fs::rename(&staging, path)?;

    Ok(())
}

/// Where this node stands relative to what `registry` already holds.
///
/// One read of the moving tag, and no store is touched. It is what turns "the
/// node has not entered a new epoch" from the same refusal a skipped epoch
/// raises into an answer a job on a timer can act on — see [`Standing`].
///
/// Cheap enough to ask before every publish: a manifest pull against the
/// moving tag, which [`publish`] and [`preview`] are each about to make anyway.
/// Asking twice is one HTTP round trip against the alternative, which is
/// threading the answer out of a call that has already started building.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn standing(
    registry: &Registry,
    profile: &dyn DriverProfile,
    sequence: u64,
    position: &serde_json::Value,
) -> Result<Standing, Error> {
    let latest = registry
        .latest(profile)?
        .map(|snapshot| snapshot.read_inscription())
        .transpose()?;

    if let Some(previous) = &latest {
        // The same refusal a publish makes, made before the report rather than
        // after it: a repository holding another dataset's chain is not "up to
        // date" with this node in any sense worth reporting.
        profile.check_same_dataset(previous, position)?;
    }

    Ok(Standing::read(
        latest.map(|previous| previous.sequence),
        sequence,
    ))
}

/// The publish this one follows, in a repository — and this publish, if an
/// earlier attempt of it was interrupted.
///
/// Holds the link it hands to the new inscription and the layers an
/// interrupted attempt recorded, keyed by the pair that identifies a layer: its
/// kind and the canonical encoding of its profile-owned scope. Nothing of the
/// predecessor's layers is held: a publish uploads and lists only its own.
pub struct Chained<'a> {
    registry: &'a Registry,
    /// Held rather than threaded, because [`Predecessor::landed`] is asked by
    /// the export's producer pool and has nowhere to take one from.
    profile: &'a (dyn DriverProfile + Sync),
    sequence: u64,
    predecessor: Option<(u64, Digest)>,
    previous: Option<Link>,
    resumable: BTreeMap<(String, String), WrittenLayer>,
    record: Option<Recording>,
    /// Atomic, like the record's lock below: an export asks a predecessor
    /// about layers from a pool of producer threads.
    resumed: AtomicUsize,
}

/// The resumption record this publish is writing, open.
///
/// Seeded with what it resumes, so the file is the whole of what is up rather
/// than the whole of what *this attempt* put up: an attempt that resumes twenty
/// layers and adds one, then dies, has to leave twenty-one behind or the third
/// attempt pays for the difference.
///
/// The lock is held across the file write as well as the map insert: each
/// write rewrites the whole record, so two producers landing layers at once
/// must serialize on the file or the later write would drop the earlier
/// layer.
struct Recording {
    path: PathBuf,
    origin: Origin,
    layers: Mutex<BTreeMap<(String, String), WrittenLayer>>,
}

impl<'a> Chained<'a> {
    /// The publish `sequence` follows in `publishing`'s repository.
    ///
    /// Every input is a part rather than a plan: the sequence being published,
    /// the `position` document the new snapshot will carry — read only by the
    /// profile, through [`DriverProfile::check_same_dataset`] — and the
    /// [`Origin`] the profile records under.
    pub fn new(
        profile: &'a (dyn DriverProfile + Sync),
        publishing: Publishing<'a>,
        latest: Option<&Snapshot>,
        sequence: u64,
        position: &serde_json::Value,
        origin: Origin,
    ) -> Result<Self, Error> {
        let Publishing {
            registry,
            record_path,
            rebuild,
            follows,
        } = publishing;

        let inscription = latest
            .map(|snapshot| snapshot.read_inscription())
            .transpose()?;

        let predecessor = inscription
            .as_ref()
            .map(|previous| Ok::<_, Error>((previous.sequence, previous.digest()?)))
            .transpose()?;

        // Ahead of every other check on this read, so a moved tag is told as
        // that and not as whatever the other writer's snapshot fails.
        if let Some(expected) = follows.filter(|expected| *expected != predecessor) {
            let named = |snapshot: Option<(u64, Digest)>| {
                snapshot.map(|(sequence, identity)| (sequence, identity.to_string()))
            };
            return Err(stelae::Error::PredecessorMoved {
                expected: named(expected),
                found: named(predecessor),
            }
            .into());
        }

        if let Some(previous) = &inscription {
            // The pull that fetched this checked as a reader; this is the
            // publish side, which extends the chain and must attest it.
            previous.check_profile_strict(profile)?;
            profile.check_same_dataset(previous, position)?;
        }

        let previous = stelae::inscription::link_for(inscription.as_ref(), sequence)?;

        // It is the *honouring* that `rebuild` gates rather than the reading —
        // the asymmetry a restore's checkpoint states, for the same reason. A
        // publisher that asked to rebuild gets a record that starts empty and
        // overwrites whatever was there, so nothing an earlier attempt believed
        // can survive the run that was meant to settle it.
        let resumable = match (rebuild, record_path.as_deref()) {
            (false, Some(record_path)) => match PublishRecord::load(record_path)? {
                Some(record) => record.table(&origin, profile, sequence)?,
                None => BTreeMap::new(),
            },
            _ => BTreeMap::new(),
        };

        if !resumable.is_empty() {
            tracing::info!(
                layers = resumable.len(),
                "an interrupted publish left layers in this repository; \
                 they will be carried forward rather than rebuilt"
            );
        }

        let record = record_path.map(|record_path| Recording {
            path: record_path,
            origin,
            layers: Mutex::new(resumable.clone()),
        });

        Ok(Self {
            registry,
            profile,
            sequence,
            predecessor,
            previous,
            resumable,
            record,
            resumed: AtomicUsize::new(0),
        })
    }

    /// The snapshot this one chains to, if the repository holds one.
    pub fn predecessor(&self) -> Option<(u64, Digest)> {
        self.predecessor
    }

    /// How many layers this publish carried from an interrupted attempt
    /// rather than built.
    pub fn resumed(&self) -> usize {
        self.resumed.load(Ordering::Relaxed)
    }

    /// Delete the resumption record.
    ///
    /// Called once the snapshot is sealed and never before it: before the seal
    /// it would be a record that outlived neither the run nor its
    /// usefulness.
    pub fn forget_record(&self) -> Result<(), Error> {
        match &self.record {
            Some(record) => PublishRecord::remove(&record.path),
            None => Ok(()),
        }
    }
}

impl Predecessor for Chained<'_> {
    fn previous(&self) -> Option<Link> {
        self.previous
    }

    /// The record's answer, gated on the registry's own blob check: a recorded
    /// layer whose blob is gone is rebuilt, not refused.
    fn resume(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let Some(recorded) = self.resumable.get(&scope_key(kind, scope)?) else {
            return Ok(None);
        };

        if !self.registry.adopt_carried(recorded.clone())? {
            tracing::warn!(
                kind,
                %scope,
                "a recorded layer's blob is no longer in the repository; rebuilding it"
            );

            return Ok(None);
        }

        self.resumed.fetch_add(1, Ordering::Relaxed);

        Ok(Some(recorded.descriptor.clone()))
    }

    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), Error> {
        let Some(record) = &self.record else {
            return Ok(());
        };

        if self
            .profile
            .role(&descriptor.kind, &descriptor.scope, self.sequence)?
            != Role::Diff
        {
            return Ok(());
        }

        let Some(written) = self.registry.carried(&descriptor.diff_id) else {
            tracing::warn!(
                kind = descriptor.kind,
                scope = %descriptor.scope,
                "this layer is not in the transport, so nothing was recorded for it; \
                 an interrupted publish will rebuild it"
            );

            return Ok(());
        };

        // The transport is asked for its *measurement* and not for its
        // descriptor: `carried` finds a layer by `diffId`, and the descriptor
        // the caller states is the one the manifest will list.
        let written = WrittenLayer {
            descriptor: descriptor.clone(),
            digests: written.digests,
        };

        // Held across the save below, not just the insert — see [`Recording`].
        let mut layers = record
            .layers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        layers.insert(scope_key(&descriptor.kind, &descriptor.scope)?, written);

        PublishRecord {
            origin: record.origin.clone(),
            layers: layers.values().cloned().collect(),
        }
        .save(&record.path)
    }
}

/// What a publish holds staged on disk at its peak.
///
/// Not the size of the snapshot: a publish never has the whole document on disk
/// at once. What it does have at once is a *tip* pass — the profile's export
/// keeps one state kind's shard sinks open across a single walk of its
/// namespace — plus whatever other layers are in flight beside it. Summing
/// *every* state layer rather than the widest kind's is deliberately
/// conservative: the peak it prices is the pre-split one, an upper bound on any
/// per-kind pass.
///
/// Which layers are the tip is [`DriverProfile::is_state_kind`]'s answer and
/// not one this crate could reach: it is the one place the arithmetic has to
/// know something about a kind beyond its name.
///
/// "Layers beside it" is a handful and not one, because the transport uploads
/// concurrently: a layer's staging file lives until its own round trip lands,
/// so as many finished layers as the transport has permits can sit on disk at
/// once alongside the pass. How many that is comes from
/// [`stelae::oci::Registry::concurrency`] — asked of the transport rather than
/// assumed here — and *which* ones is the largest that many, since the peak is
/// the worst arrangement and nothing orders the uploads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StagingPeak {
    /// Every state layer, summed — the conservative reading above.
    pub state_bytes: u64,
    /// The non-state layers that can be staged at once, largest first: as many
    /// as the transport uploads concurrently, or all of them where the snapshot
    /// has fewer than that. Empty on a snapshot that is all state, and on a
    /// [`StagingPeak::default`] nothing was measured into.
    pub concurrent_other_bytes: Vec<u64>,
    /// Layers the predecessor's manifest stated no size for. Non-zero makes
    /// every number here a floor rather than an estimate.
    pub unsized_layers: usize,
}

impl StagingPeak {
    /// Compressed bytes the scratch volume has to hold at once.
    ///
    /// Saturating throughout, because these are a manifest's numbers and a
    /// manifest is not this node's document: a wrapped sum would be a *small*
    /// need, which is the one direction a refusal must never be wrong in.
    pub fn bytes(&self) -> u64 {
        self.concurrent_other_bytes
            .iter()
            .fold(self.state_bytes, |total, bytes| {
                total.saturating_add(*bytes)
            })
    }

    /// The largest non-state layer, which is the first of the ones staged at
    /// once.
    pub fn largest_other_bytes(&self) -> u64 {
        self.concurrent_other_bytes.first().copied().unwrap_or(0)
    }
}

/// Size the staging peak of the next publish from the snapshot before it.
///
/// A proxy, and deliberately so: the numbers are the *predecessor's* layers,
/// not this publish's, because this publish's layers do not exist until it
/// builds them and sizing them would mean building them. The predecessor's
/// inscription lists only the layers its own publish wrote, so the proxy is
/// one publish against the next. One sequence of drift is what it costs,
/// against a check that otherwise cannot exist at all — and the refuse/warn
/// split is what keeps it honest, since a shortfall against the last publish's
/// sizes is still a measured shortfall.
///
/// Off the manifests the pull already fetched, and no per-layer `HEAD`: the
/// promise a profile's dry run makes about touching nothing holds here too.
///
/// `Ok(None)` is a first publish — no predecessor, nothing to size from — which
/// [`preflight`] turns into a warning rather than a refusal.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn staging_peak(
    registry: &Registry,
    profile: &dyn DriverProfile,
) -> Result<Option<StagingPeak>, Error> {
    let Some(previous) = registry.latest(profile)? else {
        return Ok(None);
    };

    let inscription = previous.read_inscription()?;
    let blobs = previous.blob_index()?;

    let mut peak = StagingPeak::default();
    let mut others = Vec::new();

    for descriptor in inscription.layers() {
        match previous.compressed_size(&blobs, descriptor)? {
            None => peak.unsized_layers += 1,
            // Every state layer adds — the conservative sum-all-state rule the
            // type documents.
            Some(bytes) if profile.is_state_kind(&descriptor.kind) => {
                peak.state_bytes = peak.state_bytes.saturating_add(bytes)
            }
            Some(bytes) => others.push(bytes),
        }
    }

    // The largest as many as the transport stages at once, and no more: a snapshot
    // with fewer non-state layers than the transport has permits cannot put
    // more of them on disk than it has.
    others.sort_unstable_by(|a, b| b.cmp(a));
    others.truncate(registry.concurrency());

    peak.concurrent_other_bytes = others;

    Ok(Some(peak))
}

/// Refuse a publish whose scratch volume cannot hold what it stages at once.
///
/// The publish side of [`crate::preflight`]'s one policy, which a profile's
/// restore is the other side of: a measured shortfall refuses, naming the
/// volume and the shortfall, and what cannot be sized warns and proceeds.
///
/// The volume is the transport's own — [`stelae::oci::Registry::scratch_dir`] —
/// so the directory sized here and the directory written to cannot come apart.
/// A transport opened without one stages in the platform temporary directory,
/// which it does not name, so there is nothing to size and nothing to refuse;
/// [`open`] always sets one, so no caller that came through it reaches that
/// case.
///
/// **Never call this from inside an async context.** See [`open`].
pub fn preflight(registry: &Registry, profile: &dyn DriverProfile) -> Result<(), Error> {
    let Some(scratch_dir) = registry.scratch_dir() else {
        tracing::warn!(
            "this transport stages in the platform temporary directory, which it does not name; \
             skipping the free-space check for {STAGING}"
        );

        return Ok(());
    };

    crate::preflight::check(&[staging_need(staging_peak(registry, profile)?, scratch_dir)])
}

/// What the staging asks of its volume, as [`crate::preflight`] takes it.
///
/// Split out of [`preflight`] so the demand and the transport that produced it
/// can be tested apart: sizing a peak needs a registry, and deciding what a
/// peak means for a volume needs none.
const STAGING: &str = "staging this publish";

fn staging_need(peak: Option<StagingPeak>, scratch_dir: &Path) -> crate::preflight::Need {
    let Some(peak) = peak else {
        return crate::preflight::Need::unsized_because(
            STAGING,
            scratch_dir,
            "this repository holds no snapshot to size the staging from",
        );
    };

    if peak.unsized_layers > 0 {
        tracing::warn!(
            unsized_layers = peak.unsized_layers,
            "the predecessor's manifest states no size for some of its layers; the staging \
             estimate is a floor"
        );
    }

    crate::preflight::Need::of(STAGING, scratch_dir, peak.bytes())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::{Inscription, LayerDigests};

    use super::*;

    /// A profile whose notes for chapter `c` stay in the tip at sequence `c`
    /// and are final from `c + 1`, and whose index is always the tip.
    struct Settling;

    impl stelae::Profile for Settling {
        fn name(&self) -> &str {
            "dev.example.settling"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes", "index"]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
            Ok(format!("application/vnd.example.stelae.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
            Ok(format!("chapter-{sequence}"))
        }
    }

    impl DriverProfile for Settling {
        fn epoch_kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn dense_epoch_kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn is_state_kind(&self, kind: &str) -> bool {
            kind == "index"
        }

        fn role(
            &self,
            kind: &str,
            scope: &serde_json::Value,
            sequence: u64,
        ) -> Result<Role, Error> {
            match (kind, scope["chapter"].as_u64()) {
                ("notes", Some(chapter)) if chapter < sequence => Ok(Role::Diff),
                _ => Ok(Role::Tip),
            }
        }

        fn check_same_dataset(
            &self,
            _previous: &Inscription,
            _position: &serde_json::Value,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn origin() -> Origin {
        Origin {
            repository: "registry.example/shelf".to_owned(),
            dataset_id: 1,
            parameters: json!({}),
            compression: Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
        }
    }

    fn written(kind: &str, chapter: u64) -> WrittenLayer {
        let diff_id = Digest::compute(format!("{kind}-{chapter}"));

        WrittenLayer {
            descriptor: LayerDescriptor {
                kind: kind.to_owned(),
                media_type: format!("application/vnd.example.stelae.{kind}.v1+zstd"),
                diff_id,
                records: 1,
                uncompressed_size: 1,
                scope: json!({"chapter": chapter}),
            },
            digests: LayerDigests {
                diff_id,
                blob_digest: Digest::compute(format!("blob-{kind}-{chapter}")),
                uncompressed_size: 1,
                compressed_size: 1,
            },
        }
    }

    /// A record offers a publish only the layers its role places in `diff` at
    /// the sequence being published, so the same record answers differently
    /// for two sequences, and a record from another origin offers nothing.
    #[test]
    fn a_record_offers_only_what_is_diff_at_the_publishing_sequence() {
        let record = PublishRecord {
            origin: origin(),
            layers: vec![written("notes", 2), written("index", 3)],
        };

        let offered = |sequence| {
            record
                .table(&origin(), &Settling, sequence)
                .unwrap()
                .into_keys()
                .collect::<Vec<_>>()
        };

        let notes = scope_key("notes", &json!({"chapter": 2})).unwrap();

        assert_eq!(offered(3), vec![notes]);
        assert!(offered(2).is_empty(), "notes not yet final were offered");

        // Any field of the origin differing is a record from somewhere else.
        for elsewhere in [
            Origin {
                repository: "registry.example/other".to_owned(),
                ..origin()
            },
            Origin {
                dataset_id: 2,
                ..origin()
            },
            Origin {
                parameters: json!({"width": 2}),
                ..origin()
            },
            Origin {
                compression: Compression {
                    algo: "zstd".to_owned(),
                    level: 3,
                },
                ..origin()
            },
        ] {
            assert!(
                record.table(&elsewhere, &Settling, 3).unwrap().is_empty(),
                "a record from {elsewhere:?} was offered"
            );
        }
    }

    /// A registry a node opens through here, as Dolos does for a restore,
    /// reads under the transport's default budget rather than a shortened one.
    #[test]
    fn an_opened_registry_reads_under_the_default_budget() {
        let options = options(false, Auth::default(), PathBuf::new(), Tuning::default());

        assert_eq!(options.read_attempts, stelae::oci::DEFAULT_READ_ATTEMPTS);
        assert_eq!(options.read_delay, stelae::oci::DEFAULT_READ_DELAY);
        assert_eq!(options.attempts, stelae::oci::DEFAULT_ATTEMPTS);
    }

    /// The publish half of the one policy, over a peak this test supplies.
    ///
    /// The other half of the chain — that the peak is the predecessor's tip
    /// shards plus its largest other layer — needs a registry to state a
    /// manifest, so it lives with a profile that can build one. Between them
    /// the composition [`preflight`] performs is covered: this end decides,
    /// that end measures.
    #[test]
    fn a_measured_staging_shortfall_refuses_and_a_first_publish_does_not() {
        let temp = tempfile::tempdir().unwrap();

        let check = |peak| -> Result<(), Error> {
            crate::preflight::check(&[staging_need(peak, temp.path())])
        };

        // No predecessor, so nothing sized it, so nothing refuses.
        check(None).unwrap();

        check(Some(StagingPeak::default())).unwrap();

        // A peak no volume holds. Both halves are set, so the refusal is on
        // their sum and not on either alone.
        let err = check(Some(StagingPeak {
            state_bytes: u64::MAX / 2,
            concurrent_other_bytes: vec![u64::MAX / 4, u64::MAX / 4],
            unsized_layers: 0,
        }))
        .unwrap_err();

        let Error::NotEnoughSpace(message) = &err else {
            panic!("{err:?}");
        };

        assert!(message.contains(STAGING), "{message}");
        assert!(
            message.contains(&temp.path().display().to_string()),
            "{message}"
        );
        assert!(message.contains("short"), "{message}");
    }
}
