//! Writing a rebuilt lineage to the registry: `rebuild --write` executes the
//! plan [`super::run`] wrote (decision 0080, point 19).
//!
//! The new lineage goes to `<root>/dolos`, the root being the old repository
//! unless another is named. Its blobs are put there first, copied inside the
//! registry's storage or pulled and pushed. Then, per sequence in ascending
//! order: the inscription, a kept tip's manifest, the diff manifest. A closing
//! pass resolves the last sequence and holds its layer set to the plan, and
//! only then does `latest` move.
//!
//! Every tag the plan writes is read before anything is written. One that
//! holds the planned manifest is skipped, so an interrupted run resumes; one
//! that holds another refuses the run. Nothing is written to the old
//! repository.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use dolos_snapshot::DolosProfile;
use stelae::{
    manifest::{manifest_bytes, read_manifest, Array, Descriptor, Manifest},
    oci::{s3_upload, Carried, Registry},
    profile::{checked_tag_for_sequence, tip_tag, Profile as _},
    Digest, Link, SnapshotReader as _,
};

use super::{PlanHeader, Planned, PLAN_FORMAT};
use crate::{publisher::dolos_repository, Auth, Repository};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("line {line} of the plan: {reason}")]
    Plan { line: usize, reason: String },

    #[error("{root} names no repository under it")]
    Target {
        root: String,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },

    #[error("the plan's lineage is in {repository}, the repository it would be written to")]
    SameRepository { repository: String },

    #[error("{what} in {repository}")]
    Registry {
        repository: String,
        what: String,
        #[source]
        source: Box<stelae::Error>,
    },

    #[error("{repository} holds the manifest {found} under {tag}, and the plan's is {planned}")]
    Differs {
        repository: String,
        tag: String,
        found: Digest,
        planned: Digest,
    },

    #[error("{repository} does not resolve at epoch-{sequence} to the plan's lineage: {reason}")]
    Resolved {
        repository: String,
        sequence: u64,
        reason: String,
    },
}

/// A plan file, read whole and held together: one lineage, each sequence's
/// documents agreeing with each other and linked to the one before.
#[derive(Debug, Clone)]
pub struct Plan {
    pub header: PlanHeader,
    /// The old lineage's repository, as the header names it.
    pub source: Repository,
    pub sequences: Vec<Planned>,
}

impl Plan {
    /// Read the plan file `text`, refusing one this build did not write or
    /// whose documents disagree.
    pub fn read(text: &str) -> Result<Self, Error> {
        let mut lines = text
            .lines()
            .enumerate()
            .map(|(index, line)| (index + 1, line))
            .filter(|(_, line)| !line.trim().is_empty());

        let (line, first) = lines.next().ok_or(Error::Plan {
            line: 1,
            reason: "the file is empty".into(),
        })?;
        let refused = |line: usize| move |reason: String| Error::Plan { line, reason };

        let header: PlanHeader = serde_json::from_str(first)
            .map_err(|error| refused(line)(format!("not a plan header: {error}")))?;
        if header.rebuild_plan != PLAN_FORMAT {
            return Err(refused(line)(format!(
                "format {}, and this build reads {PLAN_FORMAT}",
                header.rebuild_plan
            )));
        }
        let source = header
            .source
            .parse()
            .map_err(|error| refused(line)(format!("its source: {error}")))?;

        let mut sequences: Vec<Planned> = Vec::new();
        for (line, text) in lines {
            let planned: Planned = serde_json::from_str(text)
                .map_err(|error| refused(line)(format!("not a planned sequence: {error}")))?;
            let expected = header.first + sequences.len() as u64;
            let previous = sequences.last().map(|before| Link {
                sequence: before.sequence,
                inscription_digest: before.inscription_digest,
            });
            check_planned(&planned, expected, previous).map_err(refused(line))?;
            sequences.push(planned);
        }

        let newest = sequences.last().map(|newest| newest.sequence);
        if header.first > header.last || newest != Some(header.last) {
            return Err(refused(line)(format!(
                "it names epoch-{} to epoch-{}, and the file holds {}",
                header.first,
                header.last,
                match newest {
                    Some(newest) => format!("epoch-{} to epoch-{newest}", header.first),
                    None => "no sequence".into(),
                }
            )));
        }
        if sequences
            .last()
            .is_some_and(|newest| newest.tip_manifest.is_some() && !newest.kept_tip)
        {
            return Err(refused(line)("its newest tip is not kept".into()));
        }

        Ok(Self {
            header,
            source,
            sequences,
        })
    }

    /// The blobs the lineage's published manifests name, each once, in plan
    /// order: every diff manifest's and each kept tip's.
    pub fn blobs(&self) -> Vec<&Descriptor> {
        let mut seen = BTreeSet::new();
        self.sequences
            .iter()
            .flat_map(|planned| published(planned).flat_map(|(_, manifest)| &manifest.layers))
            .filter(|blob| seen.insert(blob.digest.clone()))
            .collect()
    }
}

/// A sequence's documents agree: its inscription is the Dolos profile's at
/// `sequence`, under its digest, linked to `previous`, and listed by its
/// manifests and its blobs.
fn check_planned(planned: &Planned, sequence: u64, previous: Option<Link>) -> Result<(), String> {
    let inscription = &planned.inscription;
    if planned.sequence != sequence || inscription.sequence != sequence {
        return Err(format!(
            "it plans sequence {}, where epoch-{sequence} follows",
            planned.sequence
        ));
    }
    inscription
        .check_profile_strict(&DolosProfile)
        .map_err(|error| format!("its inscription: {error}"))?;
    let digest = inscription.digest().map_err(|error| error.to_string())?;
    if digest != planned.inscription_digest {
        return Err(format!(
            "its inscription's digest is {digest}, and it names {}",
            planned.inscription_digest
        ));
    }
    if inscription.previous != previous {
        return Err(match previous {
            Some(link) => format!(
                "its inscription does not link to epoch-{}'s, {}",
                link.sequence, link.inscription_digest
            ),
            None => "the lineage's first inscription links to another".into(),
        });
    }

    read_manifest(&planned.diff_manifest, inscription, Array::Diff)
        .map_err(|error| format!("its diff manifest: {error}"))?;
    match &planned.tip_manifest {
        Some(tip) => read_manifest(tip, inscription, Array::Tip)
            .map(|_| ())
            .map_err(|error| format!("its tip manifest: {error}"))?,
        None if !inscription.tip.is_empty() => return Err("its tip has no manifest".into()),
        None if planned.kept_tip => return Err("it keeps a tip it does not have".into()),
        None => {}
    }
    for (_, manifest) in published(planned) {
        manifest_bytes(manifest).map_err(|error| error.to_string())?;
    }

    let kept = planned.tip_manifest.iter().filter(|_| planned.kept_tip);
    let named: Vec<(String, u64)> = std::iter::once(&planned.diff_manifest)
        .chain(kept)
        .flat_map(|manifest| &manifest.layers)
        .map(|layer| (layer.digest.clone(), layer.size as u64))
        .collect();
    let listed: Vec<(String, u64)> = planned
        .blobs
        .iter()
        .map(|blob| (blob.digest.to_string(), blob.size))
        .collect();
    if named != listed {
        return Err("its blobs are not the ones its published manifests name".into());
    }

    Ok(())
}

/// The manifests a sequence publishes, in the order they are tagged: a kept
/// tip's, then the diff's.
fn published(planned: &Planned) -> impl Iterator<Item = (Array, &Manifest)> {
    let tip = planned
        .tip_manifest
        .as_ref()
        .filter(|_| planned.kept_tip)
        .map(|tip| (Array::Tip, tip));
    tip.into_iter()
        .chain(std::iter::once((Array::Diff, &planned.diff_manifest)))
}

/// What `stelae-publisher rebuild --write` reads beside the plan.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The network root to write `dolos` under; the plan's source when `None`.
    pub root: Option<Repository>,
    pub insecure: bool,
    /// The identity both repositories are reached as.
    pub auth: Auth,
    /// The registry's storage, for copying blobs inside it.
    pub direct_s3: Option<Box<s3_upload::Config>>,
    pub scratch_dir: Option<PathBuf>,
}

/// What a write did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub target: Repository,
    pub first: u64,
    pub last: u64,
    /// The newest inscription's digest: the identity `latest` names.
    pub identity: Digest,
    pub blobs_present: usize,
    pub blobs_copied: usize,
    pub blobs_pulled: usize,
    pub tags_written: usize,
    /// Tags that already held the planned manifest.
    pub tags_present: usize,
}

/// One manifest to tag: its tag and exact bytes.
struct Tagged {
    tag: String,
    body: Vec<u8>,
}

/// Write `plan`'s lineage to `<root>/dolos` as the module documentation says,
/// and resolve it back.
pub fn write(plan: &Plan, options: &Options) -> Result<Written, Error> {
    let source = &plan.source;
    let root = options.root.as_ref().unwrap_or(source);
    let target = dolos_repository(root).map_err(|error| Error::Target {
        root: root.to_string(),
        source: Box::new(error),
    })?;
    if &target == source {
        return Err(Error::SameRepository {
            repository: target.to_string(),
        });
    }

    let (old, new) = (source.to_string(), target.to_string());
    let failed = |repository: &str, what: String| {
        let repository = repository.to_owned();
        move |source| Error::Registry {
            repository,
            what,
            source: Box::new(source),
        }
    };
    let open = |repository: &Repository, direct_s3| {
        Registry::open(
            repository,
            stelae::oci::Options {
                direct_s3,
                insecure: options.insecure,
                auth: options.auth.clone(),
                scratch_dir: options.scratch_dir.clone(),
                ..Default::default()
            },
        )
        .map_err(failed(
            &repository.to_string(),
            "opening the repository".into(),
        ))
    };
    let reading = open(source, None)?;
    let writing = open(&target, options.direct_s3.clone())?;

    let mut tagged: Vec<Vec<Tagged>> = Vec::new();
    for planned in &plan.sequences {
        let tag = checked_tag_for_sequence(&DolosProfile, planned.sequence)
            .map_err(failed(&new, format!("naming epoch-{}", planned.sequence)))?;
        let mut tags = Vec::new();
        for (array, manifest) in published(planned) {
            let tag = match array {
                Array::Tip => tip_tag(&tag).map_err(failed(&new, format!("naming {tag}'s tip")))?,
                Array::Diff => tag.clone(),
            };
            let body = manifest_bytes(manifest).map_err(failed(&new, format!("encoding {tag}")))?;
            tags.push(Tagged { tag, body });
        }
        tagged.push(tags);
    }
    let latest = Tagged {
        tag: DolosProfile.moving_tag().to_owned(),
        body: tagged
            .last()
            .and_then(|tags| tags.last())
            .map(|diff| diff.body.clone())
            .unwrap_or_default(),
    };

    let present = held(&writing, &new, tagged.iter().flatten().chain([&latest]))?;
    let mut written = Written {
        target: target.clone(),
        first: plan.header.first,
        last: plan.header.last,
        identity: plan.sequences[plan.sequences.len() - 1].inscription_digest,
        blobs_present: 0,
        blobs_copied: 0,
        blobs_pulled: 0,
        tags_written: 0,
        tags_present: present.len(),
    };

    let blobs = plan.blobs();
    for (index, blob) in blobs.iter().enumerate() {
        let carried = writing.carry_blob(&reading, blob).map_err(failed(
            &new,
            format!("putting blob {} from {old}", blob.digest),
        ))?;
        match carried {
            Carried::Present => written.blobs_present += 1,
            Carried::Copied => written.blobs_copied += 1,
            Carried::Pulled => written.blobs_pulled += 1,
        }
        if (index + 1) % 100 == 0 || index + 1 == blobs.len() {
            tracing::info!("{new}: {} of {} blobs in place", index + 1, blobs.len());
        }
    }

    for (planned, tags) in plan.sequences.iter().zip(&tagged) {
        let document = planned
            .inscription
            .canonicalize()
            .map_err(failed(&new, format!("encoding epoch-{}", planned.sequence)))?;
        writing.put_document(document).map_err(failed(
            &new,
            format!("writing epoch-{}'s inscription", planned.sequence),
        ))?;

        for Tagged { tag, body } in tags.iter().filter(|tagged| !present.contains(&tagged.tag)) {
            writing
                .put_manifest(tag, body.clone())
                .map_err(failed(&new, format!("tagging {tag}")))?;
            written.tags_written += 1;
        }
        tracing::info!("{new}: epoch-{} in place", planned.sequence);
    }

    resolves(&writing, plan).map_err(|(sequence, reason)| Error::Resolved {
        repository: new.clone(),
        sequence,
        reason,
    })?;

    if !present.contains(&latest.tag) {
        writing
            .put_manifest(&latest.tag, latest.body)
            .map_err(failed(&new, format!("moving {}", latest.tag)))?;
        written.tags_written += 1;
    }

    Ok(written)
}

/// The tags of `wanted` that `registry` already holds as wanted, refusing one
/// that holds another manifest. Each is read by name, so nothing rests on how
/// the registry pages its tag list.
fn held<'a>(
    registry: &Registry,
    repository: &str,
    wanted: impl Iterator<Item = &'a Tagged>,
) -> Result<BTreeSet<String>, Error> {
    let mut present = BTreeSet::new();
    for Tagged { tag, body } in wanted {
        let found = registry
            .manifest_at(tag)
            .map_err(|source| Error::Registry {
                repository: repository.to_owned(),
                what: format!("reading {tag}"),
                source: Box::new(source),
            })?;
        let Some(found) = found else {
            continue;
        };
        if &found != body {
            return Err(Error::Differs {
                repository: repository.to_owned(),
                tag: tag.clone(),
                found: Digest::compute(&found),
                planned: Digest::compute(body),
            });
        }
        present.insert(tag.clone());
    }

    Ok(present)
}

/// The closing pass: the last sequence resolves through every planned
/// inscription to the planned layer set, each layer in the blob its manifest
/// names.
fn resolves(registry: &Registry, plan: &Plan) -> Result<(), (u64, String)> {
    let newest = &plan.sequences[plan.sequences.len() - 1];
    let refused = |reason: String| (newest.sequence, reason);

    let snapshot = registry
        .resolve_sequence(&DolosProfile, newest.sequence)
        .map_err(|error| refused(error.to_string()))?;
    let chain = snapshot
        .chain()
        .ok_or_else(|| refused("it resolved no chain".into()))?;

    let found: Vec<Digest> = chain
        .inscriptions()
        .iter()
        .map(|inscription| inscription.digest())
        .collect::<Result<_, _>>()
        .map_err(|error| refused(error.to_string()))?;
    let planned: Vec<Digest> = plan
        .sequences
        .iter()
        .map(|planned| planned.inscription_digest)
        .collect();
    if found != planned {
        return Err(refused(format!(
            "its chain holds {} inscriptions from epoch-{}, and the plan {} from epoch-{}",
            found.len(),
            chain.first().sequence,
            planned.len(),
            plan.header.first
        )));
    }

    let layers: Vec<_> = plan
        .sequences
        .iter()
        .flat_map(|planned| &planned.inscription.diff)
        .chain(&newest.inscription.tip)
        .collect();
    if chain.descriptors().ne(layers.iter().copied()) {
        return Err(refused("its layer set is not the plan's".into()));
    }

    let mut blobs: BTreeMap<Digest, String> = BTreeMap::new();
    let manifests = plan
        .sequences
        .iter()
        .map(|planned| (&planned.inscription.diff, &planned.diff_manifest))
        .chain(
            newest
                .tip_manifest
                .iter()
                .map(|tip| (&newest.inscription.tip, tip)),
        );
    for (layers, manifest) in manifests {
        for (layer, blob) in layers.iter().zip(&manifest.layers) {
            blobs.insert(layer.diff_id, blob.digest.clone());
        }
    }
    let index = snapshot
        .blob_index()
        .map_err(|error| refused(error.to_string()))?;
    if index.len() != blobs.len()
        || blobs.iter().any(|(diff_id, blob)| {
            index.blob_for(diff_id).map(|found| found.to_string()) != Some(blob.clone())
        })
    {
        return Err(refused("its layers are not in the plan's blobs".into()));
    }

    Ok(())
}
