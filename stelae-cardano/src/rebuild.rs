//! Rebuilding a Dolos lineage published before the diff chain, from what its
//! repository holds and with no chain replay (decision 0080, point 19).
//!
//! An old inscription lists every layer of its snapshot in `layers` and every
//! inscription before it in `history`. [`run`] reads each `epoch-N` of such a
//! repository in ascending order and lists each sequence's layers in the new
//! `diff` and `tip` by the rule a native publish uses ([`Listing`] chooses, the
//! profile's role places). Closed epochs' `blocks` layers and retained dumps
//! are dropped. Before a sequence is accepted, the diffs up to it plus its tip
//! must equal its old layer set less what was dropped, by `diffId`.
//!
//! The epoch geometry the rule needs is read from the newest inscription's
//! `indexes` scopes, and the stability window from the configured genesis.
//! Nothing here writes to a registry: given the blocks journal, the output is
//! a [`Planned`] line per sequence, which `rebuild --write` executes.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

use dolos_cardano::{eras::ChainSummary, EraBoundary, EraState};
use dolos_snapshot::{
    is_state_kind,
    roles::{Listing, Rule},
    DolosProfile, BLOCKS, INDEXES, KINDS, PROFILE_NAME, PROFILE_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use stelae::{
    inscription::check_safe_numbers,
    manifest::{build_manifests, manifest_bytes, Manifest, DIFF_ID_ANNOTATION},
    oci::{RawDocument, Registry},
    profile::checked_tag_for_sequence,
    Compression, Digest, Inscription, LayerDescriptor, LayerDigests, Link, ProfileRef,
    WrittenLayer, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE, SCHEMA_VERSION,
};
use stelae_driver::{DriverProfile as _, Role};

use crate::{Auth, Genesis, Repository};

pub mod blocks;

/// The plan file's format, its first line's `rebuildPlan`.
pub const PLAN_FORMAT: u64 = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reading {what} in {repository}")]
    Read {
        repository: String,
        what: String,
        #[source]
        source: Box<stelae::Error>,
    },

    #[error("{repository} holds no epoch-N tag")]
    Empty { repository: String },

    #[error("{repository} holds {tag}, a tag of the diff chain, and not a lineage before it")]
    NotOld { repository: String, tag: String },

    #[error("{repository} holds epoch-{first} to epoch-{last} but not epoch-{missing}")]
    Gap {
        repository: String,
        first: u64,
        last: u64,
        missing: u64,
    },

    #[error("epoch-{sequence} is not a Dolos snapshot of the old format: {reason}")]
    Malformed { sequence: u64, reason: String },

    #[error("epoch-{sequence} follows sequence {follows}, which the repository no longer holds")]
    Truncated { sequence: u64, follows: u64 },

    #[error("epoch-{sequence}'s history does not end at the inscription read at epoch-{previous}")]
    Unlinked { sequence: u64, previous: u64 },

    #[error(
        "epoch-{sequence} names network magic {found}, and this node is configured for {expected}"
    )]
    OtherNetwork {
        sequence: u64,
        expected: u64,
        found: u64,
    },

    #[error("the indexes layers of epoch-{sequence} do not give every epoch's first slot")]
    Geometry { sequence: u64 },

    #[error("placing the layers of epoch-{sequence}")]
    Placing {
        sequence: u64,
        #[source]
        source: Box<dolos_snapshot::Error>,
    },

    /// The check ran and the assignment failed it.
    #[error(
        "the self-check refused epoch-{sequence}: the diffs up to it and its tip {}",
        differences(.missing, .extra)
    )]
    SelfCheck {
        sequence: u64,
        missing: Vec<String>,
        extra: Vec<String>,
    },

    #[error("the blocks journal names no blocks snapshot at epoch-{sequence}")]
    NoBlocks { sequence: u64 },

    #[error("line {line} of the {what} is not {expected}: {text:?}")]
    Line {
        what: &'static str,
        expected: &'static str,
        line: usize,
        text: String,
    },

    #[error("building the documents of epoch-{sequence}")]
    Documents {
        sequence: u64,
        #[source]
        source: Box<stelae::Error>,
    },

    #[error("writing the plan")]
    Write(#[source] std::io::Error),
}

fn differences(missing: &[String], extra: &[String]) -> String {
    let list = |layers: &[String]| layers.join(", ");
    match (missing.is_empty(), extra.is_empty()) {
        (false, true) => format!("lack {}", list(missing)),
        (true, false) => format!("also list {}", list(extra)),
        _ => format!("lack {} and also list {}", list(missing), list(extra)),
    }
}

/// An inscription as it was published before the diff chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OldInscription {
    pub schema: u64,
    pub profile: ProfileRef,
    pub sequence: u64,
    pub position: Value,
    pub parameters: Value,
    pub compression: Compression,
    /// Every earlier inscription, ascending and ending at `sequence - 1`.
    pub history: Vec<Link>,
    pub layers: Vec<LayerDescriptor>,
}

/// A layer's blob, as a manifest names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    pub digest: Digest,
    pub size: u64,
}

/// One `epoch-N` of an old repository: its inscription, held to the manifest
/// naming it, and each layer's blob there.
#[derive(Debug, Clone)]
pub struct OldSequence {
    pub digest: Digest,
    pub inscription: OldInscription,
    pub blobs: BTreeMap<Digest, Blob>,
}

impl OldSequence {
    /// Read the document under `epoch-{sequence}`, held as a reader of the old
    /// format held it: canonical, under its digest, and listed by its manifest
    /// layer for layer.
    pub fn read(sequence: u64, document: &RawDocument) -> Result<Self, Error> {
        let malformed = |reason: String| Error::Malformed { sequence, reason };
        let manifest = &document.manifest;
        let digest = Digest::compute(&document.config);

        if manifest.artifact_type.as_deref() != Some(ARTIFACT_TYPE)
            || manifest.config.media_type != INSCRIPTION_MEDIA_TYPE
            || manifest.config.digest != digest.to_string()
        {
            return Err(malformed("its manifest names no Stelae inscription".into()));
        }

        let value: Value = serde_json::from_slice(&document.config)
            .map_err(|error| malformed(format!("its inscription is not JSON: {error}")))?;
        let canonical = check_safe_numbers(&value).and_then(|()| stelae::canonical_json(&value));
        if canonical.ok().as_deref() != Some(document.config.as_slice()) {
            return Err(malformed("its inscription is not canonical".into()));
        }

        let inscription: OldInscription = serde_json::from_value(value)
            .map_err(|error| malformed(format!("its inscription has another shape: {error}")))?;
        let profile = ProfileRef {
            name: PROFILE_NAME.to_owned(),
            version: PROFILE_VERSION,
        };
        if inscription.schema != SCHEMA_VERSION || inscription.profile != profile {
            return Err(malformed("it is not a Dolos profile inscription".into()));
        }
        if inscription.sequence != sequence {
            return Err(malformed(format!(
                "it holds sequence {}",
                inscription.sequence
            )));
        }
        if manifest.layers.len() != inscription.layers.len() {
            return Err(malformed(
                "its manifest lists another number of layers".into(),
            ));
        }

        let mut blobs = BTreeMap::new();
        for (layer, blob) in inscription.layers.iter().zip(&manifest.layers) {
            let annotated = blob
                .annotations
                .as_ref()
                .and_then(|a| a.get(DIFF_ID_ANNOTATION));
            let size = u64::try_from(blob.size).ok();
            let named = blob.digest.parse::<Digest>().ok();
            let (Some(size), Some(named)) = (size, named) else {
                return Err(malformed(format!(
                    "its manifest names {} badly",
                    blob.digest
                )));
            };
            if annotated != Some(&layer.diff_id.to_string()) || blob.media_type != layer.media_type
            {
                return Err(malformed(format!(
                    "its manifest does not list the {} layer {} where its inscription does",
                    layer.kind, layer.diff_id
                )));
            }
            blobs.insert(
                layer.diff_id,
                Blob {
                    digest: named,
                    size,
                },
            );
        }

        Ok(Self {
            digest,
            inscription,
            blobs,
        })
    }
}

/// The epoch geometry `newest` records: each epoch's first slot, from the
/// `indexes` layer every epoch up to its own has, as eras of one epoch length.
///
/// The last era stays open, which is all [`Rule::seal`] asks of the boundaries
/// after the newest one.
pub fn geometry(newest: &OldInscription) -> Result<ChainSummary, Error> {
    let sequence = newest.sequence;
    let refused = || Error::Geometry { sequence };

    let starts: BTreeMap<u64, u64> = newest
        .layers
        .iter()
        .filter(|layer| layer.kind == INDEXES)
        .filter_map(|layer| {
            let field = |name| layer.scope.get(name).and_then(Value::as_u64);
            Some((field("epoch")?, field("startSlot")?))
        })
        .collect();

    if sequence == 0 || !(0..=sequence).all(|epoch| starts.contains_key(&epoch)) {
        return Err(refused());
    }

    let mut summary = ChainSummary::default();
    let mut eras: u16 = 0;
    let mut open: Option<EraState> = None;

    for epoch in 0..sequence {
        let length = starts[&(epoch + 1)]
            .checked_sub(starts[&epoch])
            .filter(|length| *length > 0)
            .ok_or_else(refused)?;

        if open.as_ref().is_some_and(|era| era.epoch_length == length) {
            continue;
        }

        let start = EraBoundary {
            epoch,
            slot: starts[&epoch],
            timestamp: 0,
        };
        if let Some(mut era) = open.take() {
            era.end = Some(start.clone());
            summary.append_era(era.protocol, era);
        }
        open = Some(EraState {
            start,
            end: None,
            epoch_length: length,
            slot_length: 1,
            protocol: eras,
        });
        eras += 1;
    }

    let era = open.ok_or_else(refused)?;
    summary.append_era(era.protocol, era);

    Ok(summary)
}

/// One sequence as the diff chain lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assigned {
    pub sequence: u64,
    pub diff: Vec<LayerDescriptor>,
    pub tip: Vec<LayerDescriptor>,
    /// Old layers no new inscription lists: closed epochs' `blocks`...
    pub dropped_blocks: usize,
    /// ...and retained state dumps.
    pub dropped_dumps: usize,
}

/// A rebuild's progress through an old lineage, one sequence at a time.
pub struct Rebuild<'a> {
    rule: Rule<'a>,
    magic: u64,
    /// The last sequence assigned, and its old inscription's digest.
    last: Option<(u64, Digest)>,
    /// Every layer an earlier sequence lists in its diff, by `diffId`.
    listed: BTreeMap<Digest, LayerDescriptor>,
}

impl<'a> Rebuild<'a> {
    /// A rebuild over the chain `summary` describes, with the genesis
    /// stability window `mutable_slots`, of a lineage on network `magic`.
    pub fn new(summary: &'a ChainSummary, mutable_slots: u64, magic: u64) -> Self {
        Self {
            rule: Rule::new(summary, mutable_slots),
            magic,
            last: None,
            listed: BTreeMap::new(),
        }
    }

    /// Assign the next sequence of the lineage, refusing one that does not
    /// follow the last, or whose assignment fails the self-check.
    pub fn assign(&mut self, old: &OldSequence) -> Result<Assigned, Error> {
        let inscription = &old.inscription;
        let sequence = inscription.sequence;
        let placing = |source| Error::Placing {
            sequence,
            source: Box::new(source),
        };

        match (self.last, inscription.history.last()) {
            (None, None) => {}
            (None, Some(follows)) => {
                return Err(Error::Truncated {
                    sequence,
                    follows: follows.sequence,
                })
            }
            (Some((previous, digest)), follows) => {
                let link = Link {
                    sequence: previous,
                    inscription_digest: digest,
                };
                if previous + 1 != sequence || follows != Some(&link) {
                    return Err(Error::Unlinked { sequence, previous });
                }
            }
        }

        self.check_document(inscription)?;

        let listing = Listing::new(self.rule, sequence, self.last.is_none());
        let (mut placed, mut expected) = (Vec::new(), BTreeMap::new());
        let (mut dropped_blocks, mut dropped_dumps) = (0, 0);

        for layer in &inscription.layers {
            let window = layer.scope.get("epoch").and_then(Value::as_u64);

            if is_state_kind(&layer.kind) && window.is_some() {
                dropped_dumps += 1;
                continue;
            }
            if layer.kind == BLOCKS && window.is_some_and(|epoch| epoch < sequence) {
                dropped_blocks += 1;
                continue;
            }

            expected.insert(layer.diff_id, layer);
            if listing
                .role(&layer.kind, window)
                .map_err(placing)?
                .is_some()
            {
                placed.push(layer.clone());
            }
        }

        // The order a native export writes them in.
        placed.sort_by_key(|layer| {
            let kind = KINDS.iter().position(|kind| *kind == layer.kind);
            let within = ["epoch", "shard"]
                .iter()
                .find_map(|field| layer.scope.get(*field).and_then(Value::as_u64));
            (kind, within)
        });

        let (mut diff, mut tip) = (Vec::new(), Vec::new());
        for layer in placed {
            let role = DolosProfile
                .role(&layer.kind, &layer.scope, sequence)
                .map_err(|error| placing(error.into()))?;
            match role {
                Role::Diff => diff.push(layer),
                Role::Tip => tip.push(layer),
            }
        }

        self.check_layers(sequence, &expected, &diff, &tip)?;

        self.listed
            .extend(diff.iter().map(|layer| (layer.diff_id, layer.clone())));
        self.last = Some((sequence, old.digest));

        Ok(Assigned {
            sequence,
            diff,
            tip,
            dropped_blocks,
            dropped_dumps,
        })
    }

    /// The parts of an old inscription the new one keeps or rebuilds must be
    /// this profile's: its parameters but `stateEpochs`, its compression, and
    /// a position on the configured network at the sequence's own epoch.
    fn check_document(&self, inscription: &OldInscription) -> Result<(), Error> {
        let sequence = inscription.sequence;
        let malformed = |reason: String| Error::Malformed { sequence, reason };

        let mut parameters = inscription.parameters.clone();
        if let Some(parameters) = parameters.as_object_mut() {
            parameters.remove("stateEpochs");
        }
        if parameters != dolos_snapshot::parameters() {
            return Err(malformed("its parameters are not this profile's".into()));
        }
        if inscription.compression != dolos_snapshot::compression() {
            return Err(malformed("its compression is not this profile's".into()));
        }

        let position = dolos_snapshot::read_position(&inscription.position)
            .map_err(|error| malformed(error.to_string()))?;
        if position.epoch != sequence || position.blocks.is_some() {
            return Err(malformed(
                "its position is not an old one at its epoch".into(),
            ));
        }
        if position.network.magic() != self.magic {
            return Err(Error::OtherNetwork {
                sequence,
                expected: self.magic,
                found: position.network.magic(),
            });
        }

        Ok(())
    }

    /// The self-check: the diffs up to `sequence` plus its tip are `expected`.
    fn check_layers(
        &self,
        sequence: u64,
        expected: &BTreeMap<Digest, &LayerDescriptor>,
        diff: &[LayerDescriptor],
        tip: &[LayerDescriptor],
    ) -> Result<(), Error> {
        let name =
            |layer: &LayerDescriptor| format!("{} {} ({})", layer.kind, layer.scope, layer.diff_id);
        let mut rebuilt: BTreeMap<Digest, &LayerDescriptor> = BTreeMap::new();
        let mut extra = Vec::new();

        for layer in self.listed.values().chain(diff).chain(tip) {
            if rebuilt.insert(layer.diff_id, layer).is_some()
                || !expected.contains_key(&layer.diff_id)
            {
                extra.push(name(layer));
            }
        }

        let missing: Vec<String> = expected
            .iter()
            .filter(|(diff_id, _)| !rebuilt.contains_key(*diff_id))
            .map(|(_, layer)| name(layer))
            .collect();

        if missing.is_empty() && extra.is_empty() {
            return Ok(());
        }

        Err(Error::SelfCheck {
            sequence,
            missing,
            extra,
        })
    }
}

/// The inscription a native publish at `assigned`'s sequence carries: the old
/// position naming the blocks snapshot `blocks`, this profile's parameters,
/// and the link to `previous`.
pub fn inscription(
    old: &OldInscription,
    assigned: &Assigned,
    previous: Option<Link>,
    blocks: &Digest,
) -> Result<Inscription, Error> {
    let sequence = old.sequence;
    let malformed = |error: dolos_snapshot::Error| Error::Malformed {
        sequence,
        reason: error.to_string(),
    };
    let position = dolos_snapshot::read_position(&old.position).map_err(malformed)?;
    let position = dolos_snapshot::position(
        &position.network,
        &position.point,
        position.epoch,
        Some(blocks),
    )
    .map_err(malformed)?;

    let mut inscription = Inscription::new(
        &DolosProfile,
        sequence,
        position,
        dolos_snapshot::parameters(),
        dolos_snapshot::compression(),
    );
    inscription.previous = previous;
    inscription.diff = assigned.diff.clone();
    inscription.tip = assigned.tip.clone();

    inscription.validate().map_err(|source| Error::Documents {
        sequence,
        source: Box::new(source),
    })?;

    Ok(inscription)
}

/// The plan file's first line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlanHeader {
    pub rebuild_plan: u64,
    /// The old repository the blobs are in.
    pub source: String,
    pub first: u64,
    pub last: u64,
}

/// One sequence of a rebuild plan, a line of the plan file after its header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Planned {
    pub sequence: u64,
    pub inscription_digest: Digest,
    pub inscription: Inscription,
    pub diff_manifest: Manifest,
    pub tip_manifest: Option<Manifest>,
    /// Whether the registry keeps this sequence's tip, so its manifest and
    /// blobs are published.
    pub kept_tip: bool,
    /// The blobs the published manifests name, each by its digest and size in
    /// the old repository: the diff manifest's, and a kept tip's.
    pub blobs: Vec<Blob>,
}

/// `inscription`'s plan line: its two manifests over the blobs `old` holds.
pub fn planned(old: &OldSequence, inscription: Inscription, kept: bool) -> Result<Planned, Error> {
    let sequence = inscription.sequence;
    let documents = |source| Error::Documents {
        sequence,
        source: Box::new(source),
    };

    let written: Vec<WrittenLayer> = inscription
        .layers()
        .map(|layer| {
            let blob = old.blobs[&layer.diff_id];
            WrittenLayer {
                descriptor: layer.clone(),
                digests: LayerDigests {
                    diff_id: layer.diff_id,
                    blob_digest: blob.digest,
                    uncompressed_size: layer.uncompressed_size,
                    compressed_size: blob.size,
                },
            }
        })
        .collect();
    let (manifests, _) = build_manifests(&inscription, &written).map_err(documents)?;
    for manifest in std::iter::once(&manifests.diff).chain(&manifests.tip) {
        manifest_bytes(manifest).map_err(documents)?;
    }

    let kept_tip = kept && manifests.tip.is_some();
    let published = manifests.diff.layers.iter().chain(
        manifests
            .tip
            .iter()
            .filter(|_| kept_tip)
            .flat_map(|tip| &tip.layers),
    );
    let blobs = published
        .map(|layer| {
            let digest = layer.digest.parse().map_err(documents)?;
            Ok(Blob {
                digest,
                size: layer.size as u64,
            })
        })
        .collect::<Result<_, Error>>()?;

    Ok(Planned {
        sequence,
        inscription_digest: inscription.digest().map_err(documents)?,
        inscription,
        diff_manifest: manifests.diff,
        tip_manifest: manifests.tip,
        kept_tip,
        blobs,
    })
}

/// Read a blocks journal: a line per sequence, its number and the inscription
/// digest of the blocks snapshot there, apart by a space.
pub fn read_journal(text: &str) -> Result<BTreeMap<u64, Digest>, Error> {
    let line = |(index, text): (usize, &str)| {
        let refused = || Error::Line {
            what: "blocks journal",
            expected: "a sequence and a digest",
            line: index + 1,
            text: text.to_owned(),
        };
        let (sequence, digest) = text.split_once(' ').ok_or_else(refused)?;
        Ok((
            sequence.parse().map_err(|_| refused())?,
            digest.parse().map_err(|_| refused())?,
        ))
    };

    let mut journal = BTreeMap::new();
    for (index, text) in text
        .lines()
        .enumerate()
        .filter(|(_, text)| !text.trim().is_empty())
    {
        let (sequence, digest) = line((index, text.trim()))?;
        if journal
            .insert(sequence, digest)
            .is_some_and(|other| other != digest)
        {
            return Err(Error::Line {
                what: "blocks journal",
                expected: "the only digest of its sequence",
                line: index + 1,
                text: text.to_owned(),
            });
        }
    }

    Ok(journal)
}

/// Read a kept-tips file: an epoch per line, as the retained-epochs file was.
pub fn read_epochs(text: &str) -> Result<BTreeSet<u64>, Error> {
    text.lines()
        .enumerate()
        .map(|(index, line)| (index + 1, line.trim()))
        .filter(|(_, line)| !line.is_empty())
        .map(|(line, text)| {
            text.parse().map_err(|_| Error::Line {
                what: "kept-tips file",
                expected: "an epoch",
                line,
                text: text.to_owned(),
            })
        })
        .collect()
}

/// What `stelae-publisher rebuild` reads.
pub struct Options {
    /// The old lineage's repository, `oci://…/cardano/<network>`.
    pub source: Repository,
    pub insecure: bool,
    pub auth: Auth,
    /// The blocks journal, without which no inscription is built.
    pub blocks: Option<BTreeMap<u64, Digest>>,
    /// The epochs whose tips the registry keeps; the newest tip is kept too.
    pub kept: BTreeSet<u64>,
}

/// What a rebuild found, for the command's summary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    pub first: u64,
    pub last: u64,
    pub diff_layers: usize,
    pub tip_layers: usize,
    pub dropped_blocks: usize,
    pub dropped_dumps: usize,
    /// The kept tips, of sequences the lineage holds.
    pub kept: Vec<u64>,
    /// Each sequence's new inscription digest, when the journal was given.
    pub digests: Vec<(u64, Digest)>,
}

/// Read the old lineage in `options.source`, assign and self-check every
/// sequence on the network of `genesis`, and with the blocks journal write the
/// plan to `plan`.
pub fn run(
    options: &Options,
    genesis: &Genesis,
    mut plan: Option<&mut dyn Write>,
) -> Result<Summary, Error> {
    let repository = options.source.to_string();
    let read = |what: String| {
        let repository = repository.clone();
        move |source| Error::Read {
            repository,
            what,
            source: Box::new(source),
        }
    };

    let registry = Registry::open(
        &options.source,
        stelae::oci::Options {
            insecure: options.insecure,
            auth: options.auth.clone(),
            ..Default::default()
        },
    )
    .map_err(read("the repository".into()))?;

    let sequences = sequences(
        &repository,
        registry.tags().map_err(read("its tags".into()))?,
    )?;
    let (first, last) = (sequences[0], sequences[sequences.len() - 1]);
    let pull = |sequence: u64| {
        let tag = format!("epoch-{sequence}");
        let document = registry.pull_raw(&tag).map_err(read(tag))?;
        OldSequence::read(sequence, &document)
    };

    let newest = pull(last)?;
    let summary = geometry(&newest.inscription)?;
    let mut rebuild = Rebuild::new(
        &summary,
        dolos_cardano::mutable_slots(genesis),
        u64::from(genesis.network_magic()),
    );
    let mut found = Summary {
        first,
        last,
        ..Default::default()
    };
    let mut previous: Option<Link> = None;

    if let Some(plan) = plan.as_mut().filter(|_| options.blocks.is_some()) {
        let header = PlanHeader {
            rebuild_plan: PLAN_FORMAT,
            source: repository.clone(),
            first,
            last,
        };
        write_line(plan, &header)?;
    }

    for sequence in first..=last {
        let old = match sequence == last {
            true => newest.clone(),
            false => pull(sequence)?,
        };
        let assigned = rebuild.assign(&old)?;
        let kept = sequence == last || options.kept.contains(&sequence);

        found.diff_layers += assigned.diff.len();
        found.tip_layers += assigned.tip.len();
        found.dropped_blocks += assigned.dropped_blocks;
        found.dropped_dumps += assigned.dropped_dumps;
        if kept {
            found.kept.push(sequence);
        }

        let Some(journal) = &options.blocks else {
            continue;
        };
        let blocks = journal.get(&sequence).ok_or(Error::NoBlocks { sequence })?;
        let inscription = inscription(&old.inscription, &assigned, previous, blocks)?;
        let planned = planned(&old, inscription, kept)?;

        previous = Some(Link {
            sequence,
            inscription_digest: planned.inscription_digest,
        });
        found.digests.push((sequence, planned.inscription_digest));
        if let Some(plan) = plan.as_mut() {
            write_line(plan, &planned)?;
        }
    }

    Ok(found)
}

/// The sequences of `tags`, ascending, refusing a lineage that is not of the
/// old format or skips one.
fn sequences(repository: &str, tags: Vec<String>) -> Result<Vec<u64>, Error> {
    let mut sequences = Vec::new();

    for tag in tags {
        if tag.ends_with(".tip") {
            return Err(Error::NotOld {
                repository: repository.to_owned(),
                tag,
            });
        }
        let sequence = tag.strip_prefix("epoch-").and_then(|n| n.parse().ok());
        if let Some(sequence) = sequence.filter(|sequence| {
            checked_tag_for_sequence(&DolosProfile, *sequence).is_ok_and(|named| named == tag)
        }) {
            sequences.push(sequence);
        }
    }

    sequences.sort_unstable();
    let (Some(&first), Some(&last)) = (sequences.first(), sequences.last()) else {
        return Err(Error::Empty {
            repository: repository.to_owned(),
        });
    };
    if let Some(missing) =
        (first..=last).find(|sequence| sequences.binary_search(sequence).is_err())
    {
        return Err(Error::Gap {
            repository: repository.to_owned(),
            first,
            last,
            missing,
        });
    }

    Ok(sequences)
}

fn write_line(plan: &mut dyn Write, line: &impl Serialize) -> Result<(), Error> {
    serde_json::to_writer(&mut *plan, line)
        .map_err(std::io::Error::from)
        .and_then(|()| plan.write_all(b"\n"))
        .map_err(Error::Write)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// An old inscription at `sequence` whose `indexes` layers open each epoch
    /// at the slot `start` gives.
    fn indexed(sequence: u64, start: impl Fn(u64) -> u64) -> OldInscription {
        let layers = (0..=sequence)
            .map(|epoch| LayerDescriptor {
                kind: INDEXES.to_owned(),
                media_type: "application/vnd.dolos.stelae.indexes.v1+zstd".to_owned(),
                diff_id: Digest::compute(epoch.to_be_bytes()),
                records: 1,
                uncompressed_size: 1,
                scope: json!({"epoch": epoch, "startSlot": start(epoch), "endSlot": start(epoch + 1) - 1}),
            })
            .collect();
        OldInscription {
            schema: SCHEMA_VERSION,
            profile: ProfileRef {
                name: PROFILE_NAME.to_owned(),
                version: PROFILE_VERSION,
            },
            sequence,
            position: json!({}),
            parameters: json!({}),
            compression: dolos_snapshot::compression(),
            history: Vec::new(),
            layers,
        }
    }

    /// Mainnet's slots: 208 Byron epochs of 21,600, then epochs of 432,000.
    fn mainnet(epoch: u64) -> u64 {
        match epoch {
            ..=208 => epoch * 21_600,
            _ => 208 * 21_600 + (epoch - 208) * 432_000,
        }
    }

    /// Mainnet's eras as Dolos's store records them.
    fn recorded() -> ChainSummary {
        let byron = EraState {
            start: EraBoundary {
                epoch: 0,
                slot: 0,
                timestamp: 1_506_203_091,
            },
            end: Some(EraBoundary {
                epoch: 208,
                slot: 4_492_800,
                timestamp: 1_596_059_091,
            }),
            epoch_length: 21_600,
            slot_length: 20,
            protocol: 0,
        };
        let shelley = EraState {
            start: byron.end.clone().unwrap(),
            end: None,
            epoch_length: 432_000,
            slot_length: 1,
            protocol: 2,
        };
        let mut summary = ChainSummary::default();
        summary.append_era(0, byron);
        summary.append_era(2, shelley);
        summary
    }

    /// Across the change of epoch length, the geometry read off the scopes
    /// seals every epoch where the store's eras do.
    #[test]
    fn the_indexes_scopes_give_the_eras_the_seals_are_ruled_by() {
        let (summary, recorded) = (geometry(&indexed(214, mainnet)).unwrap(), recorded());
        let window = 3 * 2160 * 20;
        let (read, stored) = (Rule::new(&summary, window), Rule::new(&recorded, window));

        for epoch in 0..230 {
            assert_eq!(summary.epoch_start(epoch), mainnet(epoch), "epoch {epoch}");
            assert_eq!(read.seal(epoch), stored.seal(epoch), "epoch {epoch}");
        }
        assert_eq!(read.seal(207), 209);
    }

    #[test]
    fn a_lineage_without_every_epochs_indexes_has_no_geometry() {
        let mut inscription = indexed(6, |epoch| epoch * 100);
        inscription.layers.remove(3);
        assert!(matches!(
            geometry(&inscription),
            Err(Error::Geometry { sequence: 6 })
        ));
        assert!(matches!(
            geometry(&indexed(0, |epoch| epoch * 100)),
            Err(Error::Geometry { sequence: 0 })
        ));
    }

    #[test]
    fn a_journal_names_each_sequence_once() {
        let (one, two) = (Digest::compute(b"one"), Digest::compute(b"two"));
        let text = format!("1 {one}\n\n2 {two}\n1 {one}\n");
        assert_eq!(
            read_journal(&text).unwrap(),
            BTreeMap::from([(1, one), (2, two)])
        );

        let conflicting = format!("1 {one}\n1 {two}\n");
        assert!(matches!(
            read_journal(&conflicting),
            Err(Error::Line { line: 2, .. })
        ));
        assert!(matches!(
            read_journal(&format!("1 {one}\nepoch-2 {two}\n")),
            Err(Error::Line { line: 2, .. })
        ));
        assert!(matches!(
            read_journal("1 sha256:00"),
            Err(Error::Line { line: 1, .. })
        ));
    }

    #[test]
    fn a_kept_tips_file_names_an_epoch_per_line() {
        assert_eq!(
            read_epochs("4\n\n208 \n4\n").unwrap(),
            BTreeSet::from([4, 208])
        );
        assert!(matches!(
            read_epochs("4\nfour\n"),
            Err(Error::Line { line: 2, .. })
        ));
    }

    #[test]
    fn only_a_contiguous_lineage_of_epoch_tags_is_read() {
        let tags = |names: &[&str]| names.iter().map(|name| name.to_string()).collect();
        assert_eq!(
            sequences(
                "r",
                tags(&[
                    "latest", "epoch-2", "epoch-10", "epoch-01", "epoch-3", "epoch-1", "epoch-4",
                    "epoch-5", "epoch-6", "epoch-7", "epoch-8", "epoch-9"
                ])
            )
            .unwrap(),
            (1..=10).collect::<Vec<_>>()
        );
        assert!(matches!(
            sequences("r", tags(&["epoch-1", "epoch-3"])),
            Err(Error::Gap { missing: 2, .. })
        ));
        assert!(matches!(
            sequences("r", tags(&["latest"])),
            Err(Error::Empty { .. })
        ));
    }
}
