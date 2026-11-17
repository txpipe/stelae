//! The rebuild of a lineage published before the diff chain, over the
//! documents release v0.6.0 published (`tests/data/rebuild-v0.6.0`), held to
//! what a native publish of the same boundaries publishes.
//!
//! The fixture is served by the loopback registry, read-only: any request but
//! a read is refused and counted.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock, Once,
    },
};

use serde_json::{json, Value};
use stelae::{inscription::Inscription, manifest::Manifest, oci::RawDocument, Digest};
use stelae_cardano::{
    rebuild::{self, Error, OldSequence, PlanHeader, Planned, Rebuild, Summary},
    testing::{
        chain::Chain,
        loopback::{Loopback, Verdict},
    },
    Auth, Genesis,
};

/// The fixture's lineage: v0.6.0's parity reference, `epoch-1` to `epoch-8`.
const UNTIL: u64 = 8;
/// The epoch no new asset appears in, as v0.6.0's reference chain has it.
const HOLE: usize = 2;
/// The epochs v0.6.0 retained a dump of.
const RETAINED: [u64; 2] = [3, 7];
const NAME: &str = "cardano/old";

fn provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/rebuild-v0.6.0")
}

/// The chain the fixture was published from, and what publishing each of its
/// boundaries natively gives, once for the test binary.
struct Reference {
    chain: Chain,
    native: Vec<Inscription>,
    digests: Vec<Digest>,
}

static REFERENCE: LazyLock<Reference> = LazyLock::new(|| {
    let chain = Chain::delegating(UNTIL as usize + 2, &[HOLE]);
    let (mut native, mut digests, mut previous) = (Vec::new(), Vec::new(), None);
    for sequence in 1..=UNTIL {
        let document = chain.frozen(sequence, previous.as_ref());
        native.push(Inscription::parse(&document.canonical).unwrap());
        digests.push(document.identity);
        previous = Some(document);
    }
    Reference {
        chain,
        native,
        digests,
    }
});

impl Reference {
    fn genesis(&self) -> &Genesis {
        &self.chain.genesis
    }

    /// The blocks journal of the chain's blocks lineage.
    fn journal(&self) -> BTreeMap<u64, Digest> {
        (1..=UNTIL)
            .map(|sequence| (sequence, self.chain.blocks_digest(sequence)))
            .collect()
    }
}

/// The fixture as a repository of a loopback that refuses and counts writes.
struct Served {
    root: tempfile::TempDir,
    loopback: Loopback,
    writes: Arc<AtomicU64>,
}

fn serve() -> Served {
    let root = tempfile::tempdir().unwrap();
    copy_dir(
        &fixture().join("manifests"),
        &root.path().join(NAME).join("manifests"),
    );
    copy_dir(
        &fixture().join("blobs"),
        &root.path().join(NAME).join("blobs"),
    );
    let loopback = Loopback::serve(root.path()).unwrap();
    let writes = Arc::new(AtomicU64::new(0));
    let counted = writes.clone();
    loopback.rule(move |seen| match seen.method {
        "GET" | "HEAD" => Verdict::Proceed,
        _ => {
            counted.fetch_add(1, Ordering::SeqCst);
            Verdict::Refuse(405)
        }
    });
    Served {
        root,
        loopback,
        writes,
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        match entry.file_type().unwrap().is_dir() {
            true => copy_dir(&entry.path(), &target),
            false => {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}

/// Every file under `dir`, by its path relative to it.
fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![dir.to_owned()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(next).unwrap() {
            let path = entry.unwrap().path();
            match path.is_dir() {
                true => pending.push(path),
                false => {
                    let relative = path.strip_prefix(dir).unwrap().to_owned();
                    found.insert(relative, std::fs::read(&path).unwrap());
                }
            }
        }
    }
    found
}

fn options(
    served: &Served,
    blocks: Option<BTreeMap<u64, Digest>>,
    kept: &[u64],
) -> rebuild::Options {
    rebuild::Options {
        source: served.loopback.repository(NAME).parse().unwrap(),
        insecure: true,
        auth: Auth::Anonymous,
        blocks,
        kept: kept.iter().copied().collect(),
    }
}

/// A rebuild of the served fixture, and the plan it wrote.
fn run(served: &Served, blocks: bool, kept: &[u64]) -> (Summary, Vec<u8>) {
    provider();
    let reference = &*REFERENCE;
    let options = options(served, blocks.then(|| reference.journal()), kept);
    let mut plan = Vec::new();
    let summary = rebuild::run(&options, reference.genesis(), Some(&mut plan)).unwrap();
    (summary, plan)
}

fn read_plan(plan: &[u8]) -> (PlanHeader, Vec<Planned>) {
    let mut lines = plan
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty());
    let header = serde_json::from_slice(lines.next().unwrap()).unwrap();
    let planned = lines
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    (header, planned)
}

/// The fixture's `epoch-{sequence}`, read off disk as a pull reads it.
fn old(sequence: u64) -> OldSequence {
    let bytes = std::fs::read(fixture().join(format!("manifests/tags/epoch-{sequence}"))).unwrap();
    let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
    let hex = manifest
        .config
        .digest
        .trim_start_matches("sha256:")
        .to_owned();
    let config = std::fs::read(fixture().join("blobs/sha256").join(hex)).unwrap();
    let document = RawDocument {
        digest: Digest::compute(&bytes),
        manifest,
        config,
    };
    OldSequence::read(sequence, &document).unwrap()
}

fn olds() -> Vec<OldSequence> {
    (1..=UNTIL).map(old).collect()
}

fn window(layer: &stelae::LayerDescriptor) -> Option<u64> {
    layer.scope.get("epoch").and_then(Value::as_u64)
}

/// The fixture is the old lineage of the chain the native publishes replay,
/// and holds every case the rebuild is about, or the parity below could pass
/// over nothing.
#[test]
fn the_fixture_is_the_old_lineage_of_the_reference_chain() {
    let reference = &*REFERENCE;
    let recorded: Vec<String> =
        serde_json::from_slice(&std::fs::read(fixture().join("chain.json")).unwrap()).unwrap();
    let replayed: Vec<String> = reference
        .chain
        .blocks
        .iter()
        .map(|raw| Digest::compute(raw.as_slice()).to_string())
        .collect();
    assert_eq!(recorded, replayed);

    let olds = olds();
    let dumps = |old: &OldSequence| -> BTreeSet<u64> {
        old.inscription
            .layers
            .iter()
            .filter(|layer| dolos_snapshot::is_state_kind(&layer.kind))
            .filter_map(window)
            .collect()
    };
    assert_eq!(dumps(&olds[2]), BTreeSet::from([3]));
    assert_eq!(dumps(&olds[7]), BTreeSet::from(RETAINED));

    // A window of `log-account-epochs` rewritten at its settling, two
    // sequences after it opened.
    let account = |sequence: u64, epoch: u64| {
        olds[sequence as usize - 1]
            .inscription
            .layers
            .iter()
            .find(|layer| layer.kind == "log-account-epochs" && window(layer) == Some(epoch))
            .map(|layer| layer.diff_id)
    };
    assert!((2..UNTIL - 1).any(|epoch| matches!(
        (account(epoch + 1, epoch), account(epoch + 2, epoch)),
        (Some(open), Some(settled)) if open != settled
    )));

    // Entity layers, which only a seal writes.
    assert!(olds[UNTIL as usize - 1]
        .inscription
        .layers
        .iter()
        .any(|layer| layer.kind.starts_with("entity-")));
}

/// Done when 1: every inscription the plan holds is the one a native publish
/// of the same boundary, chained the same way and naming the same blocks
/// snapshot, gives.
#[test]
fn a_rebuilt_lineage_is_what_a_native_publish_of_its_boundaries_publishes() {
    let reference = &*REFERENCE;
    let served = serve();
    let (summary, plan) = run(&served, true, &[]);
    let (header, planned) = read_plan(&plan);

    assert_eq!(header.rebuild_plan, rebuild::PLAN_FORMAT);
    assert_eq!((header.first, header.last), (1, UNTIL));
    assert_eq!(header.source, served.loopback.repository(NAME));
    assert_eq!(planned.len(), UNTIL as usize);

    for (planned, (native, digest)) in planned
        .iter()
        .zip(reference.native.iter().zip(&reference.digests))
    {
        assert_eq!(
            &planned.inscription, native,
            "sequence {}",
            planned.sequence
        );
        assert_eq!(&planned.inscription_digest, digest);
    }

    let expected: Vec<(u64, Digest)> = (1..=UNTIL).zip(reference.digests.iter().copied()).collect();
    assert_eq!(summary.digests, expected);
}

/// Done when 2: a rebuild sends the registry nothing but reads, and the
/// repository holds after it what it held before.
#[test]
fn a_rebuild_writes_nothing_to_the_registry() {
    let served = serve();
    let before = files(served.root.path());
    run(&served, true, &RETAINED);

    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
    assert_eq!(files(served.root.path()), before);
    assert_eq!(served.loopback.accounting(NAME), Default::default());
}

/// The diff manifest's blobs are always published, a tip's only where it is
/// kept: at the configured epochs and at the newest.
#[test]
fn kept_tips_and_the_newest_are_published_with_their_blobs() {
    let served = serve();
    let (summary, plan) = run(&served, true, &[RETAINED[0], RETAINED[1], 40]);
    let (_, planned) = read_plan(&plan);

    assert_eq!(summary.kept, vec![3, 7, UNTIL]);
    for planned in &planned {
        let kept = summary.kept.contains(&planned.sequence);
        assert_eq!(planned.kept_tip, kept, "sequence {}", planned.sequence);
        let tip = planned.tip_manifest.as_ref().unwrap();
        let mut named: Vec<&Manifest> = vec![&planned.diff_manifest];
        named.extend(kept.then_some(tip));
        let blobs: Vec<String> = planned
            .blobs
            .iter()
            .map(|blob| blob.digest.to_string())
            .collect();
        let layers: Vec<String> = named
            .iter()
            .flat_map(|manifest| &manifest.layers)
            .map(|layer| layer.digest.clone())
            .collect();
        assert_eq!(blobs, layers, "sequence {}", planned.sequence);
        assert_eq!(tip.config, planned.diff_manifest.config);
    }
}

/// Without the blocks journal the same assignment is self-checked and no
/// inscription is built.
#[test]
fn without_the_journal_only_the_assignment_and_the_self_check_run() {
    let served = serve();
    let (with, _) = run(&served, true, &[]);
    let (without, plan) = run(&served, false, &[]);

    assert!(plan.is_empty());
    assert!(without.digests.is_empty());
    assert_eq!(
        Summary {
            digests: with.digests.clone(),
            ..without
        },
        with
    );
}

/// Assign the fixture's sequences up to `through`, `edit` changing the last
/// one's layers first.
fn assign_edited(
    through: u64,
    edit: impl FnOnce(&mut Vec<stelae::LayerDescriptor>),
) -> Result<(), Error> {
    let reference = &*REFERENCE;
    let mut olds = olds();
    olds.truncate(through as usize);
    edit(&mut olds.last_mut().unwrap().inscription.layers);
    let newest = old(UNTIL);
    let summary = rebuild::geometry(&newest.inscription).unwrap();
    let mut rebuild = Rebuild::new(
        &summary,
        dolos_cardano::mutable_slots(reference.genesis()),
        u64::from(reference.genesis().network_magic()),
    );
    for old in &olds {
        rebuild.assign(old)?;
    }
    Ok(())
}

/// An old snapshot without a layer an earlier diff lists, which the chain
/// would still restore: refused, naming it.
#[test]
fn a_layer_the_old_lineage_lost_is_refused_by_the_self_check() {
    let refused = assign_edited(5, |layers| {
        layers.retain(|layer| !(layer.kind == "indexes" && window(layer) == Some(0)));
    })
    .unwrap_err();

    let Error::SelfCheck {
        sequence,
        missing,
        extra,
    } = &refused
    else {
        panic!("{refused}");
    };
    assert_eq!(*sequence, 5);
    assert!(missing.is_empty(), "{missing:?}");
    assert_eq!(extra.len(), 1, "{extra:?}");
    assert!(extra[0].starts_with("indexes "), "{refused}");
    assert!(refused.to_string().contains("\"epoch\":0"), "{refused}");
}

/// An old snapshot with a layer no array of the chain holds: refused, naming
/// it.
#[test]
fn a_layer_the_old_lineage_added_is_refused_by_the_self_check() {
    let refused = assign_edited(5, |layers| {
        let mut added = layers
            .iter()
            .find(|layer| layer.kind == "entity-asset")
            .unwrap()
            .clone();
        // Sealed by the boundary into 6, so no snapshot before it lists it.
        added.scope = json!({"epoch": 4, "startSlot": 345600, "endSlot": 431999});
        added.diff_id = Digest::compute(b"a layer the chain does not hold");
        layers.push(added);
    })
    .unwrap_err();

    let Error::SelfCheck {
        sequence,
        missing,
        extra,
    } = &refused
    else {
        panic!("{refused}");
    };
    assert_eq!(*sequence, 5);
    assert!(extra.is_empty(), "{extra:?}");
    assert_eq!(missing.len(), 1, "{missing:?}");
    assert!(missing[0].starts_with("entity-asset "), "{refused}");
}

/// A lineage read out of order, or from a sequence whose predecessors are
/// gone, is refused before any layer is assigned.
#[test]
fn a_sequence_that_does_not_follow_the_last_is_refused() {
    let reference = &*REFERENCE;
    let olds = olds();
    let summary = rebuild::geometry(&olds[UNTIL as usize - 1].inscription).unwrap();
    let rebuild = || {
        Rebuild::new(
            &summary,
            dolos_cardano::mutable_slots(reference.genesis()),
            u64::from(reference.genesis().network_magic()),
        )
    };

    let mut skipping = rebuild();
    skipping.assign(&olds[0]).unwrap();
    assert!(matches!(
        skipping.assign(&olds[2]),
        Err(Error::Unlinked {
            sequence: 3,
            previous: 1
        })
    ));

    assert!(matches!(
        rebuild().assign(&olds[1]),
        Err(Error::Truncated {
            sequence: 2,
            follows: 1
        })
    ));

    let mut other = rebuild();
    let mut moved = olds[1].clone();
    moved.inscription.history[0].inscription_digest = Digest::compute(b"another epoch-1");
    other.assign(&olds[0]).unwrap();
    assert!(matches!(other.assign(&moved), Err(Error::Unlinked { .. })));
}

/// A repository that skips a sequence, or holds a tag of the diff chain, is
/// refused before a document is read.
#[test]
fn a_repository_that_is_not_a_whole_old_lineage_is_refused() {
    provider();
    let reference = &*REFERENCE;
    let served = serve();
    let tags = served.root.path().join(NAME).join("manifests/tags");

    std::fs::copy(tags.join("epoch-8"), tags.join("epoch-8.tip")).unwrap();
    let refused = rebuild::run(&options(&served, None, &[]), reference.genesis(), None);
    assert!(matches!(refused, Err(Error::NotOld { ref tag, .. }) if tag == "epoch-8.tip"));

    std::fs::remove_file(tags.join("epoch-8.tip")).unwrap();
    std::fs::remove_file(tags.join("epoch-4")).unwrap();
    let refused = rebuild::run(&options(&served, None, &[]), reference.genesis(), None);
    assert!(matches!(refused, Err(Error::Gap { missing: 4, .. })));
}

/// A journal without the blocks snapshot of a sequence leaves it unbuildable.
#[test]
fn a_journal_without_a_sequence_is_refused() {
    provider();
    let reference = &*REFERENCE;
    let served = serve();
    let mut journal = reference.journal();
    journal.remove(&5);
    let refused = rebuild::run(
        &options(&served, Some(journal), &[]),
        reference.genesis(),
        None,
    );
    assert!(matches!(refused, Err(Error::NoBlocks { sequence: 5 })));
}

/// The fixture's sequences assigned in order from `first`, each changed by
/// `edit` before.
fn assigned_from(first: u64, edit: impl Fn(&mut OldSequence)) -> Vec<rebuild::Assigned> {
    let reference = &*REFERENCE;
    let summary = rebuild::geometry(&old(UNTIL).inscription).unwrap();
    let mut rebuild = Rebuild::new(
        &summary,
        dolos_cardano::mutable_slots(reference.genesis()),
        u64::from(reference.genesis().network_magic()),
    );
    (first..=UNTIL)
        .map(|sequence| {
            let mut old = old(sequence);
            edit(&mut old);
            rebuild.assign(&old).unwrap()
        })
        .collect()
}

/// Each array is in the order a native export writes, not the order the old
/// inscription listed its layers in.
#[test]
fn the_arrays_keep_the_export_order_whatever_the_old_order() {
    let reversed = assigned_from(1, |old| old.inscription.layers.reverse());
    assert_eq!(reversed, assigned_from(1, |_| {}));
}

/// A lineage that starts at a later sequence lists in its first diff every
/// layer final by its cut, as a native first publish there does.
#[test]
fn a_lineage_starting_later_is_what_a_native_first_publish_there_publishes() {
    const FIRST: u64 = 3;
    let reference = &*REFERENCE;
    let assigned = assigned_from(FIRST, |old| {
        if old.inscription.sequence == FIRST {
            old.inscription.history.clear();
        }
    });

    let mut old = old(FIRST);
    old.inscription.history.clear();
    let rebuilt = rebuild::inscription(
        &old.inscription,
        &assigned[0],
        None,
        &reference.chain.blocks_digest(FIRST),
    )
    .unwrap();
    let native = reference.chain.frozen(FIRST, None);
    assert_eq!(rebuilt, Inscription::parse(&native.canonical).unwrap());

    // The same layers the lineage from 1 lists up to it.
    let from_one = assigned_from(1, |_| {});
    let listed = |assigned: &[rebuild::Assigned]| -> BTreeSet<Digest> {
        assigned
            .iter()
            .flat_map(|a| &a.diff)
            .map(|layer| layer.diff_id)
            .collect()
    };
    assert_eq!(listed(&assigned[..1]), listed(&from_one[..FIRST as usize]));
    assert_eq!(assigned[0].tip, from_one[FIRST as usize - 1].tip);
}

/// The fixture's `epoch-{sequence}` as bytes, for a test to spoil.
fn document(sequence: u64) -> RawDocument {
    let bytes = std::fs::read(fixture().join(format!("manifests/tags/epoch-{sequence}"))).unwrap();
    let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
    let hex = manifest
        .config
        .digest
        .trim_start_matches("sha256:")
        .to_owned();
    RawDocument {
        digest: Digest::compute(&bytes),
        manifest,
        config: std::fs::read(fixture().join("blobs/sha256").join(hex)).unwrap(),
    }
}

/// An old document is read only as a reader of the old format read it:
/// canonical, under the digest its manifest names, at its own sequence, and
/// listed by its manifest layer for layer.
#[test]
fn an_old_document_is_held_to_its_manifest() {
    let malformed = |sequence, document: &RawDocument| {
        matches!(
            OldSequence::read(sequence, document),
            Err(Error::Malformed { .. })
        )
    };

    let mut pretty = document(3);
    let value: Value = serde_json::from_slice(&pretty.config).unwrap();
    pretty.config = serde_json::to_vec_pretty(&value).unwrap();
    pretty.manifest.config.digest = Digest::compute(&pretty.config).to_string();
    assert!(malformed(3, &pretty));

    let mut other = document(3);
    other.config = document(4).config;
    assert!(malformed(3, &other));

    assert!(malformed(4, &document(3)));

    let mut swapped = document(3);
    swapped.manifest.layers.swap(0, 1);
    assert!(malformed(3, &swapped));

    let mut short = document(3);
    short.manifest.layers.pop();
    assert!(malformed(3, &short));
}

/// What a new inscription keeps of an old one has to be this profile's, on
/// the configured network.
#[test]
fn an_old_document_of_other_parameters_or_another_network_is_refused() {
    let reference = &*REFERENCE;
    let summary = rebuild::geometry(&old(UNTIL).inscription).unwrap();
    let window = dolos_cardano::mutable_slots(reference.genesis());
    let magic = u64::from(reference.genesis().network_magic());

    let mut other = old(1);
    other.inscription.parameters["indexKeyHash"] = json!("sha256");
    assert!(matches!(
        Rebuild::new(&summary, window, magic).assign(&other),
        Err(Error::Malformed { sequence: 1, .. })
    ));

    // Only `stateEpochs` goes: it is what an old document has and a new one
    // does not.
    let mut without = old(1);
    without.inscription.parameters["stateEpochs"] = json!([]);
    Rebuild::new(&summary, window, magic)
        .assign(&without)
        .unwrap();

    assert!(matches!(
        Rebuild::new(&summary, window, magic + 1).assign(&old(1)),
        Err(Error::OtherNetwork { sequence: 1, found, .. }) if found == magic
    ));
}
