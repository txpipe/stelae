//! `rebuild --write` of the old lineage v0.6.0 published, its layers given
//! stand-in blobs, from repository `net` of a loopback registry into
//! `net/dolos`.
//!
//! The loopback refuses and counts every write outside `net/dolos`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Once,
    },
};

use serde_json::Value;
use stelae::{
    manifest::DIFF_ID_ANNOTATION,
    oci::{self, Registry},
    Digest, SnapshotReader as _,
};
use stelae_cardano::{
    rebuild::{
        self,
        write::{self, Error, Plan, Written},
    },
    testing::{
        loopback::{Loopback, Verdict},
        rebuild::{lay_old_lineage, UNTIL},
        Node,
    },
    Auth,
};

const OLD: &str = "net";
const NEW: &str = "net/dolos";

fn provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// The old lineage on a loopback, and what it wrote outside `net/dolos`.
struct Served {
    root: tempfile::TempDir,
    loopback: Loopback,
    outside: Arc<AtomicU64>,
}

impl Served {
    fn new() -> Self {
        provider();
        let root = tempfile::tempdir().unwrap();
        lay_old_lineage(root.path(), OLD);
        let served = Self {
            loopback: Loopback::serve(root.path()).unwrap(),
            root,
            outside: Arc::new(AtomicU64::new(0)),
        };
        served.guard(|_, _| Verdict::Proceed);
        served
    }

    /// Refuse and count every write outside `net/dolos`, and let `rule`
    /// judge the rest.
    fn guard(&self, rule: impl Fn(&str, &str) -> Verdict + Send + Sync + 'static) {
        let outside = self.outside.clone();
        let prefix = format!("/v2/{NEW}/");
        self.loopback.rule(move |seen| match seen.method {
            "GET" | "HEAD" => rule(seen.method, seen.path),
            _ if !seen.path.starts_with(&prefix) => {
                outside.fetch_add(1, Ordering::SeqCst);
                Verdict::Refuse(405)
            }
            _ => rule(seen.method, seen.path),
        });
    }

    fn repository(&self, name: &str) -> String {
        self.loopback.repository(name)
    }

    /// The rebuild plan of the old lineage, keeping the tips at `kept`.
    fn plan(&self, kept: &[u64]) -> String {
        let node = Node::new();
        let genesis = stelae_cardano::config::genesis(&node.config).unwrap();
        let options = rebuild::Options {
            source: self.repository(OLD).parse().unwrap(),
            insecure: true,
            auth: Auth::Anonymous,
            blocks: Some(
                (1..=UNTIL)
                    .map(|sequence| (sequence, Digest::compute(sequence.to_be_bytes())))
                    .collect(),
            ),
            kept: kept.iter().copied().collect(),
        };
        let mut plan = Vec::new();
        rebuild::run(&options, &genesis, Some(&mut plan)).unwrap();
        String::from_utf8(plan).unwrap()
    }

    fn write(&self, plan: &Plan) -> Result<Written, Error> {
        write::write(plan, &options())
    }

    /// What the old repository holds: its tags with their manifests, and its
    /// blobs.
    fn old(&self) -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, u64>) {
        (self.loopback.manifests(OLD), self.loopback.blobs(OLD))
    }

    fn open(&self, name: &str) -> Registry {
        Registry::open(
            &self.repository(name).parse().unwrap(),
            oci::Options {
                insecure: true,
                ..Default::default()
            },
        )
        .unwrap()
    }
}

/// The write options of the network the fixture lineage is on.
fn options() -> write::Options {
    let genesis = stelae_cardano::config::genesis(&Node::new().config).unwrap();
    write::Options {
        network_magic: u64::from(genesis.network_magic()),
        root: None,
        insecure: true,
        auth: Auth::Anonymous,
        direct_s3: None,
        scratch_dir: None,
    }
}

fn tags(written: &[&str]) -> BTreeSet<String> {
    written.iter().map(|tag| tag.to_string()).collect()
}

/// Done when 1 and 2: the rebuilt lineage resolves to the planned layer set
/// at every sequence, each layer in its planned blob, and the old repository
/// holds what it held.
#[test]
fn a_rebuilt_lineage_resolves_to_its_plan_at_every_sequence() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&(1..=UNTIL).collect::<Vec<_>>())).unwrap();
    let before = served.old();

    let written = served.write(&plan).unwrap();

    assert_eq!(written.target.to_string(), served.repository(NEW));
    assert_eq!((written.first, written.last), (1, UNTIL));
    assert_eq!(
        written.identity,
        plan.sequences[UNTIL as usize - 1].inscription_digest
    );
    assert_eq!(written.blobs_pulled, plan.blobs().len());
    assert_eq!((written.blobs_present, written.blobs_copied), (0, 0));
    assert_eq!(written.tags_written, 2 * UNTIL as usize + 1);
    assert_eq!(written.tags_present, 0);

    let registry = served.open(NEW);
    for (index, planned) in plan.sequences.iter().enumerate() {
        let snapshot = registry
            .resolve_sequence(&dolos_snapshot::DolosProfile, planned.sequence)
            .unwrap();
        let chain = snapshot.chain().unwrap();
        assert_eq!(chain.identity(), planned.inscription_digest);

        let expected: Vec<_> = plan.sequences[..=index]
            .iter()
            .flat_map(|planned| &planned.inscription.diff)
            .chain(&planned.inscription.tip)
            .collect();
        assert!(
            chain.descriptors().eq(expected.iter().copied()),
            "sequence {}",
            planned.sequence
        );

        let index = snapshot.blob_index().unwrap();
        for manifest in std::iter::once(&planned.diff_manifest).chain(&planned.tip_manifest) {
            for layer in &manifest.layers {
                let diff_id = layer.annotations.as_ref().unwrap()[DIFF_ID_ANNOTATION]
                    .parse()
                    .unwrap();
                assert_eq!(index.blob_for(&diff_id).unwrap().to_string(), layer.digest);
            }
        }
    }

    let latest = registry.pull_latest(&dolos_snapshot::DolosProfile).unwrap();
    assert_eq!(
        latest.read_inscription().unwrap().digest().unwrap(),
        written.identity
    );
    assert!(served.loopback.absent_blobs(NEW).is_empty());

    assert_eq!(served.old(), before);
    assert_eq!(served.outside.load(Ordering::SeqCst), 0);
    assert_eq!(served.loopback.accounting(OLD), Default::default());
}

/// A tip is tagged where it is kept and nowhere else, so a sequence whose tip
/// is gone resolves no further than its chain.
#[test]
fn only_the_kept_tips_are_tagged() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[3, 7])).unwrap();
    served.write(&plan).unwrap();

    let mut expected: BTreeSet<String> = (1..=UNTIL).map(|n| format!("epoch-{n}")).collect();
    expected.extend(tags(&[
        "epoch-3.tip",
        "epoch-7.tip",
        "epoch-8.tip",
        "latest",
    ]));
    let written: BTreeSet<String> = served.loopback.manifests(NEW).into_keys().collect();
    assert_eq!(written, expected);

    let registry = served.open(NEW);
    for sequence in [3, 7, UNTIL] {
        registry
            .resolve_sequence(&dolos_snapshot::DolosProfile, sequence)
            .unwrap();
    }
    let gone = registry
        .resolve_sequence(&dolos_snapshot::DolosProfile, 5)
        .unwrap_err();
    assert!(
        matches!(gone, stelae::Error::TipGone { sequence: 5, .. }),
        "{gone}"
    );

    let unkept: BTreeSet<String> = plan
        .sequences
        .iter()
        .filter(|planned| !planned.kept_tip)
        .flat_map(|planned| &planned.tip_manifest.as_ref().unwrap().layers)
        .map(|layer| layer.digest.clone())
        .collect();
    let kept: BTreeSet<String> = plan
        .blobs()
        .iter()
        .map(|blob| blob.digest.clone())
        .collect();
    let held: BTreeSet<String> = served.loopback.blobs(NEW).into_keys().collect();
    assert!(kept.is_subset(&held));
    assert!(unkept.difference(&kept).all(|blob| !held.contains(blob)));
}

/// A write that stops part way leaves `latest` where it was, and a second run
/// finishes it, writing each blob and tag once across both.
#[test]
fn an_interrupted_write_resumes_where_it_stopped() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[3, 7])).unwrap();
    let manifests = Arc::new(AtomicU64::new(0));
    let counted = manifests.clone();
    served.guard(move |method, path| {
        let tagging = method == "PUT" && path.contains("/manifests/");
        match tagging && counted.fetch_add(1, Ordering::SeqCst) >= 5 {
            true => Verdict::Refuse(405),
            false => Verdict::Proceed,
        }
    });

    let stopped = served.write(&plan).unwrap_err();
    assert!(matches!(stopped, Error::Registry { .. }), "{stopped}");
    assert_eq!(served.loopback.manifests(NEW).len(), 5);
    assert!(served.loopback.manifest(NEW, "latest").is_none());

    served.guard(|_, _| Verdict::Proceed);
    let resumed = served.write(&plan).unwrap();
    assert_eq!(resumed.tags_present, 5);
    assert_eq!(resumed.tags_written, UNTIL as usize + 3 + 1 - 5);
    assert_eq!(resumed.blobs_present, plan.blobs().len());
    assert_eq!(resumed.blobs_pulled, 0);

    let accounting = served.loopback.accounting(NEW);
    let tagged: Vec<&String> = accounting.tags.iter().map(|(tag, _)| tag).collect();
    let once: BTreeSet<&String> = tagged.iter().copied().collect();
    assert_eq!(tagged.len(), once.len(), "{tagged:?}");
    assert_eq!(tagged.last().map(|tag| tag.as_str()), Some("latest"));
    assert!(accounting.uploads.values().all(|(_, count)| *count == 1));

    let again = served.write(&plan).unwrap();
    assert_eq!((again.tags_written, again.blobs_pulled), (0, 0));
    assert_eq!(served.outside.load(Ordering::SeqCst), 0);
}

/// A tag that holds a manifest other than the planned one refuses the run
/// before anything is written.
#[test]
fn a_differing_manifest_refuses_the_write_before_it_starts() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[])).unwrap();
    let other = served.loopback.manifest(OLD, "epoch-3").unwrap();
    let dir = served.root.path().join(NEW).join("manifests/tags");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("epoch-3"), &other).unwrap();

    let refused = served.write(&plan).unwrap_err();
    match &refused {
        Error::Differs {
            tag,
            found,
            planned,
            ..
        } => {
            assert_eq!(tag, "epoch-3");
            assert_eq!(*found, Digest::compute(&other));
            assert_ne!(found, planned);
        }
        other => panic!("{other}"),
    }
    assert_eq!(served.loopback.accounting(NEW), Default::default());
    assert_eq!(served.loopback.manifest(NEW, "epoch-3"), Some(other));
}

/// A plan whose lineage is in the repository it would be written to is
/// refused before either is opened.
#[test]
fn the_source_is_never_the_target() {
    let served = Served::new();
    let text = served.plan(&[]);
    let (header, rest) = text.split_once('\n').unwrap();
    let mut header: Value = serde_json::from_str(header).unwrap();
    header["source"] = served.repository(NEW).into();
    let plan = Plan::read(&format!("{header}\n{rest}")).unwrap();

    let options = write::Options {
        root: Some(served.repository(OLD).parse().unwrap()),
        ..options()
    };
    let refused = write::write(&plan, &options).unwrap_err();
    assert!(matches!(refused, Error::SameRepository { .. }), "{refused}");
    assert_eq!(served.loopback.accounting(NEW), Default::default());
    assert_eq!(served.loopback.accounting(OLD), Default::default());
}

/// The closing pass reads the lineage back before `latest` moves: one that
/// does not resolve leaves `latest` unwritten.
#[test]
fn latest_moves_only_once_the_lineage_resolves() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[])).unwrap();
    let broken = format!("/v2/{NEW}/manifests/epoch-4");
    served.guard(
        move |method, path| match method == "GET" && path == broken {
            true => Verdict::Refuse(404),
            false => Verdict::Proceed,
        },
    );

    let refused = served.write(&plan).unwrap_err();
    assert!(
        matches!(
            refused,
            Error::Resolved {
                sequence: UNTIL,
                ..
            }
        ),
        "{refused}"
    );
    assert!(served
        .loopback
        .manifest(NEW, &format!("epoch-{UNTIL}"))
        .is_some());
    assert!(served.loopback.manifest(NEW, "latest").is_none());
}

/// A registry that cannot answer the closing pass is a failed read, not a
/// lineage that differs: `latest` stays unwritten, and a rerun moves it.
#[test]
fn a_registry_failing_the_closing_pass_is_not_a_lineage_that_differs() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[])).unwrap();
    let tag = served
        .root
        .path()
        .join(NEW)
        .join(format!("manifests/tags/epoch-{UNTIL}"));
    let head = format!("/v2/{NEW}/manifests/epoch-{UNTIL}");
    served.guard(move |method, path| {
        // Only once it is written: the run first reads it as absent.
        match method == "GET" && path == head && tag.exists() {
            true => Verdict::Refuse(401),
            false => Verdict::Proceed,
        }
    });

    let failed = served.write(&plan).unwrap_err();
    match &failed {
        Error::Registry { what, .. } => assert_eq!(what, &format!("resolving epoch-{UNTIL}")),
        other => panic!("{other}"),
    }
    assert!(served.loopback.manifest(NEW, "latest").is_none());

    served.guard(|_, _| Verdict::Proceed);
    let resumed = served.write(&plan).unwrap();
    assert_eq!(
        (resumed.tags_written, resumed.tags_present),
        (1, UNTIL as usize + 1)
    );
    assert!(served.loopback.manifest(NEW, "latest").is_some());
}

/// A plan of another network than the configured one is refused before
/// either repository is asked anything.
#[test]
fn a_plan_of_another_network_is_refused_before_any_request() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[])).unwrap();
    let requests = Arc::new(AtomicU64::new(0));
    let counted = requests.clone();
    served.guard(move |_, _| {
        counted.fetch_add(1, Ordering::SeqCst);
        Verdict::Proceed
    });

    let planned = options().network_magic;
    let options = write::Options {
        network_magic: planned + 1,
        ..options()
    };
    let refused = write::write(&plan, &options).unwrap_err();
    match &refused {
        Error::OtherNetwork {
            sequence,
            magic,
            reason,
        } => {
            assert_eq!((*sequence, *magic), (1, planned + 1));
            assert!(reason.contains(&format!("magic {planned}")), "{reason}");
        }
        other => panic!("{other}"),
    }
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    assert_eq!(served.outside.load(Ordering::SeqCst), 0);
    assert_eq!(served.loopback.accounting(NEW), Default::default());
}

/// The closing pass holds each layer to its planned blob: a manifest served
/// back with another blob under a layer fails it.
#[test]
fn a_layer_served_from_another_blob_fails_the_closing_pass() {
    let served = Served::new();
    let plan = Plan::read(&served.plan(&[])).unwrap();
    let tag = served.root.path().join(NEW).join("manifests/tags/epoch-4");
    let wanted = format!("/v2/{NEW}/manifests/epoch-4");
    served.guard(move |method, path| {
        // Only once it is written: the run first reads it as absent.
        let served = (method == "GET" && path == wanted)
            .then(|| std::fs::read(&tag).ok())
            .flatten();
        if let Some(served) = served {
            let mut manifest: Value = serde_json::from_slice(&served).unwrap();
            let layers = manifest["layers"].as_array_mut().unwrap();
            assert!(layers.len() > 1);
            for field in ["digest", "size"] {
                layers[0][field] = layers[1][field].clone();
            }
            std::fs::write(&tag, serde_json::to_vec(&manifest).unwrap()).unwrap();
        }
        Verdict::Proceed
    });

    let refused = served.write(&plan).unwrap_err();
    match &refused {
        Error::Resolved { reason, .. } => assert!(reason.contains("blobs"), "{reason}"),
        other => panic!("{other}"),
    }
    assert!(served.loopback.manifest(NEW, "latest").is_none());
}

/// A plan is read whole, and refused where its documents disagree.
#[test]
fn a_plan_whose_documents_disagree_is_refused() {
    let served = Served::new();
    let text = served.plan(&[3]);
    let lines: Vec<&str> = text.lines().collect();
    let line = |index: usize, edit: &dyn Fn(&mut Value)| {
        let mut lines: Vec<String> = lines.iter().map(|line| line.to_string()).collect();
        let mut value: Value = serde_json::from_str(&lines[index]).unwrap();
        edit(&mut value);
        lines[index] = value.to_string();
        lines.join("\n")
    };
    let refused = |text: &str| match Plan::read(text) {
        Err(Error::Plan { line, reason }) => (line, reason),
        other => panic!("{other:?}"),
    };

    assert_eq!(
        refused(&line(0, &|header| header["rebuildPlan"] = 2.into())).0,
        1
    );

    let gap = [&lines[..4], &lines[5..]].concat().join("\n");
    assert_eq!(refused(&gap).0, 5);

    let digest = line(3, &|planned| {
        planned["inscriptionDigest"] = Digest::compute(b"another").to_string().into()
    });
    assert_eq!(refused(&digest).0, 4);

    let blobs = line(2, &|planned| {
        planned["blobs"].as_array_mut().unwrap().pop();
    });
    assert_eq!(refused(&blobs).0, 3);

    let unkept = line(UNTIL as usize, &|planned| {
        let diff = planned["diffManifest"]["layers"].as_array().unwrap().len();
        planned["blobs"].as_array_mut().unwrap().truncate(diff);
        planned["keptTip"] = false.into();
    });
    assert!(refused(&unkept).1.contains("newest tip"));

    // Starting at epoch-2, whose inscription links to the epoch-1 left out.
    let mut header: Value = serde_json::from_str(lines[0]).unwrap();
    header["first"] = 2.into();
    let late = [&[header.to_string().as_str()], &lines[2..]]
        .concat()
        .join("\n");
    assert!(refused(&late).1.contains("first inscription links"));

    let short = lines[..UNTIL as usize].join("\n");
    assert!(refused(&short).1.contains("epoch-1 to epoch-8"));

    Plan::read(&text).unwrap();
}
