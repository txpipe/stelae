//! Whole publications over real stores and the real transport, held byte for
//! byte to what publishing one boundary at a time gives, and restored by the
//! consumer that was there before batches.
//!
//! These run in the ordinary test job: the registry is a loopback server, so no
//! Docker. `tests/batch.rs` covers the journal's state machine, with a fake
//! repository and every fault that takes.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock, Mutex, Once,
    },
};

use dolos::engine::{BulkReplaySession, ReplayWorkspace};
use dolos_core::ChainPoint;
use dolos_snapshot::{export::Following, source::SnapshotSource};
use pallas::ledger::traverse::MultiEraBlock;
use serde_json::Value;
use stelae::{inscription::Inscription, progress::Observer};
use stelae_cardano::{
    backfill::{self, Options, Outcome},
    batch::{self, Limits},
    initialize,
    testing::{chain::Chain, loopback::Loopback, Node},
    Auth, MithrilSource, Point, RestoreSource, RetainedEpochs,
};
use tokio_util::sync::CancellationToken;
use tracing::{
    field::{Field, Visit},
    span, Event, Metadata, Subscriber,
};

const UNTIL: u64 = 8;
/// Retained dumps fall on either side of most windows' batch boundaries.
const RETAINED: [u64; 2] = [3, 7];
/// The epoch no new asset appears in, which leaves a hole in `entity-asset`.
const HOLE: u64 = 2;

fn provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
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

/// Counts every store traversal an index staging starts, on the thread that
/// runs a publication, which is where they run.
#[derive(Default)]
struct Traversals(Mutex<BTreeMap<String, u64>>, AtomicU64);

struct TraversalName<'a> {
    message: bool,
    traversal: &'a mut Option<String>,
}

impl Visit for TraversalName<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}") == "index staging traversal started",
            "traversal" => *self.traversal = Some(format!("{value:?}").replace('"', "")),
            _ => {}
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "traversal" {
            *self.traversal = Some(value.to_owned());
        }
    }
}

impl Subscriber for Traversals {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(self.1.fetch_add(1, Ordering::Relaxed) + 1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut traversal = None;
        let mut visitor = TraversalName {
            message: false,
            traversal: &mut traversal,
        };
        event.record(&mut visitor);
        if visitor.message {
            let name = traversal.unwrap_or_default();
            *self.0.lock().unwrap().entry(name).or_default() += 1;
        }
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

fn known_genesis_vkey() -> String {
    let node = Node::new();
    let source = stelae_cardano::config::mithril_source(&node.config, None, None).unwrap();
    source.genesis_vkey
}

/// One run of the publisher, and every store traversal it started by kind.
fn publish(
    chain: &Chain,
    loopback: &Loopback,
    name: &str,
    limits: Limits,
    until: Option<u64>,
) -> (Node, Outcome, BTreeMap<String, u64>) {
    provider();
    let node = chain.node();
    let download = chain.write_immutable(&node);
    let options = Options {
        direct_s3: None,
        repository: loopback.repository(name).parse().unwrap(),
        auth: Auth::Anonymous,
        insecure: true,
        scratch_dir: None,
        concurrency: None,
        verify_carried: false,
        download_dir: Some(download),
        window: 1,
        until_epoch: until,
        skip_validation: true,
        mithril: MithrilSource {
            aggregator: loopback.aggregator(),
            // A listing builds a verifier from this, though it checks nothing.
            genesis_vkey: known_genesis_vkey(),
        },
        retained: retained(),
        limits,
    };
    let traversals = Arc::new(Traversals::default());
    let outcome = tracing::subscriber::with_default(traversals.clone(), || {
        backfill::run(
            &node.config,
            &chain.genesis,
            &options,
            CancellationToken::new(),
            &Observer::silent(),
        )
    })
    .unwrap_or_else(|error| panic!("publishing {name}: {error}"));
    let counted = traversals.0.lock().unwrap().clone();
    (node, outcome, counted)
}

/// The document a stele's manifest names, as the registry holds it.
fn canonical(root: &std::path::Path, name: &str, sequence: u64) -> Vec<u8> {
    let manifest = std::fs::read(
        root.join(name)
            .join("manifests/tags")
            .join(format!("epoch-{sequence}")),
    )
    .unwrap();
    let manifest: Value = serde_json::from_slice(&manifest).unwrap();
    let digest = manifest["config"]["digest"].as_str().unwrap();
    let hex = digest.trim_start_matches("sha256:");
    std::fs::read(root.join(name).join("blobs/sha256").join(hex)).unwrap()
}

/// A repository holds exactly what another does, down to what it took to fill.
fn assert_same_repository(loopback: &Loopback, found: &str, expected: &str) {
    let (found_tags, expected_tags) = (loopback.manifests(found), loopback.manifests(expected));
    assert_eq!(
        found_tags.keys().collect::<Vec<_>>(),
        expected_tags.keys().collect::<Vec<_>>(),
        "{found}'s tags"
    );
    for (tag, manifest) in &expected_tags {
        assert_eq!(&found_tags[tag], manifest, "{found}'s manifest at {tag}");
    }
    assert_eq!(
        loopback.blobs(found),
        loopback.blobs(expected),
        "{found}'s blobs, which the server held to their digests"
    );
    assert_eq!(
        loopback.accounting(found).uploads,
        loopback.accounting(expected).uploads,
        "what {found} took to upload"
    );
}

fn assert_batch_is_gone(node: &Node) {
    let dir = batch::dir_in(&node.config.storage.path);
    assert!(!dir.join("journal.json").exists());
    assert!(!dir.join("staging").exists());
    for held in ["prepared/bundles", "prepared/blobs/sha256"] {
        let held = std::fs::read_dir(dir.join(held)).map_or(0, |entries| entries.count());
        assert_eq!(held, 0, "{held}");
    }
}

/// How many batches a window makes of the sequences up to [`UNTIL`].
fn batches(window: u64) -> u64 {
    UNTIL.div_ceil(window)
}

fn assert_one_pass_per_batch(traversals: &BTreeMap<String, u64>, window: u64) {
    let count = |kind: &str| traversals.get(kind).copied().unwrap_or(0);
    let unknown: Vec<_> = traversals
        .keys()
        .filter(|kind| !["tag", "exact", "entity"].contains(&kind.as_str()))
        .collect();
    assert!(unknown.is_empty(), "a traversal not counted: {unknown:?}");
    assert_eq!(count("tag"), batches(window), "{traversals:?}");
    assert_eq!(count("exact"), batches(window), "{traversals:?}");
    assert!(count("entity") <= batches(window), "{traversals:?}");
}

/// One boundary at a time, once for the whole test binary: every test holds
/// what it publishes to this.
struct Reference {
    root: tempfile::TempDir,
    loopback: Loopback,
    chain: Chain,
    traversals: BTreeMap<String, u64>,
}

const ONE: &str = "cardano/one";

static SHELLEY: LazyLock<Reference> = LazyLock::new(|| {
    let root = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(root.path()).unwrap();
    // Epochs past UNTIL, so the last boundary isn't the end of the source.
    let chain = Chain::delegating(UNTIL as usize + 2, &[HOLE as usize]);
    let (_, outcome, traversals) = publish(&chain, &loopback, ONE, Limits::ONE, Some(UNTIL));
    assert_eq!(outcome, Outcome::UntilEpoch { sequence: UNTIL });
    Reference {
        root,
        loopback,
        chain,
        traversals,
    }
});

impl Reference {
    fn inscription(&self, name: &str, sequence: u64) -> Inscription {
        Inscription::parse(&self.canonical(name, sequence)).unwrap()
    }

    fn canonical(&self, name: &str, sequence: u64) -> Vec<u8> {
        canonical(self.root.path(), name, sequence)
    }

    /// The window `(kind, epoch)`'s layer in a stele, if it has one.
    fn window(&self, sequence: u64, kind: &str, epoch: u64) -> Option<String> {
        let inscription = self.inscription(ONE, sequence);
        let layer = inscription.layers.into_iter().find(|layer| {
            layer.kind == kind && layer.scope.get("epoch") == Some(&Value::from(epoch))
        })?;
        Some(layer.diff_id.to_hex())
    }

    fn epochs_of(&self, sequence: u64, kind: &str) -> BTreeSet<u64> {
        let inscription = self.inscription(ONE, sequence);
        inscription
            .layers
            .iter()
            .filter(|layer| layer.kind == kind)
            .filter_map(|layer| layer.scope.get("epoch")?.as_u64())
            .collect()
    }
}

/// The chain has what this file claims to cover, or every comparison below
/// could pass over nothing. Each case names what it would miss without it.
#[test]
fn the_reference_holds_every_case_the_comparisons_are_about() {
    let reference = &*SHELLEY;

    // One pass per boundary is what the instrument reads, or it reads nothing.
    assert_eq!(reference.traversals["tag"], UNTIL);
    assert_eq!(reference.traversals["exact"], UNTIL);
    assert!(reference.traversals["entity"] >= 1);

    // Retained dumps: from their boundary on, carried by each later stele.
    let dumps = |sequence: u64, epoch: u64| {
        let inscription = reference.inscription(ONE, sequence);
        inscription
            .layers
            .iter()
            .filter(|layer| layer.kind.starts_with("state-"))
            .filter(|layer| layer.scope.get("epoch").and_then(Value::as_u64) == Some(epoch))
            .count()
    };
    assert_eq!(dumps(2, 3), 0);
    assert!(dumps(3, 3) > 0);
    assert_eq!(dumps(3, 3), dumps(UNTIL, 3));
    assert!(dumps(7, 7) > 0);

    // Delayed seals: a window closes at the next boundary, and its entities
    // arrive at the one after, whatever a later view shows of them.
    for epoch in [1, 3, 4] {
        assert!(!reference
            .epochs_of(epoch + 1, "entity-asset")
            .contains(&epoch));
        assert!(reference
            .epochs_of(epoch + 2, "entity-asset")
            .contains(&epoch));
    }

    // A hole: a sealed epoch with no entity has no layer, here or later.
    assert!(!reference.epochs_of(UNTIL, "entity-asset").contains(&HOLE));
    assert!(reference
        .epochs_of(UNTIL, "entity-asset")
        .contains(&(HOLE + 1)));

    // Settling account rows: each publication holds the window its boundary
    // stands in, and a closed window once it has settled.
    let settling = reference.epochs_of(UNTIL, "log-account-epochs");
    assert!(settling.contains(&UNTIL) && settling.contains(&(UNTIL - 1)));
    let settled = reference.window(UNTIL - 2, "log-account-epochs", UNTIL - 3);
    assert_eq!(
        settled,
        reference.window(UNTIL, "log-account-epochs", UNTIL - 3)
    );

    // Inherited layers: a closed window is carried, not uploaded again.
    let closed = reference.window(2, "blocks", 1).unwrap();
    assert_eq!(reference.window(UNTIL, "blocks", 1).unwrap(), closed);
    let accounting = reference.loopback.accounting(ONE);
    let repeated: Vec<_> = accounting
        .uploads
        .iter()
        .filter(|(_, (_, count))| *count > 1)
        .collect();
    assert!(repeated.is_empty(), "uploaded twice: {repeated:?}");
    assert!(accounting.uploaded() > 0);
}

/// Every window in order: every tag, manifest and blob the same as one boundary
/// at a time, the same uploads, and one store traversal per family per batch.
#[test]
fn every_window_publishes_what_one_boundary_at_a_time_publishes() {
    let reference = &*SHELLEY;
    // Aligned, unaligned with a small final batch, and one batch for it all.
    for window in [2, 3, 16] {
        let name = format!("cardano/window-{window}");
        let (node, outcome, traversals) = publish(
            &reference.chain,
            &reference.loopback,
            &name,
            limits(window),
            Some(UNTIL),
        );
        assert_eq!(outcome, Outcome::UntilEpoch { sequence: UNTIL });
        assert_one_pass_per_batch(&traversals, window);
        assert_same_repository(&reference.loopback, &name, ONE);
        assert_batch_is_gone(&node);
    }
}

/// A source running out closes a batch smaller than its limit, and the partial
/// epoch past its last boundary stays unpublished.
#[test]
fn the_end_of_the_source_publishes_what_one_boundary_at_a_time_would() {
    let root = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(root.path()).unwrap();
    let chain = Chain::delegating(5, &[]);

    let (_, one, _) = publish(&chain, &loopback, ONE, Limits::ONE, None);
    let (_, batched, _) = publish(&chain, &loopback, "cardano/batched", limits(16), None);

    assert_eq!(one, Outcome::UpToDate);
    assert_eq!(batched, Outcome::UpToDate);
    assert_same_repository(&loopback, "cardano/batched", ONE);
    assert_eq!(
        loopback.manifests(ONE).get("latest"),
        loopback.manifests(ONE).get("epoch-4"),
        "epoch 4 is the last with a boundary"
    );
}

/// Each publication is what stores frozen at its own boundary publish, so the
/// comparison isn't only between two runs of the staging both paths share.
#[test]
fn every_publication_is_what_stores_frozen_at_its_boundary_publish() {
    let reference = &*SHELLEY;
    let mut previous = None;
    for sequence in 1..=UNTIL {
        let frozen = reference
            .chain
            .frozen(sequence, &retained(), previous.as_ref());
        assert_eq!(
            frozen.canonical,
            reference.canonical(ONE, sequence),
            "at sequence {sequence}"
        );
        previous = Some(frozen);
    }
}

/// Restore `sequence` into fresh stores, hold them to the stele, and replay on
/// to the next boundary, as a node restoring from it would.
fn restores_and_continues(reference: &Reference, name: &str, sequence: u64, next: Option<&[u8]>) {
    let chain = &reference.chain;
    let node = chain.node();
    let outcome = initialize::run(
        &node.config,
        &chain.genesis,
        &initialize::Options {
            source: RestoreSource::Repo(reference.loopback.repository(name).parse().unwrap()),
            point: Point::Epoch(sequence),
            auth: Auth::Anonymous,
            insecure: true,
            scratch_dir: None,
            skip_space_check: true,
            resume: false,
            allow_genesis: false,
        },
        &Observer::silent(),
    )
    .unwrap();
    assert_eq!(outcome, initialize::Outcome::Restored { sequence });

    let published = reference.canonical(name, sequence);
    let workspace = ReplayWorkspace::open(&node.config, chain.genesis.clone()).unwrap();
    let cursor = {
        let snapshot = workspace.snapshot();
        let plan = snapshot.plan(chain.magic(), retained()).unwrap();
        let inscription = Inscription::parse(&published).unwrap();
        let reproduced = snapshot.verify_reproduction(&inscription, &plan).unwrap();
        assert_eq!(reproduced, inscription, "reproducing {sequence}");
        snapshot.committed_position().unwrap().unwrap()
    };
    workspace.finish().unwrap();

    let Some(next) = next else {
        return;
    };
    let mut session =
        BulkReplaySession::open(&node.config, chain.genesis.clone(), Some(sequence + 1)).unwrap();
    let progress = session.import_blocks(blocks_after(chain, &cursor)).unwrap();
    assert!(progress.is_boundary(), "{progress:?}");
    let snapshot = session.snapshot();
    let plan = snapshot.plan(chain.magic(), retained()).unwrap();
    let following = Following::read(&published, &plan).unwrap();
    let document = snapshot.digest_document(&plan, &following).unwrap();
    assert_eq!(document.canonical, next, "the stele after {sequence}");
    drop(snapshot);
    session.finish().unwrap();
}

fn blocks_after(chain: &Chain, cursor: &ChainPoint) -> Vec<dolos_core::RawBlock> {
    let hash = cursor.hash().unwrap();
    let at = chain
        .blocks
        .iter()
        .position(|block| MultiEraBlock::decode(block).unwrap().hash() == hash)
        .expect("the cursor is on the chain");
    chain.blocks[at + 1..].to_vec()
}

/// The consumer knows nothing of batches: `initialize` restores whichever stele
/// it is given, the stores reproduce that stele, and replaying on from them
/// reaches the next one byte for byte. The batch publishes one boundary at a
/// time's bytes, so this holds for both.
#[test]
fn a_consumer_restores_every_publication_of_a_batch_and_replays_on() {
    let reference = &*SHELLEY;
    let name = "cardano/restored";
    publish(
        &reference.chain,
        &reference.loopback,
        name,
        limits(3),
        Some(UNTIL),
    );
    for sequence in 1..=UNTIL {
        let next = (sequence < UNTIL).then(|| reference.canonical(name, sequence + 1));
        restores_and_continues(reference, name, sequence, next.as_deref());
    }
}

static BYRON: LazyLock<Reference> = LazyLock::new(|| {
    let root = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(root.path()).unwrap();
    let chain = Chain::byron(UNTIL + 2);
    let (_, outcome, traversals) = publish(&chain, &loopback, ONE, Limits::ONE, Some(UNTIL));
    assert_eq!(outcome, Outcome::UntilEpoch { sequence: UNTIL });
    Reference {
        root,
        loopback,
        chain,
        traversals,
    }
});

/// A Byron main block shares its boundary block's slot. A later archive
/// filtered by slot takes that main block into the earlier boundary's stele,
/// and a replay stopped at the boundary doesn't hold it. So each boundary's
/// window holds the boundary block alone, stands on it, and matches whatever
/// window batched it.
#[test]
fn a_byron_boundary_holds_its_boundary_block_and_not_the_main_block_beside_it() {
    let reference = &*BYRON;
    let chain = &reference.chain;
    assert_eq!(reference.traversals["tag"], UNTIL);

    let mut previous = None;
    for sequence in 1..=UNTIL {
        let inscription = reference.inscription(ONE, sequence);

        // Each epoch opens with its boundary block, then two main blocks, the
        // first of them at the boundary block's own slot.
        let at = sequence as usize * 3;
        let ebb = MultiEraBlock::decode(&chain.blocks[at]).unwrap();
        let main = MultiEraBlock::decode(&chain.blocks[at + 1]).unwrap();
        assert_eq!(main.slot(), ebb.slot(), "the case this test is about");
        let point = &inscription.position["point"];
        assert_eq!(point["slot"], Value::from(ebb.slot()));
        assert_eq!(point["hash"], Value::from(ebb.hash().to_string()));

        // The header record and the boundary block, and no main block.
        let own = inscription
            .layers
            .iter()
            .find(|layer| {
                layer.kind == "blocks" && layer.scope.get("epoch") == Some(&Value::from(sequence))
            })
            .expect("the boundary's own blocks window");
        assert_eq!(own.records, 2, "at sequence {sequence}");

        let frozen = chain.frozen(sequence, &retained(), previous.as_ref());
        assert_eq!(frozen.canonical, reference.canonical(ONE, sequence));
        previous = Some(frozen);
    }

    for window in [3, 16] {
        let name = format!("cardano/byron-{window}");
        let (_, outcome, traversals) = publish(
            chain,
            &reference.loopback,
            &name,
            limits(window),
            Some(UNTIL),
        );
        assert_eq!(outcome, Outcome::UntilEpoch { sequence: UNTIL });
        assert_one_pass_per_batch(&traversals, window);
        assert_same_repository(&reference.loopback, &name, ONE);
    }
}

/// A restored Byron boundary replays on into the main block beside it, which it
/// couldn't if the stele had already taken it.
#[test]
fn a_consumer_restores_a_byron_boundary_and_replays_the_main_block_beside_it() {
    let reference = &*BYRON;
    let name = "cardano/byron-restored";
    publish(
        &reference.chain,
        &reference.loopback,
        name,
        limits(4),
        Some(UNTIL),
    );
    for sequence in 1..=UNTIL {
        let next = (sequence < UNTIL).then(|| reference.canonical(name, sequence + 1));
        restores_and_continues(reference, name, sequence, next.as_deref());
    }
}
