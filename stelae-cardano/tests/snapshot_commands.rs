use std::path::{Path, PathBuf};

use dolos::engine::ReplayWorkspace;
use dolos_core::Genesis;
use dolos_snapshot::{export::First, source::SnapshotSource as _, DolosProfile};
use stelae::{dir::SnapshotDir, progress::Observer, SnapshotReader as _};
use stelae_cardano::{
    publisher::{publish_once, Destination, PublishOutcome},
    snapshot::{self, Inputs, Published},
    testing::Node,
    Auth, Point, RestoreSource, Selection,
};

/// A node replayed to its first boundary, and that boundary published into a
/// network root on disk: its blocks snapshot and the stele naming it.
struct Fixture {
    node: Node,
    genesis: Genesis,
    root: PathBuf,
    identity: String,
}

impl Fixture {
    fn published() -> Self {
        let node = Node::new();
        let (genesis, _) = node.replay_to_the_first_boundary();
        let root = node.path().join("root");
        let (_, outcome) = publish_once(
            &node.config,
            &genesis,
            Selection::default(),
            Destination::Directory {
                path: root.clone(),
                scratch_dir: None,
            },
            false,
            &Observer::silent(),
        )
        .unwrap();
        let PublishOutcome::Directory { identity, .. } = outcome else {
            panic!("fixture was not published to the directory");
        };
        Self {
            genesis: (*genesis).clone(),
            node,
            root,
            identity,
        }
    }

    fn open(&self) -> Published {
        Published::open(
            &self.node.config,
            &RestoreSource::Dir(self.root.clone()),
            false,
            Auth::Anonymous,
        )
        .unwrap()
    }

    fn with_plan<T>(
        &self,
        then: impl FnOnce(
            &dyn dolos_snapshot::source::SnapshotSource,
            &dolos_snapshot::export::Plan,
        ) -> Result<T, snapshot::Error>,
    ) -> Result<T, snapshot::Error> {
        snapshot::with_plan(
            &self.node.config,
            &self.genesis,
            Selection::default(),
            None,
            &|_, _| {},
            then,
        )
    }

    /// The canonical inscription the `dolos` layout holds at `latest`.
    fn canonical(&self) -> Vec<u8> {
        SnapshotDir::open(self.root.join("dolos"))
            .unwrap()
            .pull_latest(&DolosProfile)
            .unwrap()
            .read_inscription()
            .unwrap()
            .canonicalize()
            .unwrap()
    }
}

/// A reproduction spools where it is told to, by default under the stores.
#[test]
fn a_digest_spools_under_the_scratch_directory_it_names() {
    let node = Node::new();
    let (genesis, _) = node.replay_to_the_first_boundary();
    let named = node.path().join("spool");
    for (scratch, expected) in [
        (None, node.config.storage.path.join("scratch")),
        (Some(named.as_path()), named.clone()),
    ] {
        assert!(!expected.exists());
        snapshot::with_plan(
            &node.config,
            &genesis,
            Selection::default(),
            scratch,
            &|_, _| {},
            |source, plan| snapshot::digest(source, plan, Inputs::default()),
        )
        .unwrap();
        assert!(expected.is_dir(), "{}", expected.display());
    }
}

fn blobs(layout: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(layout.join("blobs").join("sha256"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

/// A digest over the inputs the root holds is the stele published there: the
/// first of its lineage, naming the blocks snapshot beside it.
#[test]
fn digest_is_the_profile_document_over_the_same_store_and_plan() {
    let fixture = Fixture::published();
    let published = fixture.open();

    let digested = fixture
        .with_plan(|source, plan| {
            let inputs = published.inputs(&fixture.node.config, plan)?;
            snapshot::digest(source, plan, inputs)
        })
        .unwrap();
    assert_eq!(digested.inputs.previous, None);
    let blocks = digested.inputs.blocks.expect("the root holds blocks");

    let workspace =
        ReplayWorkspace::open(&fixture.node.config, fixture.genesis.clone().into()).unwrap();
    let expected = {
        let source = workspace.snapshot();
        let plan = source
            .selected_plan(
                u64::from(fixture.genesis.network_magic()),
                Selection::default(),
            )
            .unwrap()
            .with_blocks(blocks);
        source.digest_document(&plan, &First).unwrap()
    };
    workspace.finish().unwrap();

    assert_eq!(digested.document.canonical, expected.canonical);
    assert_eq!(digested.document.identity, expected.identity);
    assert_eq!(digested.document.canonical, fixture.canonical());
    assert_eq!(digested.document.identity.to_string(), fixture.identity);

    // Naming no blocks snapshot is another document.
    let unnamed = fixture
        .with_plan(|source, plan| snapshot::digest(source, plan, Inputs::default()))
        .unwrap();
    assert_ne!(unnamed.document.identity, digested.document.identity);
}

#[test]
fn verify_passes_on_the_published_directory_and_finds_an_altered_layer() {
    let fixture = Fixture::published();

    let verified = fixture.open().verify(Point::Latest).unwrap();
    assert_eq!(verified.inscription.sequence, 1);
    assert_eq!(verified.identity.to_string(), fixture.identity);
    assert!(verified.compressed_bytes > 0);

    // A blob of a layer, not a manifest or the inscription: the largest.
    let blob = blobs(&fixture.root.join("dolos"))
        .into_iter()
        .max_by_key(|blob| std::fs::metadata(blob).unwrap().len())
        .unwrap();
    let mut bytes = std::fs::read(&blob).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&blob, bytes).unwrap();

    let error = fixture.open().verify(Point::Latest).unwrap_err();
    assert!(error.is_mismatch(), "{error:?}");
}

/// A reproduction holds the published link to the one it is given: none is
/// what a lineage's first stele follows, and any other is a mismatch.
#[test]
fn verify_reproduce_holds_the_published_link_to_the_one_given() {
    let fixture = Fixture::published();
    let verified = fixture.open().verify(Point::Latest).unwrap();

    let reproduced = fixture
        .with_plan(|source, plan| snapshot::reproduce(source, plan, &verified.inscription, None))
        .unwrap();
    assert_eq!(
        reproduced.layers().count(),
        verified.inscription.layers().count()
    );

    let other = snapshot::previous_at(1, stelae::Digest::compute(b"another")).unwrap();
    let error = fixture
        .with_plan(|source, plan| {
            snapshot::reproduce(source, plan, &verified.inscription, Some(other))
        })
        .unwrap_err();
    assert!(error.is_mismatch(), "{error:?}");
    assert!(
        causes(&error).contains("--previous"),
        "the refusal names what it compared"
    );

    assert!(matches!(
        snapshot::previous_at(0, stelae::Digest::compute(b"another")),
        Err(snapshot::Error::NothingBefore)
    ));
}

#[test]
fn inspect_lists_the_published_point() {
    let fixture = Fixture::published();
    let published = fixture.open();

    let inspected = published.inspect(Point::Latest).unwrap();
    assert_eq!(inspected.inscription.sequence, 1);
    assert_eq!(inspected.identity.to_string(), fixture.identity);
    assert_eq!(
        inspected.compressed.len(),
        inspected.chain.descriptors().count()
    );
    assert!(inspected.compressed.iter().all(Option::is_some));
    // Counted per layer, as verify streams them: identical layers share a blob.
    let streamed = published.verify(Point::Latest).unwrap().compressed_bytes;
    assert_eq!(inspected.total_compressed, streamed);

    published.inspect(Point::Epoch(1)).unwrap();
    let error = published.inspect(Point::Epoch(2)).unwrap_err();
    assert!(!error.is_mismatch(), "{error:?}");
    let error = causes(&error);
    assert!(error.contains("epoch-2"), "{error}");
}

/// An error and every cause under it, as one line.
fn causes(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut next = error.source();
    while let Some(cause) = next {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        next = cause.source();
    }
    text
}
