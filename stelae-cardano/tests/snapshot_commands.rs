use std::path::{Path, PathBuf};

use dolos::engine::ReplayWorkspace;
use dolos_core::Genesis;
use dolos_snapshot::{export::First, source::SnapshotSource as _};
use stelae::progress::Observer;
use stelae_cardano::{
    publisher::{publish_once, Destination, PublishOutcome},
    snapshot::{self, Published},
    testing::Node,
    Auth, Point, RestoreSource, RetainedEpochs, Selection,
};

/// A node replayed to its first boundary, and that boundary published to a
/// directory retaining a dump of epoch 1.
struct Fixture {
    node: Node,
    genesis: Genesis,
    stele: PathBuf,
    identity: String,
}

fn retained() -> RetainedEpochs {
    dolos_snapshot::planning::retained_epochs(&[1]).unwrap()
}

impl Fixture {
    fn published() -> Self {
        let node = Node::new();
        let (genesis, _) = node.replay_to_the_first_boundary();
        let stele = node.path().join("stele");
        let (_, outcome) = publish_once(
            &node.config,
            &genesis,
            &retained(),
            Selection::default(),
            Destination::Directory(stele.clone()),
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
            stele,
            identity,
        }
    }

    fn open(&self) -> Published {
        Published::open(
            &self.node.config,
            &RestoreSource::Dir(self.stele.clone()),
            false,
            Auth::Anonymous,
        )
        .unwrap()
    }

    fn with_plan<T>(
        &self,
        retained: RetainedEpochs,
        then: impl FnOnce(
            &dyn dolos_snapshot::source::SnapshotSource,
            &dolos_snapshot::export::Plan,
        ) -> Result<T, snapshot::Error>,
    ) -> Result<T, snapshot::Error> {
        snapshot::with_plan(
            &self.node.config,
            &self.genesis,
            retained,
            Selection::default(),
            &|_, _| {},
            then,
        )
    }
}

fn blobs(stele: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(stele.join("blobs").join("sha256"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

#[test]
fn digest_is_the_profile_document_over_the_same_store_and_plan() {
    let fixture = Fixture::published();

    let digested = fixture
        .with_plan(retained(), |source, plan| {
            snapshot::digest(source, plan, None)
        })
        .unwrap();
    assert_eq!(digested.history, None);

    let workspace =
        ReplayWorkspace::open(&fixture.node.config, fixture.genesis.clone().into()).unwrap();
    let expected = {
        let source = workspace.snapshot();
        let plan = source
            .selected_plan(
                u64::from(fixture.genesis.network_magic()),
                retained(),
                Selection::default(),
            )
            .unwrap();
        source.digest_document(&plan, &First).unwrap()
    };
    workspace.finish().unwrap();

    assert_eq!(digested.document.canonical, expected.canonical);
    assert_eq!(digested.document.identity, expected.identity);
    // The first stele of a chain is what a digest with no predecessor says.
    assert_eq!(
        digested.document.canonical,
        std::fs::read(fixture.stele.join("inscription.json")).unwrap()
    );
    assert_eq!(digested.document.identity.to_string(), fixture.identity);
}

#[test]
fn verify_passes_on_the_published_directory_and_finds_an_altered_layer() {
    let fixture = Fixture::published();

    let verified = fixture.open().verify(Point::Latest).unwrap();
    assert_eq!(verified.inscription.sequence, 1);
    assert_eq!(verified.identity.to_string(), fixture.identity);
    assert!(verified.compressed_bytes > 0);

    let blob = blobs(&fixture.stele).into_iter().next().unwrap();
    let mut bytes = std::fs::read(&blob).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&blob, bytes).unwrap();

    let error = fixture.open().verify(Point::Latest).unwrap_err();
    assert!(error.is_mismatch(), "{error:?}");
}

#[test]
fn verify_reproduce_passes_with_the_fixtures_retained_epochs() {
    let fixture = Fixture::published();
    let verified = fixture.open().verify(Point::Latest).unwrap();

    let reproduced = fixture
        .with_plan(retained(), |source, plan| {
            snapshot::reproduce(source, plan, &verified.inscription)
        })
        .unwrap();
    assert_eq!(reproduced.layers.len(), verified.inscription.layers.len());

    // Retaining the tip alone is a different signed parameter, so a different
    // document.
    let error = fixture
        .with_plan(RetainedEpochs::default(), |source, plan| {
            snapshot::reproduce(source, plan, &verified.inscription)
        })
        .unwrap_err();
    assert!(error.is_mismatch(), "{error:?}");
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
        inspected.inscription.layers.len()
    );
    assert!(inspected.compressed.iter().all(Option::is_some));
    // Counted per layer, as verify streams them: identical layers share a blob.
    let streamed = published.verify(Point::Latest).unwrap().compressed_bytes;
    assert_eq!(inspected.total_compressed, streamed);

    published.inspect(Point::Epoch(1)).unwrap();
    let error = published.inspect(Point::Epoch(2)).unwrap_err();
    assert!(!error.is_mismatch(), "{error:?}");
    assert!(error.to_string().contains("sequence 1"), "{error}");
}
