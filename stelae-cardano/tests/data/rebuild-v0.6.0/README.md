# An old lineage, recorded from release v0.6.0

The documents of a Dolos lineage published before the diff chain, laid out as
the test loopback registry keeps one repository: each tag's manifest under
`manifests/tags/`, and each manifest's config blob, the old inscription, under
`blobs/sha256/`. No layer blob is kept, because the rebuild plan reads none.
`chain.json` lists the sha256 of every block of the chain they were published
from, so a test can show it replays the same chain.

The lineage is v0.6.0's own parity reference (`stelae-cardano/tests/parity.rs`,
`SHELLEY`): `Chain::delegating(10, &[2])` back-filled one boundary at a time to
epoch 8, with retained dumps at epochs 3 and 7. It holds `epoch-1` to
`epoch-8` and `latest`. It covers a `log-account-epochs` window that settles,
seals of period logs and entity layers, and two retained dumps.

To record it again, check out `v0.6.0`, append this test to
`stelae-cardano/tests/parity.rs`, and run it with `REBUILD_FIXTURE_OUT` naming
an empty directory:

```rust
#[test]
#[ignore]
fn record_the_rebuild_fixture() {
    let out = std::path::PathBuf::from(std::env::var("REBUILD_FIXTURE_OUT").unwrap());
    let reference = &*SHELLEY;
    let root = reference.root.path().join(ONE);
    let tags = out.join("manifests/tags");
    let blobs = out.join("blobs/sha256");
    std::fs::create_dir_all(&tags).unwrap();
    std::fs::create_dir_all(&blobs).unwrap();
    for (tag, manifest) in reference.loopback.manifests(ONE) {
        std::fs::write(tags.join(&tag), &manifest).unwrap();
        let manifest: Value = serde_json::from_slice(&manifest).unwrap();
        let hex = manifest["config"]["digest"].as_str().unwrap().trim_start_matches("sha256:").to_owned();
        std::fs::copy(root.join("blobs/sha256").join(&hex), blobs.join(&hex)).unwrap();
    }
    let blocks: Vec<String> = reference.chain.blocks.iter().map(|raw| stelae::Digest::compute(raw.as_slice()).to_string()).collect();
    std::fs::write(out.join("chain.json"), serde_json::to_vec(&blocks).unwrap()).unwrap();
}
```

```sh
REBUILD_FIXTURE_OUT=/path/to/empty cargo test --locked -p stelae-cardano --test parity \
  -- --ignored record_the_rebuild_fixture
```
