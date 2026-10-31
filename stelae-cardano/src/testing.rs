//! A bounded preview node for tests, here so the publisher's binary tests can
//! build one without a Dolos edge of their own.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::BulkReplaySession;
use dolos_core::{
    config::{ChainConfig, RootConfig},
    ChainPoint, Genesis,
};
use dolos_testing::blocks::make_conway_block_with_prev;

/// A preview node whose stores live in a temporary directory.
pub struct Node {
    pub root: tempfile::TempDir,
    pub config: RootConfig,
}

impl Node {
    pub fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let compatibility = RootConfig::new(ChainConfig::known("preview")).compatibility();
        let document = format!(
            r#"
            compatibility = "{compatibility}"

            [chain]
            protocol = "cardano"
            network = "preview"

            [storage]
            path = {}
            "#,
            toml::Value::String(root.path().join("data").display().to_string()),
        );
        let path = root.path().join("dolos.toml");
        std::fs::write(&path, document).unwrap();
        let config = crate::config::load(Some(&path)).unwrap();
        Self { root, config }
    }

    pub fn path(&self) -> &Path {
        self.root.path()
    }

    pub fn config_path(&self) -> PathBuf {
        self.path().join("dolos.toml")
    }

    /// Replays the last block of epoch 0 and the first of epoch 1, stopping on
    /// the boundary a stele is cut at.
    pub fn replay_to_the_first_boundary(&self) -> (Arc<Genesis>, ChainPoint) {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.force_protocol = Some(9);
        let epoch_one = genesis.shelley.epoch_length.unwrap() as u64;
        let genesis = Arc::new(genesis);
        let (before, block_before) = make_conway_block_with_prev(epoch_one - 1, None, 1);
        let (boundary, block_boundary) = make_conway_block_with_prev(epoch_one, before.hash(), 2);
        let mut session = BulkReplaySession::open(&self.config, genesis.clone(), Some(1)).unwrap();
        session
            .import_blocks(vec![block_before, block_boundary])
            .unwrap();
        session.finish().unwrap();
        (genesis, boundary)
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}
