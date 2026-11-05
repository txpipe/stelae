//! A bounded preview node for tests, here so the publisher's binary tests can
//! build one without a Dolos edge of their own.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use dolos::engine::BulkReplaySession;
use dolos_core::{
    config::{CardanoConfig, ChainConfig, CustomUtxo, GenesisConfig, RootConfig, UpstreamConfig},
    ChainPoint, Genesis,
};
use dolos_testing::blocks::make_conway_block_with_prev;

/// A registry holding nothing: it answers every request with a 404, so a
/// publish reads it as empty, spools, stages and fails at its first upload.
pub fn empty_registry() -> String {
    use std::io::{Read as _, Write as _};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut request = [0; 4096];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    format!("oci://127.0.0.1:{port}/cardano/test")
}

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

    /// Put the node on a chain seeding `utxos` over the preview genesis, which
    /// a replay of synthetic blocks spends from.
    pub fn on_custom_chain(&mut self, magic: u32, utxos: Vec<CustomUtxo>) {
        dolos_cardano::include::preview::save(self.path()).unwrap();
        self.config.chain = ChainConfig::custom(CardanoConfig {
            magic,
            is_testnet: true,
            genesis: GenesisConfig::in_dir(self.path()),
            custom_utxos: utxos,
        });
        self.config.upstream = Some(UpstreamConfig::peer("unused.invalid:3001"));
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
