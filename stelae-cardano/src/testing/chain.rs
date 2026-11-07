//! Chains a real replay imports, written as the immutable db a publisher reads.

use std::{path::PathBuf, sync::Arc};

use dolos::engine::BulkReplaySession;
use dolos_core::{
    config::{ChainConfig, CustomUtxo},
    Genesis, RawBlock, TxoRef,
};
use dolos_snapshot::{
    export::{Document, First, Following},
    source::SnapshotSource,
    RetainedEpochs,
};
use dolos_testing::{
    blocks::{make_byron_block, make_byron_ebb, write_immutable_chunks},
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    tx_sequence_to_hash, utxo_with_value,
};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{
            conway::{Certificate, Value},
            RationalNumber, Set, StakeCredential,
        },
        traverse::MultiEraBlock,
    },
};

use super::Node;

/// The magic of the preview genesis a custom chain runs over.
const PREVIEW_MAGIC: u32 = 2;
const PREPROD_MAGIC: u32 = 1;

enum Network {
    Custom(Vec<CustomUtxo>),
    Preprod,
}

/// A chain, and the genesis and network a replay of it needs.
pub struct Chain {
    pub blocks: Vec<RawBlock>,
    pub genesis: Arc<Genesis>,
    /// Slots per immutable file, as the genesis's own geometry numbers them.
    width: u64,
    network: Network,
}

/// What [`delegation_certificates`] take in deposits on the preview genesis.
const DELEGATION_DEPOSITS: u64 = 2_000_000 + 500_000_000;

/// A stake credential registered and delegated to a pool registered beside it,
/// so every boundary writes an account row that settles one boundary late.
fn delegation_certificates() -> Vec<Certificate> {
    let operator = Hash::new([7; 28]);
    let credential = StakeCredential::AddrKeyhash(Hash::new([9; 28]));
    let mut reward_account = vec![0xe0];
    reward_account.extend_from_slice(&[9; 28]);
    vec![
        Certificate::StakeRegistration(credential.clone()),
        Certificate::PoolRegistration {
            operator,
            vrf_keyhash: Hash::new([8; 32]),
            pledge: 0,
            cost: 0,
            margin: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            reward_account: reward_account.into(),
            pool_owners: Set::from(vec![Hash::new([9; 28])]),
            relays: Vec::new(),
            pool_metadata: None,
        },
        Certificate::StakeDelegation(credential, operator),
    ]
}

impl Chain {
    /// Post-Byron blocks half an epoch apart from slot 0, so an epoch boundary
    /// falls on every second block. `shape` starts from what makes that so.
    pub fn shelley(count: usize, shape: impl FnOnce(&mut SyntheticBlockConfig)) -> Self {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.force_protocol = Some(9);
        let epoch = genesis.shelley.epoch_length.unwrap() as u64;
        let mut config = SyntheticBlockConfig {
            block_count: count,
            txs_per_block: 1,
            slot: 0,
            start_block: 0,
            slot_step: epoch / 2,
            plain_txs: true,
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        };
        shape(&mut config);
        let delegating = !config.extra_certs_by_block.is_empty();
        let (seed_address, seed_amount) = (config.seed_address.clone(), config.seed_amount);
        let width = genesis
            .shelley
            .security_param
            .map_or(21_600, |k| u64::from(k) * 10);
        let (blocks, _, mut built) = build_synthetic_blocks(config);
        if delegating {
            // The first transaction pays the deposits out of what it spends,
            // or the pots no longer add up across a boundary.
            let value = Value::Coin(seed_amount + DELEGATION_DEPOSITS);
            let first = TxoRef(tx_sequence_to_hash(1), 0);
            let seed = built
                .custom_utxos
                .iter_mut()
                .find(|utxo| utxo.ref_ == first);
            seed.expect("the first transaction's seed").cbor =
                utxo_with_value(seed_address, value).1;
        }
        Self {
            blocks,
            genesis: Arc::new(genesis),
            width,
            network: Network::Custom(built.custom_utxos),
        }
    }

    /// [`Self::shelley`] with an account delegating from the first block, and a
    /// new asset each epoch but for the epochs in `no_new_assets`, whose entity
    /// layers don't exist.
    pub fn delegating(epochs: usize, no_new_assets: &[usize]) -> Self {
        let blocks = epochs * 2;
        Self::shelley(blocks, |config| {
            let mut certificates = vec![Vec::new(); blocks];
            certificates[0] = vec![delegation_certificates()];
            config.extra_certs_by_block = certificates;
            config.asset_names_by_block = (0..blocks)
                .map(|block| {
                    let epoch = block / 2;
                    let new = !no_new_assets.contains(&epoch);
                    format!("asset{}", if new { epoch } else { 0 })
                })
                .collect();
        })
    }

    /// Byron epochs each opened by a boundary block whose slot the epoch's
    /// first main block shares, which is what filtering a later archive by slot
    /// can't tell apart.
    pub fn byron(epochs: u64) -> Self {
        let genesis = dolos_cardano::include::preprod::load();
        let width = genesis
            .shelley
            .security_param
            .map_or(21_600, |k| u64::from(k) * 10);
        let mut blocks = Vec::new();
        let mut previous = Hash::new([0; 32]);
        let mut number = 0;
        for epoch in 0..epochs {
            let (point, ebb) = make_byron_ebb(epoch, previous);
            blocks.push(ebb);
            previous = point.hash().expect("a block's point has a hash");
            for slot in [0, 10_000] {
                number += 1;
                let (point, block) = make_byron_block(epoch, slot, number, previous);
                blocks.push(block);
                previous = point.hash().expect("a block's point has a hash");
            }
        }
        Self {
            blocks,
            genesis: Arc::new(genesis),
            width,
            network: Network::Preprod,
        }
    }

    pub fn magic(&self) -> u64 {
        match self.network {
            Network::Custom(_) => PREVIEW_MAGIC.into(),
            Network::Preprod => PREPROD_MAGIC.into(),
        }
    }

    /// A node on this chain, with its stores empty.
    pub fn node(&self) -> Node {
        let mut node = Node::new();
        match &self.network {
            Network::Custom(utxos) => {
                node.on_custom_chain(PREVIEW_MAGIC, utxos.clone());
                if let Some(cardano) = &mut node.config.chain.cardano {
                    cardano.genesis.force_protocol = self.genesis.force_protocol;
                }
            }
            Network::Preprod => node.config.chain = ChainConfig::known("preprod"),
        }
        node
    }

    /// The chain as an immutable db under `node`, with each block in the file
    /// the network's geometry puts it in, and returns the download directory.
    pub fn write_immutable(&self, node: &Node) -> PathBuf {
        let download = node.path().join("mithril");
        let immutable = download.join("immutable");
        std::fs::create_dir_all(&immutable).unwrap();

        let mut files: Vec<(u64, Vec<RawBlock>)> = Vec::new();
        for block in &self.blocks {
            let number = MultiEraBlock::decode(block).unwrap().slot() / self.width;
            match files.last_mut() {
                Some((last, blocks)) if *last == number => blocks.push(block.clone()),
                _ => files.push((number, vec![block.clone()])),
            }
        }
        let chunks: Vec<&[RawBlock]> = files.iter().map(|(_, blocks)| blocks.as_slice()).collect();
        write_immutable_chunks(&immutable, &chunks);
        std::fs::remove_file(immutable.join(format!("{:05}.chunk", chunks.len()))).unwrap();
        // Highest first: numbers only grow, so no rename lands on a file not yet moved.
        for (index, (number, _)) in files.iter().enumerate().rev() {
            for extension in ["chunk", "primary", "secondary"] {
                std::fs::rename(
                    immutable.join(format!("{index:05}.{extension}")),
                    immutable.join(format!("{number:05}.{extension}")),
                )
                .unwrap();
            }
        }
        // The volatile tail a reader leaves out.
        let tail = files.last().map_or(0, |(number, _)| number + 1);
        std::fs::write(immutable.join(format!("{tail:05}.chunk")), []).unwrap();
        download
    }

    /// What an independent replay stopped at `stop` publishes, knowing nothing
    /// of a batch.
    pub fn frozen(
        &self,
        stop: u64,
        retained: &RetainedEpochs,
        previous: Option<&Document>,
    ) -> Document {
        let node = self.node();
        let mut session =
            BulkReplaySession::open(&node.config, self.genesis.clone(), Some(stop)).unwrap();
        let progress = session.import_blocks(self.blocks.clone()).unwrap();
        assert!(progress.is_boundary(), "stop {stop}: {progress:?}");
        let snapshot = session.snapshot();
        let plan = snapshot.plan(self.magic(), retained.clone()).unwrap();
        let document = match previous {
            None => snapshot.digest_document(&plan, &First).unwrap(),
            Some(previous) => snapshot
                .digest_document(&plan, &Following::read(&previous.canonical, &plan).unwrap())
                .unwrap(),
        };
        drop(snapshot);
        session.finish().unwrap();
        document
    }
}
