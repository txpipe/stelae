//! Serve the disposable registry and Mithril listing a measurement publishes
//! to. See `docs/batched-export-measurement.md`.
//!
//! ```text
//! loopback ROOT PORT
//! loopback replay CONFIG IMMUTABLE_DIR EPOCH
//! ```
//!
//! With `LOOPBACK_FAIL_UPLOAD_EVERY=N` the registry answers 503 to every Nth
//! blob upload, which is what a retry costs. With
//! `LOOPBACK_DISCARD_ABOVE=BYTES` it keeps only the size of larger blobs, so
//! its writes don't compete with the publisher's on the volume being measured.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dolos::engine::BulkReplaySession;
use pallas::{interop::hardano::storage::immutable, network::miniprotocols::Point};
use stelae_cardano::testing::loopback::{Loopback, Verdict};

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["replay", config, immutable, epoch] => replay(config, immutable, epoch),
        [root, port] => serve(root, port),
        _ => {
            eprintln!("usage: loopback ROOT PORT | loopback replay CONFIG IMMUTABLE_DIR EPOCH");
            std::process::exit(2);
        }
    }
}

/// Serve `root` until killed.
fn serve(root: &str, port: &str) {
    let loopback = Loopback::serve_on(Path::new(root), port.parse().unwrap()).unwrap();
    if let Ok(every) = std::env::var("LOOPBACK_FAIL_UPLOAD_EVERY") {
        let every: u64 = every.parse().unwrap();
        let seen = AtomicU64::new(0);
        loopback.rule(move |request| {
            let uploads = request.method == "PUT" && request.path.contains("/blobs/uploads/");
            match uploads && (seen.fetch_add(1, Ordering::SeqCst) + 1).is_multiple_of(every) {
                true => Verdict::Refuse(503),
                false => Verdict::Proceed,
            }
        });
    }
    if let Ok(bytes) = std::env::var("LOOPBACK_DISCARD_ABOVE") {
        loopback.discard_above(bytes.parse().unwrap());
    }
    println!("{}", loopback.port());
    loop {
        std::thread::park();
    }
}

/// Replay an immutable db into empty stores, up to the first block of `epoch`,
/// publishing nothing: the base every arm of a measurement copies.
fn replay(config: &str, immutable: &str, epoch: &str) {
    let config = stelae_cardano::config::load(Some(Path::new(config))).unwrap();
    let genesis = Arc::new(stelae_cardano::config::genesis(&config).unwrap());
    let epoch: u64 = epoch.parse().unwrap();
    let mut session = BulkReplaySession::open(&config, genesis, Some(epoch)).unwrap();
    let blocks = immutable::read_blocks_from_point(Path::new(immutable), Point::Origin).unwrap();
    let mut pending = Vec::with_capacity(100);
    let mut boundary = false;
    for block in blocks {
        pending.push(Arc::new(block.unwrap()));
        if pending.len() == 100 {
            boundary = session
                .import_blocks(std::mem::take(&mut pending))
                .unwrap()
                .is_boundary();
            if boundary {
                break;
            }
        }
    }
    if !boundary && !pending.is_empty() {
        boundary = session.import_blocks(pending).unwrap().is_boundary();
    }
    assert!(boundary, "the immutable db ends before epoch {epoch}");
    println!("{:?}", session.committed_position().unwrap());
    session.finish().unwrap();
}
