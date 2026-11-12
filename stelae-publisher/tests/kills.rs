//! A real process, really killed, at the points of a batched publication where
//! a rerun has something to work out. Faults in `stelae-cardano/tests/batch.rs`
//! cancel a run and put files back, over a fake repository. Here the process
//! gets SIGKILL, over the real transport, with a request of its in flight.

#![cfg(unix)]

use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
    time::{Duration, Instant},
};

use stelae_cardano::{
    batch, config,
    testing::{
        chain::Chain,
        loopback::{Loopback, Seen, Verdict},
        Node,
    },
};

const UNTIL: u64 = 6;
const WINDOW: u64 = 3;
const NAME: &str = "cardano/killed";
const PATIENCE: Duration = Duration::from_secs(900);
const SIGKILL: i32 = 9;

static CHAIN: LazyLock<Chain> = LazyLock::new(|| Chain::delegating(UNTIL as usize + 2, &[]));

/// What one uninterrupted run publishes, and what its log says it traversed.
static UNINTERRUPTED: LazyLock<(BTreeMap<String, Vec<u8>>, String)> = LazyLock::new(|| {
    let run = Run::new();
    let mut child = run.spawn();
    run.finishes(&mut child);
    assert_eq!(run.loopback.accounting(NAME).tags.len() as u64, UNTIL * 2);
    (run.loopback.manifests(NAME), run.log())
});

/// One node and one registry, and a process to start over them.
struct Run {
    node: Node,
    download: PathBuf,
    config: PathBuf,
    epochs: PathBuf,
    loopback: Loopback,
    /// The registry's files, kept for as long as it serves them.
    _registry: tempfile::TempDir,
    log: PathBuf,
}

impl Run {
    fn new() -> Self {
        let node = CHAIN.node();
        let download = CHAIN.write_immutable(&node);
        let config = node.write_config();
        let epochs = node.path().join("state-epochs");
        std::fs::write(&epochs, "2\n").unwrap();
        let registry = tempfile::tempdir().unwrap();
        let loopback = Loopback::serve(registry.path()).unwrap();
        let log = node.path().join("publisher.log");
        Self {
            node,
            download,
            config,
            epochs,
            loopback,
            _registry: registry,
            log,
        }
    }

    fn storage(&self) -> &Path {
        &self.node.config.storage.path
    }

    fn spawn(&self) -> Child {
        let log = File::options()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        // A listing builds a verifier from a real key, though it checks nothing.
        let known = config::mithril_source(&Node::new().config, None, None).unwrap();
        Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
            .arg("--config")
            .arg(&self.config)
            .arg("backfill")
            .args(["--repo", &self.loopback.repository(NAME), "--insecure"])
            .arg("--state-epochs-file")
            .arg(&self.epochs)
            .args(["--mithril-aggregator", &self.loopback.aggregator()])
            .args(["--mithril-genesis-vkey", &known.genesis_vkey])
            .arg("--download-dir")
            .arg(&self.download)
            .args(["--until-epoch", &UNTIL.to_string()])
            .args(["--window", "1", "--skip-validation"])
            .args(["--batch-epochs", &WINDOW.to_string()])
            .args(["--batch-scratch-mib", "4096", "--batch-reserve-mib", "0"])
            .env("NO_COLOR", "1")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap()
    }

    fn finishes(&self, child: &mut Child) {
        let began = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            self.give_up_after_patience(child, began, "never finished");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "{status}: {}", self.log());
    }

    /// Wait for `reached`, and kill the process then. The kill has to be what
    /// ends it: a process that finished first leaves a rerun nothing to do.
    fn kill_when(&self, child: &mut Child, reached: impl Fn() -> bool) {
        let began = Instant::now();
        while !reached() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("exited {status} before the kill: {}", self.log());
            }
            self.give_up_after_patience(child, began, "never reached");
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(SIGKILL), "{status}: {}", self.log());
    }

    /// Past `PATIENCE`, kill the process and fail with its log.
    fn give_up_after_patience(&self, child: &mut Child, began: Instant, what: &str) {
        if began.elapsed() > PATIENCE {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what} in {PATIENCE:?}: {}", self.log());
        }
    }

    /// Kill the process once the registry holds a request of its.
    fn kill_on_hold(&self, child: &mut Child) {
        self.kill_when(child, || {
            self.loopback.wait_for_hold(Duration::from_millis(5))
        });
        self.loopback.release();
    }

    /// Rerun, and hold the repository to the run nothing interrupted.
    fn rerun_converges(&self) {
        let mut child = self.spawn();
        self.finishes(&mut child);
        let found = self.loopback.manifests(NAME);
        let (expected, _) = &*UNINTERRUPTED;
        assert_eq!(
            found.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        );
        for (tag, manifest) in expected {
            assert_eq!(&found[tag], manifest, "at {tag}: {}", self.log());
        }
        // The registry takes a manifest whatever it names, so a seal over a
        // blob that never landed shows only here.
        let absent = self.loopback.absent_blobs(NAME);
        assert!(absent.is_empty(), "named and never held: {absent:?}");
        let dir = batch::dir_in(self.storage());
        assert!(!dir.join("journal.json").exists(), "{}", self.log());
        assert!(!dir.join("staging").exists(), "{}", self.log());
        self.assert_every_write_kept_to_order();
    }

    /// As [`Self::rerun_converges`], uploading nothing that went up before it.
    fn rerun_converges_without_another_upload(&self) {
        let uploaded = self.loopback.accounting(NAME).uploads;
        self.rerun_converges();
        let accounting = self.loopback.accounting(NAME);
        for (digest, (_, count)) in &uploaded {
            assert_eq!(
                accounting.uploads[digest].1, *count,
                "{digest} went up again"
            );
        }
    }

    /// Through kills and reruns, `latest` only ever moved to the next sequence,
    /// and nothing was written under a sequence it had already reached.
    fn assert_every_write_kept_to_order(&self) {
        let digests: BTreeMap<String, u64> = (1..=UNTIL)
            .map(|sequence| {
                let manifest = &UNINTERRUPTED.0[&format!("epoch-{sequence}")];
                let digest = stelae::digest::Digest::compute(manifest).to_hex();
                (format!("sha256:{digest}"), sequence)
            })
            .collect();
        let mut latest = 0;
        for (tag, digest) in self.loopback.accounting(NAME).tags {
            if tag == "latest" {
                let sequence = digests[&digest];
                assert!(
                    sequence == latest || sequence == latest + 1,
                    "latest moved from {latest} to {sequence}"
                );
                latest = sequence;
            } else {
                let sequence: u64 = tag.strip_prefix("epoch-").unwrap().parse().unwrap();
                assert!(sequence > latest, "{tag} rewritten with latest at {latest}");
            }
        }
        assert_eq!(latest, UNTIL);
    }

    fn latest_is(&self, sequence: u64) -> bool {
        let tag = format!("epoch-{sequence}");
        self.loopback.manifest(NAME, "latest").is_some()
            && self.loopback.manifest(NAME, "latest") == self.loopback.manifest(NAME, &tag)
    }

    fn tag_exists(&self, sequence: u64) -> bool {
        let tag = format!("epoch-{sequence}");
        self.loopback.manifest(NAME, &tag).is_some()
    }
}

/// A rule acting on the `nth` request `matches` sees, and every one after it.
fn from_the_nth(
    matches: impl Fn(&Seen<'_>) -> bool + Send + Sync + 'static,
    nth: u64,
    verdict: Verdict,
) -> impl Fn(&Seen<'_>) -> Verdict + Send + Sync + 'static {
    let seen = AtomicU64::new(0);
    move |request| {
        if !matches(request) {
            return Verdict::Proceed;
        }
        match seen.fetch_add(1, Ordering::SeqCst) + 1 >= nth {
            true => verdict,
            false => Verdict::Proceed,
        }
    }
}

fn sets_latest(request: &Seen<'_>) -> bool {
    request.method == "PUT" && request.path.ends_with("/manifests/latest")
}

/// One traversal of each family per batch, counted in the log of the whole
/// process, whichever thread started it.
#[test]
fn an_uninterrupted_run_traverses_once_a_batch() {
    let (_, log) = &*UNINTERRUPTED;
    let count = |kind: &str| {
        log.lines()
            .filter(|line| line.contains("index staging traversal started"))
            .filter(|line| line.contains(&format!("traversal=\"{kind}\"")))
            .count() as u64
    };
    let batches = UNTIL.div_ceil(WINDOW);
    assert_eq!(count("tag"), batches, "{log}");
    assert_eq!(count("exact"), batches, "{log}");
    assert!(count("entity") <= batches, "{log}");
    assert!(count("entity") >= 1, "{log}");
}

#[test]
fn a_kill_after_a_capture_converges() {
    let run = Run::new();
    let bundle = batch::dir_in(run.storage()).join("prepared/bundles/2.json");
    let mut child = run.spawn();
    run.kill_when(&mut child, || bundle.exists());
    run.rerun_converges();
}

#[test]
fn a_kill_after_a_staging_marker_converges() {
    let run = Run::new();
    let staging = batch::dir_in(run.storage()).join("staging");
    let mut child = run.spawn();
    run.kill_when(&mut child, || {
        std::fs::read_dir(&staging).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        })
    });
    run.rerun_converges();
}

#[test]
fn a_kill_during_an_upload_converges() {
    let run = Run::new();
    const HELD_FROM: u64 = 20;
    run.loopback.rule(from_the_nth(
        |request| request.method == "PUT" && request.path.contains("/blobs/uploads/"),
        HELD_FROM,
        Verdict::HoldUnserved,
    ));
    let mut child = run.spawn();
    run.kill_on_hold(&mut child);
    assert!(
        !run.tag_exists(1),
        "the kill was meant to land before a seal"
    );
    let completed: u64 = run
        .loopback
        .accounting(NAME)
        .uploads
        .values()
        .map(|(_, count)| count)
        .sum();
    assert!(completed < HELD_FROM, "a held upload landed: {completed}");
    run.rerun_converges();
}

#[test]
fn a_kill_between_a_seals_two_tags_converges() {
    let run = Run::new();
    run.loopback
        .rule(from_the_nth(sets_latest, 2, Verdict::HoldUnserved));
    let mut child = run.spawn();
    run.kill_on_hold(&mut child);
    assert!(run.tag_exists(2) && run.latest_is(1), "not half a seal");
    run.rerun_converges();
}

#[test]
fn a_kill_before_a_seals_answer_converges_without_another_upload() {
    let run = Run::new();
    run.loopback
        .rule(from_the_nth(sets_latest, 2, Verdict::HoldAnswer));
    let mut child = run.spawn();
    run.kill_on_hold(&mut child);
    assert!(run.latest_is(2), "the seal was meant to land");
    run.rerun_converges_without_another_upload();
}

#[test]
fn a_kill_between_two_sequences_converges() {
    let run = Run::new();
    let sealed = AtomicU64::new(0);
    run.loopback.rule(move |request| {
        if sets_latest(request) {
            sealed.fetch_add(1, Ordering::SeqCst);
            return Verdict::Proceed;
        }
        // The read the next sequence starts with: the last one is journaled.
        let reads = request.method == "GET" && request.path.ends_with("/manifests/latest");
        match reads && sealed.load(Ordering::SeqCst) == 2 {
            true => Verdict::Hold,
            false => Verdict::Proceed,
        }
    });
    let mut child = run.spawn();
    run.kill_on_hold(&mut child);
    assert!(run.latest_is(2));
    run.rerun_converges();
}

/// A retry repeats only what the registry refused, and what it refused still
/// crossed the wire.
#[test]
fn uploads_the_registry_refuses_are_retried_to_the_same_repository() {
    let run = Run::new();
    let seen = AtomicU64::new(0);
    run.loopback.rule(move |request| {
        let uploads = request.method == "PUT" && request.path.contains("/blobs/uploads/");
        match uploads && (seen.fetch_add(1, Ordering::SeqCst) + 1).is_multiple_of(25) {
            true => Verdict::Refuse(503),
            false => Verdict::Proceed,
        }
    });
    run.rerun_converges();
    let log = run.log();
    assert!(!log.contains("another writer moved it"), "{log}");
    let accounting = run.loopback.accounting(NAME);
    assert!(
        accounting.received > accounting.uploaded(),
        "{accounting:?}"
    );
}

/// The last seal lands and the process dies before it hears so, with the batch
/// still journaled: the rerun reproduces the last sequence to compare it,
/// journals it and clears the batch, uploading nothing again. A cleanup stopped
/// part way is `tests/batch.rs`'s, since nothing here can stop the process
/// between two of its file removals.
#[test]
fn a_kill_as_the_last_seal_lands_converges_without_another_upload() {
    let run = Run::new();
    run.loopback
        .rule(from_the_nth(sets_latest, UNTIL, Verdict::HoldAnswer));
    let mut child = run.spawn();
    run.kill_on_hold(&mut child);
    assert!(run.latest_is(UNTIL), "the last seal was meant to land");
    let journal = batch::dir_in(run.storage()).join("journal.json");
    assert!(journal.exists(), "the kill left nothing to recover");
    run.rerun_converges_without_another_upload();
}
