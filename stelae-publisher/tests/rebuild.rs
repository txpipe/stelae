//! `stelae-publisher rebuild` over the old lineage v0.6.0 published, served by
//! a loopback that refuses and counts every request but a read, and
//! `rebuild blocks` over an old lineage of preprod's first immutable files.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use serde_json::{json, Value};
use stelae::Digest;
use stelae_cardano::testing::{
    loopback::{Loopback, Verdict},
    rebuild::{
        blocks::{digest_list, exported_lineage, immutable, lay_old_blocks_lineage, EPOCHS},
        lay_old_lineage,
    },
    Node,
};

const NAME: &str = "cardano/old";
const UNTIL: u64 = 8;

struct Served {
    root: tempfile::TempDir,
    loopback: Loopback,
    writes: Arc<AtomicU64>,
    node: Node,
}

impl Served {
    fn new() -> Self {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../stelae-cardano/tests/data/rebuild-v0.6.0");
        let root = tempfile::tempdir().unwrap();
        for part in ["manifests/tags", "blobs/sha256"] {
            let to = root.path().join(NAME).join(part);
            std::fs::create_dir_all(&to).unwrap();
            for entry in std::fs::read_dir(fixture.join(part)).unwrap() {
                let entry = entry.unwrap();
                std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
            }
        }
        Self::serving(root, None)
    }

    /// The lineage with a blob behind every layer, refusing and counting the
    /// writes outside its `dolos` repository.
    fn writable() -> Self {
        let root = tempfile::tempdir().unwrap();
        lay_old_lineage(root.path(), NAME);
        Self::serving(root, Some(format!("/v2/{NAME}/dolos/")))
    }

    fn serving(root: tempfile::TempDir, writable: Option<String>) -> Self {
        let loopback = Loopback::serve(root.path()).unwrap();
        let writes = Arc::new(AtomicU64::new(0));
        let counted = writes.clone();
        loopback.rule(move |seen| match seen.method {
            "GET" | "HEAD" => Verdict::Proceed,
            _ if writable
                .as_ref()
                .is_some_and(|prefix| seen.path.starts_with(prefix)) =>
            {
                Verdict::Proceed
            }
            _ => {
                counted.fetch_add(1, Ordering::SeqCst);
                Verdict::Refuse(405)
            }
        });
        Self {
            root,
            loopback,
            writes,
            node: Node::new(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.node.path().join(name)
    }

    /// A journal naming a blocks snapshot at every sequence, and a kept-tips
    /// file.
    fn inputs(&self) -> (PathBuf, PathBuf) {
        let journal: String = (1..=UNTIL)
            .map(|sequence| format!("{sequence} {}\n", Digest::compute(sequence.to_be_bytes())))
            .collect();
        std::fs::write(self.path("journal"), journal).unwrap();
        std::fs::write(self.path("kept"), "3\n7\n").unwrap();
        (self.path("journal"), self.path("kept"))
    }

    fn run(&self, args: &[&str]) -> Output {
        let source = self.loopback.repository(NAME);
        let mut all = vec!["--source", &source];
        all.extend(args);
        self.rebuild(&all)
    }

    /// `stelae-publisher rebuild --insecure` and `args`.
    fn rebuild(&self, args: &[&str]) -> Output {
        self.rebuild_with(&self.node.config_path(), args)
    }

    /// The same, configured by `config`.
    fn rebuild_with(&self, config: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
            .arg("--config")
            .arg(config)
            .args(["rebuild", "--insecure"])
            .args(args)
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN")
            .output()
            .unwrap()
    }

    /// A plan of the lineage keeping the tips at 3 and 7, written by the
    /// command.
    fn plan(&self) -> PathBuf {
        let (journal, kept) = self.inputs();
        let plan = self.path("rebuild.plan");
        let output = self.run(&[
            "--blocks-journal",
            journal.to_str().unwrap(),
            "--kept-tips-file",
            kept.to_str().unwrap(),
            "--plan",
            plan.to_str().unwrap(),
        ]);
        assert!(output.status.success(), "{}", text(&output.stderr));
        plan
    }

    /// Give `epoch-8` a layer no array of the chain holds, keeping its
    /// manifest and inscription consistent.
    fn add_a_layer_to_the_newest(&self) {
        let repository = self.root.path().join(NAME);
        let tag = repository.join(format!("manifests/tags/epoch-{UNTIL}"));
        let mut manifest: Value = serde_json::from_slice(&std::fs::read(&tag).unwrap()).unwrap();
        let config = manifest["config"]["digest"]
            .as_str()
            .unwrap()
            .replace("sha256:", "");
        let config = repository.join("blobs/sha256").join(config);
        let mut inscription: Value =
            serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();

        let diff_id = Digest::compute(b"a layer the chain does not hold").to_string();
        let mut layer = inscription["layers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|layer| layer["kind"] == "entity-asset")
            .unwrap()
            .clone();
        // Sealed by the boundary into 9, so no snapshot up to 8 lists it.
        layer["scope"] = json!({"epoch": 7, "startSlot": 604800, "endSlot": 691199});
        layer["diffId"] = json!(diff_id);
        inscription["layers"]
            .as_array_mut()
            .unwrap()
            .push(layer.clone());
        let bytes = stelae::canonical_json(&inscription).unwrap();
        let digest = Digest::compute(&bytes);
        std::fs::write(
            repository.join("blobs/sha256").join(digest.to_hex()),
            &bytes,
        )
        .unwrap();

        manifest["config"]["digest"] = json!(digest.to_string());
        manifest["config"]["size"] = json!(bytes.len());
        let mut blob = manifest["layers"][0].clone();
        blob["mediaType"] = layer["mediaType"].clone();
        blob["annotations"]["store.stelae.layer.diffId"] = json!(diff_id);
        manifest["layers"].as_array_mut().unwrap().push(blob);
        std::fs::write(&tag, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn a_rebuild_prints_its_digests_and_writes_its_plan_and_nothing_else() {
    let served = Served::new();
    let (journal, kept) = served.inputs();
    let plan = served.path("rebuild.plan");
    let output = served.run(&[
        "--blocks-journal",
        journal.to_str().unwrap(),
        "--kept-tips-file",
        kept.to_str().unwrap(),
        "--plan",
        plan.to_str().unwrap(),
        "--print-digests",
    ]);

    assert!(output.status.success(), "{}", text(&output.stderr));
    let lines: Vec<String> = std::fs::read_to_string(&plan)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), UNTIL as usize + 1);
    let printed: Vec<String> = lines[1..]
        .iter()
        .map(|line| {
            let planned: Value = serde_json::from_str(line).unwrap();
            format!(
                "{} {}",
                planned["sequence"],
                planned["inscriptionDigest"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(text(&output.stdout).lines().collect::<Vec<_>>(), printed);

    let stderr = text(&output.stderr);
    assert!(
        stderr.contains("epoch-1 to epoch-8, every sequence self-checked"),
        "{stderr}"
    );
    assert!(stderr.contains("kept tips: 3, 7, 8"), "{stderr}");
    assert!(!plan.with_extension("partial").exists());
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}

#[test]
fn without_a_journal_a_rebuild_reports_its_self_check_alone() {
    let served = Served::new();
    let output = served.run(&[]);

    assert!(output.status.success(), "{}", text(&output.stderr));
    let stdout = text(&output.stdout);
    assert!(stdout.contains("every sequence self-checked"), "{stdout}");
    assert!(
        stdout.contains("the assignment and the self-check only"),
        "{stdout}"
    );

    let refused = served.run(&["--plan", served.path("plan").to_str().unwrap()]);
    assert_eq!(refused.status.code(), Some(2), "{}", text(&refused.stderr));
}

#[test]
fn a_failed_self_check_exits_three_and_leaves_no_plan() {
    let served = Served::new();
    served.add_a_layer_to_the_newest();
    let (journal, _) = served.inputs();
    let plan = served.path("rebuild.plan");
    let output = served.run(&[
        "--blocks-journal",
        journal.to_str().unwrap(),
        "--plan",
        plan.to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(3), "{}", text(&output.stderr));
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains("the self-check refused epoch-8"),
        "{stderr}"
    );
    assert!(stderr.contains("entity-asset"), "{stderr}");
    assert!(!plan.exists());
    assert!(!plan.with_extension("partial").exists());
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}

#[test]
fn a_written_plan_lands_under_the_root_and_a_rerun_writes_nothing() {
    let served = Served::writable();
    let plan = served.plan();
    let write = || served.rebuild(&["--write", "--plan", plan.to_str().unwrap()]);

    let output = write();
    assert!(output.status.success(), "{}", text(&output.stderr));
    let stdout = text(&output.stdout);
    let target = served.loopback.repository(&format!("{NAME}/dolos"));
    assert!(
        stdout.contains(&format!(
            "{target}: epoch-1 to epoch-8, resolved as planned"
        )),
        "{stdout}"
    );
    assert!(
        stdout.contains("tags: 12 written, 0 already there"),
        "{stdout}"
    );

    let tags = served.loopback.manifests(&format!("{NAME}/dolos"));
    assert_eq!(tags.len(), 12, "{:?}", tags.keys());
    assert!(tags.contains_key("epoch-7.tip") && tags.contains_key("latest"));

    let again = write();
    assert!(again.status.success(), "{}", text(&again.stderr));
    let stdout = text(&again.stdout);
    assert!(
        stdout.contains("tags: 0 written, 12 already there"),
        "{stdout}"
    );
    assert!(stdout.contains("blobs: 0 copied, 0 pulled"), "{stdout}");
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}

#[test]
fn a_lineage_that_does_not_resolve_exits_three_and_moves_no_latest() {
    let served = Served::writable();
    let plan = served.plan();
    let broken = format!("/v2/{NAME}/dolos/manifests/epoch-4");
    let writable = format!("/v2/{NAME}/dolos/");
    served.loopback.rule(move |seen| match seen.method {
        "GET" if seen.path == broken => Verdict::Refuse(404),
        "GET" | "HEAD" => Verdict::Proceed,
        _ if seen.path.starts_with(&writable) => Verdict::Proceed,
        _ => Verdict::Refuse(405),
    });

    let output = served.rebuild(&["--write", "--plan", plan.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(3), "{}", text(&output.stderr));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("does not resolve at epoch-8"), "{stderr}");
    let dolos = format!("{NAME}/dolos");
    assert!(served.loopback.manifest(&dolos, "epoch-8").is_some());
    assert!(served.loopback.manifest(&dolos, "latest").is_none());
}

#[test]
fn a_registry_failing_the_closing_pass_exits_one_and_moves_no_latest() {
    let served = Served::writable();
    let plan = served.plan();
    let tag = served
        .root
        .path()
        .join(format!("{NAME}/dolos/manifests/tags/epoch-{UNTIL}"));
    let head = format!("/v2/{NAME}/dolos/manifests/epoch-{UNTIL}");
    let writable = format!("/v2/{NAME}/dolos/");
    served.loopback.rule(move |seen| match seen.method {
        // Only once it is written: the run first reads it as absent.
        "GET" if seen.path == head && tag.exists() => Verdict::Refuse(401),
        "GET" | "HEAD" => Verdict::Proceed,
        _ if seen.path.starts_with(&writable) => Verdict::Proceed,
        _ => Verdict::Refuse(405),
    });

    let output = served.rebuild(&["--write", "--plan", plan.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output.stderr));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("resolving epoch-8"), "{stderr}");
    let dolos = format!("{NAME}/dolos");
    assert!(served.loopback.manifest(&dolos, "epoch-8").is_some());
    assert!(served.loopback.manifest(&dolos, "latest").is_none());
}

#[test]
fn a_plan_of_another_network_than_the_configured_one_writes_nothing() {
    let served = Served::writable();
    let plan = served.plan();
    let config = std::fs::read_to_string(served.node.config_path()).unwrap();
    let preprod = config.replace(r#"network = "preview""#, r#"network = "preprod""#);
    assert_ne!(preprod, config);
    let preprod_config = served.path("preprod.toml");
    std::fs::write(&preprod_config, preprod).unwrap();

    let output = served.rebuild_with(
        &preprod_config,
        &["--write", "--plan", plan.to_str().unwrap()],
    );
    assert_eq!(output.status.code(), Some(1), "{}", text(&output.stderr));
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains("epoch-1 of the plan is not on the configured network"),
        "{stderr}"
    );
    let dolos = format!("{NAME}/dolos");
    assert_eq!(served.loopback.accounting(&dolos), Default::default());
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}

#[test]
fn a_write_takes_its_source_from_the_plan_alone() {
    let served = Served::new();
    let plan = served.path("plan");
    let source = served.loopback.repository(NAME);
    for args in [
        vec!["--write"],
        vec![
            "--write",
            "--plan",
            plan.to_str().unwrap(),
            "--source",
            &source,
        ],
        vec!["--source", &source, "--root", &source],
        vec![
            "--source",
            &source,
            "--direct-s3-config",
            plan.to_str().unwrap(),
        ],
    ] {
        let output = served.rebuild(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            text(&output.stderr)
        );
    }
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}

/// The network root `rebuild blocks` uploads to, another than the source's.
const BLOCKS_ROOT: &str = "cardano/new";

/// The old blocks lineage `epoch-1` to `epoch-3` under [`NAME`], served
/// beside an empty [`BLOCKS_ROOT`] to a node configured for preprod. Writes
/// to the root's `blocks` repository proceed; every other write is refused.
struct OldBlocks {
    _root: tempfile::TempDir,
    immutable: tempfile::TempDir,
    loopback: Loopback,
    /// Every request but a read, refused or not.
    writes: Arc<AtomicU64>,
    /// The writes outside the root's `blocks` repository.
    refused: Arc<AtomicU64>,
    node: Node,
    config: PathBuf,
    exported: Vec<Digest>,
}

impl OldBlocks {
    fn new() -> Self {
        let immutable = immutable();
        let root = tempfile::tempdir().unwrap();
        lay_old_blocks_lineage(root.path(), NAME, immutable.path());
        let loopback = Loopback::serve(root.path()).unwrap();
        let (writes, refused) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let (all, outside) = (writes.clone(), refused.clone());
        let writable = format!("/v2/{BLOCKS_ROOT}/blocks/");
        loopback.rule(move |seen| match seen.method {
            "GET" | "HEAD" => Verdict::Proceed,
            _ => {
                all.fetch_add(1, Ordering::SeqCst);
                match seen.path.starts_with(&writable) {
                    true => Verdict::Proceed,
                    false => {
                        outside.fetch_add(1, Ordering::SeqCst);
                        Verdict::Refuse(405)
                    }
                }
            }
        });

        let node = Node::new();
        let preview = std::fs::read_to_string(node.config_path()).unwrap();
        let preprod = preview.replace(r#"network = "preview""#, r#"network = "preprod""#);
        assert_ne!(preprod, preview);
        let config = node.path().join("preprod.toml");
        std::fs::write(&config, preprod).unwrap();

        Self {
            exported: exported_lineage(immutable.path()),
            _root: root,
            immutable,
            loopback,
            writes,
            refused,
            node,
            config,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.node.path().join(name)
    }

    /// A saved digest list certifying every file.
    fn digests(&self) -> PathBuf {
        let path = self.path("digests.json");
        std::fs::write(&path, digest_list(self.immutable.path(), 0..EPOCHS)).unwrap();
        path
    }

    fn root(&self) -> String {
        self.loopback.repository(BLOCKS_ROOT)
    }

    /// `stelae-publisher rebuild blocks --insecure` from the old lineage,
    /// comparing with `digests`, and `args`.
    fn run(&self, digests: &Path, journal: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
            .arg("--config")
            .arg(&self.config)
            .args(["rebuild", "blocks", "--insecure", "--source"])
            .arg(self.loopback.repository(NAME))
            .arg("--journal")
            .arg(journal)
            .arg("--mithril-digests")
            .arg(digests)
            .args(args)
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN")
            .env_remove("DOLOS_MITHRIL_AGGREGATOR")
            // Plain reports, whatever the caller's terminal settings.
            .env("NO_COLOR", "1")
            .env_remove("FORCE_COLOR")
            .env_remove("CLICOLOR_FORCE")
            .output()
            .unwrap()
    }

    /// The journal of the exported lineage through `through`.
    fn journal(&self, through: u64) -> String {
        (1..=through)
            .map(|sequence| format!("{sequence} {}\n", self.exported[sequence as usize - 1]))
            .collect()
    }

    /// The inscription digest each `epoch-N` of `repository` names.
    fn sealed(&self, repository: &str) -> Vec<String> {
        let tags = self.loopback.manifests(repository);
        (1..=tags.keys().filter(|tag| tag.starts_with("epoch-")).count())
            .map(|sequence| {
                let manifest: Value =
                    serde_json::from_slice(&tags[&format!("epoch-{sequence}")]).unwrap();
                manifest["config"]["digest"].as_str().unwrap().to_owned()
            })
            .collect()
    }
}

#[test]
fn a_blocks_upload_seals_the_lineage_under_the_root_and_writes_nothing_else() {
    let served = OldBlocks::new();
    let journal = served.path("blocks.journal");
    assert!(!journal.exists());
    let root = served.root();

    let output = served.run(&served.digests(), &journal, &["--upload", &root]);
    assert!(output.status.success(), "{}", text(&output.stderr));
    let stdout = text(&output.stdout);
    assert!(
        stdout.contains("epoch-1 to epoch-3, 0 resumed from the journal, 3 re-encoded"),
        "{stdout}"
    );
    assert!(
        stdout.contains("compared with Mithril: 3 immutable files, 0 mismatches"),
        "{stdout}"
    );
    assert!(
        stdout.contains(&format!("uploaded to the blocks repository of {root}")),
        "{stdout}"
    );

    assert_eq!(
        std::fs::read_to_string(&journal).unwrap(),
        served.journal(EPOCHS)
    );
    let exported: Vec<String> = served.exported.iter().map(Digest::to_string).collect();
    assert_eq!(served.sealed(&format!("{BLOCKS_ROOT}/blocks")), exported);
    assert!(served.writes.load(Ordering::SeqCst) > 0);
    assert_eq!(served.refused.load(Ordering::SeqCst), 0);
    assert!(served
        .loopback
        .manifests(&format!("{NAME}/blocks"))
        .is_empty());

    // A rerun resumes the whole journal and seals nothing more.
    let again = served.run(&served.digests(), &journal, &["--upload", &root]);
    assert!(again.status.success(), "{}", text(&again.stderr));
    assert!(
        text(&again.stdout).contains("3 resumed from the journal, 0 re-encoded"),
        "{}",
        text(&again.stdout)
    );
    assert_eq!(
        std::fs::read_to_string(&journal).unwrap(),
        served.journal(EPOCHS)
    );
}

#[test]
fn a_blocks_run_without_upload_journals_the_lineage_and_writes_to_no_registry() {
    let served = OldBlocks::new();
    let journal = served.path("blocks.journal");

    let output = served.run(&served.digests(), &journal, &[]);
    assert!(output.status.success(), "{}", text(&output.stderr));
    let stdout = text(&output.stdout);
    assert!(stdout.contains("nothing written to a registry"), "{stdout}");

    assert_eq!(
        std::fs::read_to_string(&journal).unwrap(),
        served.journal(EPOCHS)
    );
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
    assert!(served
        .loopback
        .manifests(&format!("{BLOCKS_ROOT}/blocks"))
        .is_empty());
}

#[test]
fn a_file_mithril_lists_otherwise_exits_three_and_is_named() {
    let served = OldBlocks::new();
    let journal = served.path("blocks.journal");
    let mut list: Value =
        serde_json::from_slice(&digest_list(served.immutable.path(), 0..EPOCHS)).unwrap();
    let file = list
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|item| item["immutable_file_name"] == "00001.chunk")
        .unwrap();
    file["digest"] = json!(Digest::compute(b"not file 1").to_hex());
    let digests = served.path("altered.json");
    std::fs::write(&digests, serde_json::to_vec(&list).unwrap()).unwrap();

    let output = served.run(&digests, &journal, &[]);
    assert_eq!(output.status.code(), Some(3), "{}", text(&output.stderr));
    // The report as one line: miette wraps it in a frame.
    let stderr = text(&output.stderr)
        .split(|c: char| c.is_whitespace() || c == '│')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(stderr.contains("refused epoch 1"), "{stderr}");
    assert!(stderr.contains("immutable file 00001"), "{stderr}");
    // Epoch 0 was sealed before the refusal; epoch 1 is not journaled.
    assert_eq!(
        std::fs::read_to_string(&journal).unwrap(),
        served.journal(1)
    );
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);

    // Rerun with a list certifying every file: the run continues after the
    // journal's last line, appending to it rather than writing over it.
    let again = served.run(&served.digests(), &journal, &[]);
    assert!(again.status.success(), "{}", text(&again.stderr));
    assert!(
        text(&again.stdout).contains("1 resumed from the journal, 2 re-encoded"),
        "{}",
        text(&again.stdout)
    );
    assert_eq!(
        std::fs::read_to_string(&journal).unwrap(),
        served.journal(EPOCHS)
    );
    assert_eq!(served.writes.load(Ordering::SeqCst), 0);
}
