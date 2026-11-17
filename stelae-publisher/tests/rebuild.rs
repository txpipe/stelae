//! `stelae-publisher rebuild` over the old lineage v0.6.0 published, served by
//! a loopback that refuses and counts every request but a read.

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
        let loopback = Loopback::serve(root.path()).unwrap();
        let writes = Arc::new(AtomicU64::new(0));
        let counted = writes.clone();
        loopback.rule(move |seen| match seen.method {
            "GET" | "HEAD" => Verdict::Proceed,
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
        Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
            .arg("--config")
            .arg(self.node.config_path())
            .args([
                "rebuild",
                "--source",
                &self.loopback.repository(NAME),
                "--insecure",
            ])
            .args(args)
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN")
            .output()
            .unwrap()
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
