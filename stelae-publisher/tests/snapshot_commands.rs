use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use stelae::progress::Observer;
use stelae_cardano::{
    config,
    publisher::{publish_once, Destination},
    testing::Node,
    Selection,
};

/// A node at its first boundary, published to a directory retaining a dump of
/// epoch 1, and the state epochs file that says so.
struct Fixture {
    node: Node,
    stele: PathBuf,
    epochs: PathBuf,
}

impl Fixture {
    fn published() -> Self {
        let node = Node::new();
        let (genesis, _) = node.replay_to_the_first_boundary();
        let stele = node.path().join("stele");
        let epochs = node.path().join("state-epochs");
        std::fs::write(&epochs, "1\n").unwrap();
        publish_once(
            &node.config,
            &genesis,
            &config::read_state_epochs(&epochs).unwrap(),
            Selection::default(),
            Destination::Directory(stele.clone()),
            false,
            &Observer::silent(),
        )
        .unwrap();
        Self {
            node,
            stele,
            epochs,
        }
    }

    fn repo(&self) -> String {
        format!("file://{}", self.stele.display())
    }

    fn run(&self, args: &[&str]) -> Output {
        let config = self.node.config_path();
        Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
            .arg("--config")
            .arg(&config)
            .args(args)
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN")
            .output()
            .unwrap()
    }

    fn inscription(&self) -> Vec<u8> {
        std::fs::read(self.stele.join("inscription.json")).unwrap()
    }
}

fn code(output: &Output) -> i32 {
    output.status.code().unwrap()
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{error}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn first_blob(stele: &Path) -> PathBuf {
    std::fs::read_dir(stele.join("blobs").join("sha256"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
}

#[test]
fn help_lists_the_three_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_stelae-publisher"))
        .arg("--help")
        .output()
        .unwrap();
    assert_eq!(code(&output), 0);
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["digest", "verify", "inspect"] {
        assert!(
            help.lines()
                .any(|line| line.trim_start().starts_with(command)),
            "{command}: {help}"
        );
    }
}

#[test]
fn verify_exits_zero_on_the_published_directory_and_three_on_an_altered_layer() {
    let fixture = Fixture::published();
    let repo = fixture.repo();

    let text = fixture.run(&["verify", "--repo", &repo]);
    assert_eq!(code(&text), 0, "{}", String::from_utf8_lossy(&text.stderr));
    let stdout = String::from_utf8(text.stdout).unwrap();
    assert!(stdout.contains("sequence: 1"), "{stdout}");

    let report = json(&fixture.run(&["verify", "--repo", &repo, "--format", "json"]));
    assert_eq!(report["sequence"], 1);
    assert_eq!(report["reproduced"], serde_json::Value::Null);

    let epochs = fixture.epochs.to_str().unwrap();
    let reproduced = fixture.run(&[
        "verify",
        "--repo",
        &repo,
        "--reproduce",
        "--state-epochs-file",
        epochs,
        "--format",
        "json",
    ]);
    assert_eq!(
        code(&reproduced),
        0,
        "{}",
        String::from_utf8_lossy(&reproduced.stderr)
    );
    let report = json(&reproduced);
    assert_eq!(report["reproduced"]["layers"], report["layers"]);

    let blob = first_blob(&fixture.stele);
    let mut bytes = std::fs::read(&blob).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&blob, bytes).unwrap();

    let altered = fixture.run(&["verify", "--repo", &repo, "--format", "json"]);
    assert_eq!(
        code(&altered),
        3,
        "{}",
        String::from_utf8_lossy(&altered.stderr)
    );
    assert!(altered.stdout.is_empty());
}

#[test]
fn a_reproduction_under_other_parameters_exits_three() {
    let fixture = Fixture::published();
    let tip_only = fixture.node.path().join("tip-only");
    std::fs::write(&tip_only, "").unwrap();

    let output = fixture.run(&[
        "verify",
        "--repo",
        &fixture.repo(),
        "--reproduce",
        "--state-epochs-file",
        tip_only.to_str().unwrap(),
    ]);
    assert_eq!(
        code(&output),
        3,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn inspect_and_digest_print_the_published_inscription() {
    let fixture = Fixture::published();
    let repo = fixture.repo();

    let text = fixture.run(&["inspect", "--repo", &repo]);
    assert_eq!(code(&text), 0, "{}", String::from_utf8_lossy(&text.stderr));
    let stdout = String::from_utf8(text.stdout).unwrap();
    assert!(stdout.contains("sequence: 1"), "{stdout}");

    let inspected = fixture.run(&["inspect", "--repo", &repo, "--format", "json"]);
    assert_eq!(code(&inspected), 0);
    assert_eq!(json(&inspected)["sequence"], 1);
    assert_eq!(inspected.stdout, fixture.inscription());

    let epochs = fixture.epochs.to_str().unwrap();
    let digested = fixture.run(&["digest", "--state-epochs-file", epochs, "--format", "json"]);
    assert_eq!(
        code(&digested),
        0,
        "{}",
        String::from_utf8_lossy(&digested.stderr)
    );
    json(&digested);
    assert_eq!(digested.stdout, fixture.inscription());
}

#[test]
fn an_unreadable_stele_exits_one_and_a_bad_invocation_two() {
    let fixture = Fixture::published();
    let missing = format!("file://{}", fixture.node.path().join("missing").display());
    assert_eq!(code(&fixture.run(&["verify", "--repo", &missing])), 1);
    assert_eq!(code(&fixture.run(&["inspect", "--repo", &missing])), 1);

    let repo = fixture.repo();
    assert_eq!(
        code(&fixture.run(&["verify", "--repo", &repo, "--epochs", "1"])),
        2
    );
    assert_eq!(
        code(&fixture.run(&["verify", "--repo", &repo, "--reproduce"])),
        2
    );
    assert_eq!(code(&fixture.run(&["digest"])), 2);
}
