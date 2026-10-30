use std::{
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use stelae::{inscription::Inscription, progress::Observer};
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
        self.command(args).output().unwrap()
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_stelae-publisher"));
        command
            .arg("--config")
            .arg(self.node.config_path())
            .args(args)
            .env_remove("DOLOS_STELAE_USER")
            .env_remove("DOLOS_STELAE_PASSWORD")
            .env_remove("DOLOS_STELAE_TOKEN");
        command
    }

    fn inscription(&self) -> Vec<u8> {
        std::fs::read(self.stele.join("inscription.json")).unwrap()
    }

    fn rewrite_inscription(&self, edit: impl FnOnce(&mut Inscription)) {
        let mut inscription = Inscription::parse(&self.inscription()).unwrap();
        edit(&mut inscription);
        std::fs::write(
            self.stele.join("inscription.json"),
            inscription.canonicalize().unwrap(),
        )
        .unwrap();
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

/// Stderr's words, rejoined across miette's wrapping.
fn stderr_words(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr)
        .split_whitespace()
        .filter(|word| *word != "│")
        .collect::<Vec<_>>()
        .join(" ")
}

fn flip_a_byte(blob: &Path) {
    let mut bytes = std::fs::read(blob).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(blob, bytes).unwrap();
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

    flip_a_byte(&first_blob(&fixture.stele));

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

#[test]
fn a_blob_whose_bytes_differ_from_its_name_exits_three() {
    let fixture = Fixture::published();
    // A skippable zstd frame decompresses to nothing, so the layer still reads
    // as its descriptor says and only the blob's name disagrees with it.
    let blob = first_blob(&fixture.stele);
    let mut bytes = std::fs::read(&blob).unwrap();
    bytes.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 0]);
    std::fs::write(&blob, bytes).unwrap();

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 3, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("the blob index names this layer's blob"),
        "{}",
        stderr_words(&output)
    );
}

#[test]
fn a_non_canonical_inscription_exits_three() {
    let fixture = Fixture::published();
    let document: serde_json::Value = serde_json::from_slice(&fixture.inscription()).unwrap();
    std::fs::write(
        fixture.stele.join("inscription.json"),
        serde_json::to_vec_pretty(&document).unwrap(),
    )
    .unwrap();

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 3, "{}", stderr_words(&output));
}

#[test]
fn a_directory_inscription_of_another_profile_is_refused() {
    let edits: [fn(&mut Inscription); 2] = [
        |inscription| inscription.profile.version += 1,
        |inscription| inscription.profile.name = "io.example.other".to_owned(),
    ];
    for edit in edits {
        let fixture = Fixture::published();
        fixture.rewrite_inscription(edit);
        let repo = fixture.repo();

        let verified = fixture.run(&["verify", "--repo", &repo]);
        assert_eq!(code(&verified), 3, "{}", stderr_words(&verified));
        let inspected = fixture.run(&["inspect", "--repo", &repo]);
        assert_eq!(code(&inspected), 1, "{}", stderr_words(&inspected));
    }
}

#[test]
fn a_directory_without_its_blob_index() {
    let fixture = Fixture::published();
    let repo = fixture.repo();
    std::fs::remove_file(fixture.stele.join("blobs.json")).unwrap();

    let verified = fixture.run(&["verify", "--repo", &repo]);
    assert_eq!(code(&verified), 0, "{}", stderr_words(&verified));

    // Inspect reads no blob for sizes it cannot state without reading all.
    flip_a_byte(&first_blob(&fixture.stele));
    let inspected = fixture.run(&["inspect", "--repo", &repo]);
    assert_eq!(code(&inspected), 0, "{}", stderr_words(&inspected));
    let stdout = String::from_utf8(inspected.stdout).unwrap();
    let totals = stdout
        .lines()
        .find(|line| line.starts_with("totals:"))
        .unwrap_or_else(|| panic!("{stdout}"));
    assert!(totals.ends_with(", ? compressed bytes"), "{totals}");

    // The scan that rebuilds the index is a verification of every blob.
    let verified = fixture.run(&["verify", "--repo", &repo]);
    assert_eq!(code(&verified), 3, "{}", stderr_words(&verified));
}

#[test]
fn a_point_the_directory_does_not_hold_is_refused_before_a_layer_is_read() {
    let fixture = Fixture::published();
    flip_a_byte(&first_blob(&fixture.stele));

    let output = fixture.run(&["verify", "--repo", &fixture.repo(), "--point", "epoch-99"]);
    assert_eq!(code(&output), 1, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("holds one stele"),
        "{}",
        stderr_words(&output)
    );
}

#[test]
fn a_blob_that_cannot_be_read_exits_one() {
    use std::os::unix::fs::PermissionsExt as _;

    let fixture = Fixture::published();
    let blob = first_blob(&fixture.stele);
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&blob).is_ok() {
        // Privileged: permissions do not stop this process.
        return;
    }

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 1, "{}", stderr_words(&output));
}

#[test]
fn digest_refuses_a_predecessor_it_cannot_chain_onto() {
    let fixture = Fixture::published();
    let epochs = fixture.epochs.to_str().unwrap();
    let digest = |chain_from: &Path| {
        fixture.run(&[
            "digest",
            "--state-epochs-file",
            epochs,
            "--chain-from",
            chain_from.to_str().unwrap(),
        ])
    };

    let missing = digest(&fixture.node.path().join("missing.json"));
    assert_eq!(code(&missing), 1, "{}", stderr_words(&missing));
    assert!(missing.stdout.is_empty());

    // The published stele is at this plan's own sequence, not its predecessor's.
    let itself = digest(&fixture.stele.join("inscription.json"));
    assert_eq!(code(&itself), 1, "{}", stderr_words(&itself));
    assert!(itself.stdout.is_empty());
    assert!(
        stderr_words(&itself).contains("at or behind the repository's latest"),
        "{}",
        stderr_words(&itself)
    );

    let pretty = fixture.node.path().join("pretty.json");
    let document: serde_json::Value = serde_json::from_slice(&fixture.inscription()).unwrap();
    std::fs::write(&pretty, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
    let re_encoded = digest(&pretty);
    assert_eq!(code(&re_encoded), 1, "{}", stderr_words(&re_encoded));
    assert!(
        stderr_words(&re_encoded).contains("not in canonical form"),
        "{}",
        stderr_words(&re_encoded)
    );
}

#[test]
fn a_command_off_the_registry_ignores_its_credentials() {
    let fixture = Fixture::published();
    let repo = fixture.repo();
    let epochs = fixture.epochs.to_str().unwrap();

    let commands: [&[&str]; 3] = [
        &["verify", "--repo", &repo],
        &["inspect", "--repo", &repo],
        &["digest", "--state-epochs-file", epochs],
    ];
    for args in commands {
        let output = fixture
            .command(args)
            .env("DOLOS_STELAE_PASSWORD", "secret")
            .output()
            .unwrap();
        assert_eq!(code(&output), 0, "{}: {}", args[0], stderr_words(&output));
    }
}

/// Bytes the directory holds under a blob's name that do not decompress are
/// the stele's, however the decoder gives up on them.
#[test]
fn a_blob_that_does_not_decompress_exits_three() {
    type Alter = fn(&mut Vec<u8>);
    let alterations: [(&str, Alter); 4] = [
        ("truncated", |bytes| bytes.truncate(bytes.len() / 2)),
        ("emptied", Vec::clear),
        ("first byte flipped", |bytes| bytes[0] ^= 0xff),
        ("replaced", |bytes| *bytes = b"not a zstd frame".to_vec()),
    ];
    for (alteration, alter) in alterations {
        let fixture = Fixture::published();
        let blob = first_blob(&fixture.stele);
        let mut bytes = std::fs::read(&blob).unwrap();
        alter(&mut bytes);
        std::fs::write(&blob, bytes).unwrap();

        let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
        assert_eq!(code(&output), 3, "{alteration}: {}", stderr_words(&output));
        assert!(
            stderr_words(&output).contains("does not decompress"),
            "{alteration}: {}",
            stderr_words(&output)
        );
    }

    // Under their own name, so only the decoder has anything to say about them.
    let fixture = Fixture::published();
    let blob = first_blob(&fixture.stele);
    let bytes = b"not a zstd frame";
    let renamed = stelae::Digest::compute(bytes).to_hex();
    std::fs::write(blob.with_file_name(&renamed), bytes).unwrap();
    std::fs::remove_file(&blob).unwrap();
    let sidecar = fixture.stele.join(stelae::dir::BLOB_INDEX_FILE);
    let index = std::fs::read_to_string(&sidecar).unwrap();
    let named = blob.file_name().unwrap().to_str().unwrap();
    assert!(index.contains(named));
    std::fs::write(&sidecar, index.replace(named, &renamed)).unwrap();

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 3, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("does not decompress"),
        "{}",
        stderr_words(&output)
    );
}

/// Plain verify never reads the position; a reproduction does, and refuses
/// one that names its network wrongly before walking the stores.
#[test]
fn a_malformed_position_is_refused_by_a_reproduction() {
    let fixture = Fixture::published();
    fixture.rewrite_inscription(|inscription| {
        inscription.position["network"]["name"] = "not-this-network".into();
    });
    let repo = fixture.repo();

    let plain = fixture.run(&["verify", "--repo", &repo]);
    assert_eq!(code(&plain), 0, "{}", stderr_words(&plain));

    let reproduced = fixture.run(&[
        "verify",
        "--repo",
        &repo,
        "--reproduce",
        "--state-epochs-file",
        fixture.epochs.to_str().unwrap(),
    ]);
    assert_eq!(code(&reproduced), 3, "{}", stderr_words(&reproduced));
    assert!(
        stderr_words(&reproduced).contains("position"),
        "{}",
        stderr_words(&reproduced)
    );
}

#[test]
fn a_json_report_stdout_will_not_take_exits_one() {
    let fixture = Fixture::published();
    let mut child = fixture
        .command(&["verify", "--repo", &fixture.repo(), "--format", "json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Closed before the stores open, so the report's write meets a broken pipe.
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();
    assert_eq!(code(&output), 1, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("writing to stdout"),
        "{}",
        stderr_words(&output)
    );
}
