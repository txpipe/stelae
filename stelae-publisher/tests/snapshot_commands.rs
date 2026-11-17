use std::{
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use stelae::{inscription::Inscription, progress::Observer};
use stelae_cardano::{
    publisher::{publish_once, Destination},
    snapshot::{read_position, Published},
    testing::{loopback::Loopback, open_repository, seal_blank_blocks, Node},
    Auth, Point, RestoreSource, Selection,
};

/// A node at its first boundary, published into a network root on disk: the
/// blocks snapshot in its `blocks` layout and the stele in its `dolos` one.
struct Fixture {
    node: Node,
    root: PathBuf,
}

impl Fixture {
    fn published() -> Self {
        let node = Node::new();
        let (genesis, _) = node.replay_to_the_first_boundary();
        let root = node.path().join("root");
        publish_once(
            &node.config,
            &genesis,
            Selection::default(),
            Destination::Directory {
                path: root.clone(),
                scratch_dir: None,
            },
            false,
            &Observer::silent(),
        )
        .unwrap();
        Self { node, root }
    }

    fn repo(&self) -> String {
        format!("file://{}", self.root.display())
    }

    fn dolos(&self) -> PathBuf {
        self.root.join("dolos")
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

    /// The canonical inscription the `dolos` layout holds at `latest`.
    fn inscription(&self) -> Vec<u8> {
        Published::open(
            &self.node.config,
            &RestoreSource::Dir(self.root.clone()),
            false,
            Auth::Anonymous,
        )
        .unwrap()
        .inspect(Point::Latest)
        .unwrap()
        .inscription
        .canonicalize()
        .unwrap()
    }

    fn rewrite_inscription(&self, edit: impl FnOnce(&mut Inscription)) {
        rewrite_config(&self.dolos(), |bytes| {
            let mut inscription = Inscription::parse(bytes).unwrap();
            edit(&mut inscription);
            inscription.canonicalize().unwrap()
        });
    }

    /// The largest blob of the `dolos` layout, which is a layer's.
    fn layer_blob(&self) -> PathBuf {
        std::fs::read_dir(self.dolos().join("blobs").join("sha256"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .max_by_key(|blob| std::fs::metadata(blob).unwrap().len())
            .unwrap()
    }
}

/// Replace the inscription every manifest of the layout at `layout` carries
/// with `rewrite`'s bytes, and the manifests and tags that name it, as a
/// writer of other documents would leave them.
fn rewrite_config(layout: &Path, rewrite: impl FnOnce(&[u8]) -> Vec<u8>) {
    use serde_json::{json, Value};

    let blob = |digest: &str| {
        let hex = digest.trim_start_matches("sha256:");
        layout.join("blobs").join("sha256").join(hex)
    };
    let put = |bytes: &[u8]| {
        let digest = stelae::Digest::compute(bytes);
        std::fs::write(blob(&digest.to_string()), bytes).unwrap();
        (digest.to_string(), bytes.len())
    };

    let index_path = layout.join("index.json");
    let mut index: Value = serde_json::from_slice(&std::fs::read(&index_path).unwrap()).unwrap();
    let mut rewrite = Some(rewrite);
    let mut config = None;
    for entry in index["manifests"].as_array_mut().unwrap() {
        let path = blob(entry["digest"].as_str().unwrap());
        let mut manifest: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let (digest, size) = config.get_or_insert_with(|| {
            let held = std::fs::read(blob(manifest["config"]["digest"].as_str().unwrap())).unwrap();
            put(&rewrite.take().unwrap()(&held))
        });
        manifest["config"]["digest"] = json!(digest);
        manifest["config"]["size"] = json!(size);
        let (digest, size) = put(&serde_json::to_vec(&manifest).unwrap());
        entry["digest"] = json!(digest);
        entry["size"] = json!(size);
    }
    std::fs::write(&index_path, serde_json::to_vec(&index).unwrap()).unwrap();
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
    assert!(
        stdout.contains("previous: none; this stele starts a lineage"),
        "{stdout}"
    );

    let report = json(&fixture.run(&["verify", "--repo", &repo, "--format", "json"]));
    assert_eq!(report["sequence"], 1);
    assert_eq!(report["previous"], serde_json::Value::Null);
    assert_eq!(report["reproduced"], serde_json::Value::Null);

    let reproduced = fixture.run(&["verify", "--repo", &repo, "--reproduce", "--format", "json"]);
    assert_eq!(
        code(&reproduced),
        0,
        "{}",
        String::from_utf8_lossy(&reproduced.stderr)
    );
    let report = json(&reproduced);
    assert_eq!(report["reproduced"]["layers"], report["layers"]);

    flip_a_byte(&fixture.layer_blob());

    let altered = fixture.run(&["verify", "--repo", &repo, "--format", "json"]);
    assert_eq!(
        code(&altered),
        3,
        "{}",
        String::from_utf8_lossy(&altered.stderr)
    );
    assert!(altered.stdout.is_empty());
}

/// A reproduction held to a link the published stele doesn't carry.
#[test]
fn a_reproduction_held_to_another_previous_exits_three() {
    let fixture = Fixture::published();
    let other = stelae::Digest::compute(b"another stele").to_string();

    let output = fixture.run(&[
        "verify",
        "--repo",
        &fixture.repo(),
        "--reproduce",
        "--previous",
        &other,
    ]);
    assert_eq!(code(&output), 3, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("--previous"),
        "{}",
        stderr_words(&output)
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
    assert!(stdout.contains("diff and"), "{stdout}");

    let inspected = fixture.run(&["inspect", "--repo", &repo, "--format", "json"]);
    assert_eq!(code(&inspected), 0);
    assert_eq!(json(&inspected)["sequence"], 1);
    assert_eq!(inspected.stdout, fixture.inscription());

    let digested = fixture.run(&["digest", "--repo", &repo, "--format", "json"]);
    assert_eq!(
        code(&digested),
        0,
        "{}",
        String::from_utf8_lossy(&digested.stderr)
    );
    json(&digested);
    assert_eq!(digested.stdout, fixture.inscription());
}

/// A digest is chained and named by what the root holds, or by the flags in
/// their place, and says which.
#[test]
fn a_digest_takes_its_link_and_blocks_from_the_root_or_the_flags() {
    let fixture = Fixture::published();
    let published = fixture.inscription();
    let position = Inscription::parse(&published).unwrap().position;
    let blocks = read_position(&position)
        .unwrap()
        .blocks
        .unwrap()
        .to_string();

    // What the root holds: a lineage's first stele, naming its blocks.
    let from_root = fixture.run(&["digest", "--repo", &fixture.repo()]);
    assert_eq!(code(&from_root), 0, "{}", stderr_words(&from_root));
    assert_eq!(from_root.stdout, published);
    let words = stderr_words(&from_root);
    assert!(words.contains("previous: none"), "{words}");
    assert!(words.contains(&format!("blocks: {blocks}")), "{words}");

    // The same inputs given as flags.
    let given = fixture.run(&["digest", "--blocks", &blocks]);
    assert_eq!(code(&given), 0, "{}", stderr_words(&given));
    assert_eq!(given.stdout, published);

    // Neither: a stele naming no blocks snapshot, which is another document.
    let neither = fixture.run(&["digest"]);
    assert_eq!(code(&neither), 0, "{}", stderr_words(&neither));
    assert_ne!(neither.stdout, published);
    assert!(
        stderr_words(&neither).contains("blocks: none named"),
        "{}",
        stderr_words(&neither)
    );

    // A link given in place of the root's.
    let other = stelae::Digest::compute(b"another stele").to_string();
    let linked = fixture.run(&["digest", "--repo", &fixture.repo(), "--previous", &other]);
    assert_eq!(code(&linked), 0, "{}", stderr_words(&linked));
    assert_ne!(linked.stdout, published);
    let reproduced = Inscription::parse(&linked.stdout).unwrap();
    let link = reproduced.previous.unwrap();
    assert_eq!(
        (link.sequence, link.inscription_digest.to_string()),
        (0, other.clone())
    );
    assert!(
        stderr_words(&linked).contains(&format!("previous: sequence 0 ({other})")),
        "{}",
        stderr_words(&linked)
    );
}

#[test]
fn an_unreadable_stele_exits_one_and_a_bad_invocation_two() {
    let fixture = Fixture::published();
    let missing = format!("file://{}", fixture.node.path().join("missing").display());
    assert_eq!(code(&fixture.run(&["verify", "--repo", &missing])), 1);
    assert_eq!(code(&fixture.run(&["inspect", "--repo", &missing])), 1);
    assert_eq!(code(&fixture.run(&["digest", "--repo", &missing])), 1);

    let repo = fixture.repo();
    let other = stelae::Digest::compute(b"another stele").to_string();
    assert_eq!(
        code(&fixture.run(&["verify", "--repo", &repo, "--epochs", "1"])),
        2
    );
    assert_eq!(
        code(&fixture.run(&["verify", "--repo", &repo, "--previous", &other])),
        2
    );
    assert_eq!(code(&fixture.run(&["digest", "--insecure"])), 2);
    assert_eq!(
        code(&fixture.run(&["digest", "--previous", "sha256:no"])),
        2
    );
}

#[test]
fn a_blob_whose_bytes_differ_from_its_name_exits_three() {
    let fixture = Fixture::published();
    // A skippable zstd frame decompresses to nothing, so the layer still reads
    // as its descriptor says and only the blob's name disagrees with it.
    let blob = fixture.layer_blob();
    let mut bytes = std::fs::read(&blob).unwrap();
    bytes.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 0]);
    std::fs::write(&blob, bytes).unwrap();

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 3, "{}", stderr_words(&output));
}

#[test]
fn a_non_canonical_inscription_exits_three() {
    let fixture = Fixture::published();
    rewrite_config(&fixture.dolos(), |bytes| {
        let document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        serde_json::to_vec_pretty(&document).unwrap()
    });

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
fn a_point_the_directory_does_not_hold_is_refused_before_a_layer_is_read() {
    let fixture = Fixture::published();
    flip_a_byte(&fixture.layer_blob());

    let output = fixture.run(&["verify", "--repo", &fixture.repo(), "--point", "epoch-99"]);
    assert_eq!(code(&output), 1, "{}", stderr_words(&output));
    assert!(
        stderr_words(&output).contains("epoch-99"),
        "{}",
        stderr_words(&output)
    );
}

#[test]
fn a_blob_that_cannot_be_read_exits_one() {
    use std::os::unix::fs::PermissionsExt as _;

    let fixture = Fixture::published();
    let blob = fixture.layer_blob();
    std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&blob).is_ok() {
        // Privileged: permissions do not stop this process.
        return;
    }

    let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
    assert_eq!(code(&output), 1, "{}", stderr_words(&output));
}

#[test]
fn a_command_off_the_registry_ignores_its_credentials() {
    let fixture = Fixture::published();
    let repo = fixture.repo();

    let commands: [&[&str]; 4] = [
        &["verify", "--repo", &repo],
        &["inspect", "--repo", &repo],
        &["digest", "--repo", &repo],
        &["digest"],
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
        let blob = fixture.layer_blob();
        let mut bytes = std::fs::read(&blob).unwrap();
        alter(&mut bytes);
        std::fs::write(&blob, bytes).unwrap();

        let output = fixture.run(&["verify", "--repo", &fixture.repo()]);
        assert_eq!(code(&output), 3, "{alteration}: {}", stderr_words(&output));
    }
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

    let reproduced = fixture.run(&["verify", "--repo", &repo, "--reproduce"]);
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

#[test]
fn a_directory_publish_spools_under_the_scratch_directory_it_is_given() {
    let fixture = Fixture::published();
    let spool = fixture.node.path().join("named-spool");
    let output = fixture.node.path().join("again");
    assert!(!spool.exists());

    let run = fixture.run(&[
        "publish",
        "--output-dir",
        output.to_str().unwrap(),
        "--scratch-dir",
        spool.to_str().unwrap(),
    ]);
    assert_eq!(code(&run), 0, "{}", stderr_words(&run));
    assert!(spool.is_dir());
    for layout in ["dolos", "blocks"] {
        assert!(output.join(layout).join("oci-layout").is_file(), "{layout}");
    }
}

/// Naming a spool keeps the publish away from the default under the stores.
#[test]
fn a_directory_publish_needs_no_room_on_the_store_volume_once_it_names_a_spool() {
    let fixture = Fixture::published();
    let scratch = fixture.node.config.storage.path.join("scratch");
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::write(&scratch, b"not a directory").unwrap();
    let spool = fixture.node.path().join("named-spool");
    let output = fixture.node.path().join("again");

    let run = fixture.run(&[
        "publish",
        "--output-dir",
        output.to_str().unwrap(),
        "--scratch-dir",
        spool.to_str().unwrap(),
    ]);
    assert_eq!(code(&run), 0, "{}", stderr_words(&run));
}

#[test]
fn a_digest_spools_under_the_scratch_dir_it_is_given() {
    let fixture = Fixture::published();
    let spool = fixture.node.path().join("named-spool");
    assert!(!spool.exists());

    let output = fixture.run(&[
        "digest",
        "--format",
        "json",
        "--scratch-dir",
        spool.to_str().unwrap(),
    ]);
    assert_eq!(
        code(&output),
        0,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(spool.is_dir());
}

#[test]
fn a_reproduction_spools_under_the_scratch_dir_it_is_given() {
    let fixture = Fixture::published();
    let spool = fixture.node.path().join("named-spool");
    assert!(!spool.exists());

    let output = fixture.run(&[
        "verify",
        "--repo",
        &fixture.repo(),
        "--reproduce",
        "--scratch-dir",
        spool.to_str().unwrap(),
    ]);
    assert_eq!(
        code(&output),
        0,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(spool.is_dir());
}

/// The flag reaches both a repository publish's spool and its staging: nothing
/// lands on the store volume.
#[test]
fn a_repository_publish_spools_under_the_scratch_directory_it_is_given() {
    let fixture = Fixture::published();
    let default = fixture.node.config.storage.path.join("scratch");
    std::fs::remove_dir_all(&default).unwrap();
    let spool = fixture.node.path().join("named-spool");
    let registry = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(registry.path()).unwrap();
    seed_first_blocks(&fixture, &loopback, "cardano/test");

    let run = fixture.run(&[
        "publish",
        "--repo",
        &loopback.repository("cardano/test"),
        "--insecure",
        "--scratch-dir",
        spool.to_str().unwrap(),
    ]);

    assert_eq!(code(&run), 0, "{}", stderr_words(&run));
    assert!(spool.is_dir(), "{}", stderr_words(&run));
    assert!(!default.exists(), "{}", stderr_words(&run));
    assert!(loopback.manifest("cardano/test/dolos", "epoch-1").is_some());
}

/// A repository publish names the blocks snapshot under the same network root,
/// and refuses without one, writing nothing.
#[test]
fn a_repository_publish_refuses_without_its_blocks_snapshot() {
    let fixture = Fixture::published();
    let registry = tempfile::tempdir().unwrap();
    let loopback = Loopback::serve(registry.path()).unwrap();
    let repo = loopback.repository("cardano/test");

    let refused = fixture.run(&["publish", "--repo", &repo, "--insecure"]);
    assert_eq!(code(&refused), 1, "{}", stderr_words(&refused));
    let words = stderr_words(&refused);
    assert!(
        words.contains("holds no blocks snapshot at epoch-1"),
        "{words}"
    );
    assert!(words.contains("cardano/test/blocks"), "{words}");
    assert!(loopback.manifests("cardano/test/dolos").is_empty());

    let blocks = seed_first_blocks(&fixture, &loopback, "cardano/test");
    let published = fixture.run(&["publish", "--repo", &repo, "--insecure"]);
    assert_eq!(code(&published), 0, "{}", stderr_words(&published));
    let inspected = fixture.run(&["inspect", "--repo", &repo, "--insecure", "--format", "json"]);
    let position = Inscription::parse(&inspected.stdout).unwrap().position;
    let named = read_position(&position).unwrap().blocks;
    assert_eq!(named, Some(blocks));
}

/// A blank blocks snapshot at sequence 1 under the network root `name`.
fn seed_first_blocks(fixture: &Fixture, loopback: &Loopback, name: &str) -> stelae::Digest {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let scratch = tempfile::tempdir().unwrap();
    let blocks = open_repository(
        &loopback.repository(&format!("{name}/blocks")),
        scratch.path(),
    );
    let genesis = stelae_cardano::config::genesis(&fixture.node.config).unwrap();
    let epoch = genesis.shelley.epoch_length.unwrap() as u64;
    seal_blank_blocks(
        &blocks,
        u64::from(genesis.network_magic()),
        1,
        (0, 0, epoch - 1),
    )
}
