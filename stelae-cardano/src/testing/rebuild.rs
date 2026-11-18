//! The old lineage release v0.6.0 published (`tests/data/rebuild-v0.6.0`),
//! laid out for the loopback registry with a blob behind every layer, so a
//! rebuild has something to write.

pub mod blocks;

use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use stelae::{manifest::DIFF_ID_ANNOTATION, Digest};

/// The fixture's lineage: `epoch-1` to `epoch-8`.
pub const UNTIL: u64 = 8;

/// Where the fixture's documents are.
pub fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/rebuild-v0.6.0")
}

/// The bytes that stand in for the blob of the layer `diff_id`. The fixture
/// keeps no layer, and a rebuild never reads one.
pub fn blob_of(diff_id: &str) -> Vec<u8> {
    format!("the blob of layer {diff_id}").into_bytes()
}

/// Lay the fixture under `root` as the loopback repository `name`: its
/// inscriptions, its tags, and each manifest naming its layers' stand-in
/// blobs, which are laid beside them.
pub fn lay_old_lineage(root: &Path, name: &str) {
    let (from, to) = (fixture(), root.join(name));
    let (tags, blobs) = (to.join("manifests/tags"), to.join("blobs/sha256"));
    std::fs::create_dir_all(&tags).unwrap();
    std::fs::create_dir_all(&blobs).unwrap();

    for entry in std::fs::read_dir(from.join("blobs/sha256")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), blobs.join(entry.file_name())).unwrap();
    }

    for entry in std::fs::read_dir(from.join("manifests/tags")).unwrap() {
        let entry = entry.unwrap();
        let mut manifest: Value =
            serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
        for layer in manifest["layers"].as_array_mut().unwrap() {
            let bytes = blob_of(layer["annotations"][DIFF_ID_ANNOTATION].as_str().unwrap());
            let digest = Digest::compute(&bytes);
            layer["digest"] = json!(digest.to_string());
            layer["size"] = json!(bytes.len());
            std::fs::write(blobs.join(digest.to_hex()), &bytes).unwrap();
        }
        std::fs::write(
            tags.join(entry.file_name()),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
}
