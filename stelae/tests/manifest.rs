//! A snapshot's manifests, frozen without a network.
//!
//! These tests need **no transport and no compressor**, and run without the
//! `oci` feature: they build a sequence's diff and tip manifests from fixed
//! digests and compare the exact bytes, so a change to their shape is a
//! deliberate re-pin in the same commit and never a silent drift. A registry
//! and a directory hold these same documents, so they are frozen once, here.

use std::collections::BTreeMap;

use serde_json::json;

use stelae::{
    inscription::LayerDescriptor,
    manifest::{
        build_manifests, manifest_bytes, read_manifest, Array, Manifest, Manifests,
        DIFF_ID_ANNOTATION, KIND_ANNOTATION, SCOPE_ANNOTATION,
    },
    Compression, Digest, Error, Inscription, LayerDigests, Link, Profile, WrittenLayer,
};

/// The envelope earlier releases wrote. Refused, with no alias.
const RETIRED_ENVELOPE: &str = "application/vnd.stelae.stele.v1";

const PROFILE_NAME: &str = "dev.example.toy";
const NOTES_MEDIA_TYPE: &str = "application/vnd.example.stelae.notes.v1+zstd";
const INDEX_MEDIA_TYPE: &str = "application/vnd.example.stelae.index.v1+zstd";
const COMPRESSION_LEVEL: i32 = 9;

/// The toy profile the transport suites use, so the golden here is the one
/// they published before the manifest left the registry transport.
struct ToyProfile;

impl Profile for ToyProfile {
    fn name(&self) -> &str {
        PROFILE_NAME
    }

    fn version(&self) -> u64 {
        1
    }

    fn kinds(&self) -> &[&str] {
        &["notes", "index"]
    }

    fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
        match kind {
            "notes" => Ok(NOTES_MEDIA_TYPE.to_owned()),
            "index" => Ok(INDEX_MEDIA_TYPE.to_owned()),
            other => Err(Error::UnknownLayerKind {
                profile: PROFILE_NAME.to_owned(),
                kind: other.to_owned(),
            }),
        }
    }

    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
        Ok(format!("chapter-{sequence}"))
    }
}

fn digest_of(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

/// An inscription and the layers a publisher would have written for it, both
/// entirely synthetic.
///
/// Nothing here is compressed. That is deliberate: a manifest carries the
/// *compressed* digest and size of every layer, and zstd's output moves between
/// library versions — which is the whole reason identity is anchored on
/// uncompressed bytes. A golden over real blobs would pin the compressor, fail
/// on an unrelated upgrade, and teach whoever hit it that the golden is noise.
fn fixture() -> (Inscription, Vec<WrittenLayer>) {
    let mut inscription = Inscription::new(
        &ToyProfile,
        3,
        json!({"chapter": 3, "shelf": "east"}),
        json!({"noteWidth": 40}),
        Compression {
            algo: "zstd".to_owned(),
            level: COMPRESSION_LEVEL as i64,
        },
    );

    inscription.previous = Some(Link {
        sequence: 2,
        inscription_digest: digest_of(0x22),
    });

    inscription.diff = vec![LayerDescriptor {
        kind: "notes".to_owned(),
        media_type: NOTES_MEDIA_TYPE.to_owned(),
        diff_id: digest_of(0xaa),
        records: 4,
        uncompressed_size: 155,
        scope: json!({"chapter": 3}),
    }];

    inscription.tip = vec![LayerDescriptor {
        kind: "index".to_owned(),
        media_type: INDEX_MEDIA_TYPE.to_owned(),
        diff_id: digest_of(0xbb),
        records: 4,
        uncompressed_size: 77,
        scope: json!({"chapter": 3, "sortedBy": "title"}),
    }];

    // The tip's layer finished first, which is not the order the manifest
    // lists them in.
    let layers = vec![
        written(&inscription.tip[0], digest_of(0xb1), 60, 77),
        written(&inscription.diff[0], digest_of(0xa1), 91, 155),
    ];

    (inscription, layers)
}

fn written(
    descriptor: &LayerDescriptor,
    blob_digest: Digest,
    compressed_size: u64,
    uncompressed_size: u64,
) -> WrittenLayer {
    WrittenLayer {
        descriptor: descriptor.clone(),
        digests: LayerDigests {
            diff_id: descriptor.diff_id,
            blob_digest,
            uncompressed_size,
            compressed_size,
        },
    }
}

/// The fixture's manifests, with the tip the fixture always has.
fn manifests() -> (Inscription, Manifests, Manifest) {
    let (inscription, layers) = fixture();
    let (manifests, _) = build_manifests(&inscription, &layers).unwrap();
    let tip = manifests.tip.clone().expect("the fixture lists a tip");

    (inscription, manifests, tip)
}

/// Done criterion 3: the manifests' shape is a re-pin, never a drift.
///
/// Every byte below is determined by the specification — the artifact type, the
/// config media type, the profile's own layer media types, the annotation keys
/// and the canonical JSON encoding. If any of them moves, these strings move
/// with it, in the same commit.
#[test]
fn the_manifest_shape_is_frozen() {
    let (inscription, layers) = fixture();

    let (manifests, config) = build_manifests(&inscription, &layers).unwrap();
    let tip = manifests.tip.as_ref().expect("the fixture lists a tip");

    // The config blob is the canonical inscription, and both manifests name it
    // by its own digest — the one place identity and transport meet, and one
    // blob under two manifests.
    assert_eq!(config, inscription.canonicalize().unwrap());
    assert_eq!(
        manifests.diff.config.digest,
        inscription.digest().unwrap().to_string()
    );
    assert_eq!(manifests.diff.config.size as usize, config.len());
    assert_eq!(tip.config, manifests.diff.config);

    assert_eq!(
        String::from_utf8(manifest_bytes(&manifests.diff).unwrap()).unwrap(),
        concat!(
            r#"{"artifactType":"application/vnd.stelae.snapshot.v1","#,
            r#""config":{"digest":"sha256:486ad3376c0285ad01f99f147983050d8b664c9b76a2419d012f624cdae4d82e","#,
            r#""mediaType":"application/vnd.stelae.inscription.v1+json","size":771},"#,
            r#""layers":[{"annotations":{"#,
            r#""store.stelae.layer.diffId":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","#,
            r#""store.stelae.layer.kind":"notes","#,
            r#""store.stelae.layer.scope":"{\"chapter\":3}"},"#,
            r#""digest":"sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1","#,
            r#""mediaType":"application/vnd.example.stelae.notes.v1+zstd","size":91}],"#,
            r#""mediaType":"application/vnd.oci.image.manifest.v1+json","schemaVersion":2}"#,
        ),
        "the diff manifest's shape drifted"
    );

    assert_eq!(
        String::from_utf8(manifest_bytes(tip).unwrap()).unwrap(),
        concat!(
            r#"{"artifactType":"application/vnd.stelae.snapshot.v1","#,
            r#""config":{"digest":"sha256:486ad3376c0285ad01f99f147983050d8b664c9b76a2419d012f624cdae4d82e","#,
            r#""mediaType":"application/vnd.stelae.inscription.v1+json","size":771},"#,
            r#""layers":[{"annotations":{"#,
            r#""store.stelae.layer.diffId":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","#,
            r#""store.stelae.layer.kind":"index","#,
            r#""store.stelae.layer.scope":"{\"chapter\":3,\"sortedBy\":\"title\"}"},"#,
            r#""digest":"sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1","#,
            r#""mediaType":"application/vnd.example.stelae.index.v1+zstd","size":60}],"#,
            r#""mediaType":"application/vnd.oci.image.manifest.v1+json","schemaVersion":2}"#,
        ),
        "the tip manifest's shape drifted"
    );
}

/// A sequence whose tip is empty is published under one manifest.
#[test]
fn an_empty_tip_has_no_manifest() {
    let (mut inscription, layers) = fixture();
    inscription.tip.clear();

    let (manifests, _) = build_manifests(&inscription, &layers[1..]).unwrap();

    assert_eq!(manifests.tip, None);
    assert_eq!(manifests.diff.layers.len(), 1);
}

/// The map a pull reads off the manifests is the map a directory rebuilds by
/// decompressing everything, and this is where the two are held to be the same
/// thing. Each manifest yields its own array's half.
#[test]
fn a_manifest_yields_the_identity_to_blob_map() {
    let (inscription, manifests, tip) = manifests();

    let diff = read_manifest(&manifests.diff, &inscription, Array::Diff).unwrap();
    let tip = read_manifest(&tip, &inscription, Array::Tip).unwrap();

    assert_eq!(diff.len(), 1);
    assert_eq!(diff.blob_for(&digest_of(0xaa)), Some(digest_of(0xa1)));
    assert_eq!(tip.len(), 1);
    assert_eq!(tip.blob_for(&digest_of(0xbb)), Some(digest_of(0xb1)));

    let mut blobs = diff;
    blobs.extend(tip);

    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs.blob_for(&digest_of(0xbb)), Some(digest_of(0xb1)));
    assert_eq!(blobs.blob_for(&digest_of(0xcc)), None);
}

/// A manifest read back from its own bytes is the manifest that was built, and
/// fields the specification may add are ignored rather than refused, so a
/// transport that parses the document itself reads what a registry serves.
#[test]
fn a_manifest_reads_back_from_its_bytes() {
    let (inscription, manifests, _) = manifests();
    let manifest = manifests.diff;
    let body = manifest_bytes(&manifest).unwrap();

    let read: Manifest = serde_json::from_slice(&body).unwrap();

    assert_eq!(read, manifest);
    assert_eq!(manifest_bytes(&read).unwrap(), body);

    let mut extended: serde_json::Value = serde_json::from_slice(&body).unwrap();
    extended["futureField"] = json!(true);
    extended["config"]["futureField"] = json!(1);
    extended["layers"][0]["futureField"] = json!("x");

    let read: Manifest = serde_json::from_value(extended).unwrap();

    assert_eq!(read, manifest);
    assert!(read_manifest(&read, &inscription, Array::Diff).is_ok());

    // Every optional field the types carry, so none is dropped on the way in.
    let mut full = manifest.clone();
    full.annotations = Some(BTreeMap::from([("m".to_owned(), "n".to_owned())]));
    full.config.urls = Some(vec!["https://mirror.invalid/blob".to_owned()]);
    full.config.annotations = Some(BTreeMap::from([("k".to_owned(), "v".to_owned())]));
    full.config.artifact_type = Some("application/vnd.example.thing.v1".to_owned());
    full.subject = Some(full.config.clone());

    let read: Manifest = serde_json::from_slice(&manifest_bytes(&full).unwrap()).unwrap();

    assert_eq!(read, full);
}

/// The annotations Dolos ADR-004 asks for, checked as content rather than as
/// bytes, so a reader of this file can see what a generic OCI tool would find.
#[test]
fn every_layer_is_annotated_with_its_kind_diff_id_and_scope() {
    let (inscription, manifests, tip) = manifests();

    let listed = manifests.diff.layers.iter().chain(&tip.layers);

    for (oci, described) in listed.zip(inscription.layers()) {
        let annotations = oci.annotations.as_ref().unwrap();

        assert_eq!(annotations[KIND_ANNOTATION], described.kind);
        assert_eq!(
            annotations[DIFF_ID_ANNOTATION],
            described.diff_id.to_string()
        );
        assert_eq!(
            annotations[SCOPE_ANNOTATION],
            serde_json::to_string(&described.scope).unwrap()
        );
    }
}

/// A disagreement between the two documents is a refusal, never a preference.
#[test]
fn a_manifest_that_disagrees_with_the_inscription_is_refused() {
    let (inscription, layers) = fixture();

    // A layer the inscription describes and nobody wrote: here the diff's.
    let err = build_manifests(&inscription, &layers[..1]).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("never written")),
        "{err:?}"
    );

    // A layer that was written and the inscription does not describe: a blob
    // nothing attests.
    let mut orphaned = layers.clone();
    orphaned.push(written(&layers[0].descriptor, digest_of(0xc1), 10, 20));
    let err = build_manifests(&inscription, &orphaned).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("does not describe")),
        "{err:?}"
    );

    let (_, manifests, tip) = manifests();
    let manifest = manifests.diff;

    // Fewer layers than the document describes.
    let mut short = manifest.clone();
    short.layers.pop();
    let err = read_manifest(&short, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("layer(s)")),
        "{err:?}"
    );

    // Each manifest is held to its own array: the tip manifest read as the
    // diff, and the other way round.
    let err = read_manifest(&tip, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("diff layer 0 is annotated")),
        "{err:?}"
    );
    let err = read_manifest(&manifest, &inscription, Array::Tip).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("tip layer 0 is annotated")),
        "{err:?}"
    );

    // The right blobs in the wrong order. The `diffId` annotations still map
    // every layer to a real blob, so only the positional check catches this.
    let mut both = inscription.clone();
    both.diff.append(&mut both.tip);
    let (two, _) = build_manifests(&both, &layers).unwrap();
    let mut reordered = two.diff;
    reordered.layers.reverse();
    let err = read_manifest(&reordered, &both, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("annotated")),
        "{err:?}"
    );

    // A layer with no annotation says nothing about which layer it holds.
    let mut unannotated = manifest.clone();
    unannotated.layers[0].annotations = None;
    let err = read_manifest(&unannotated, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains(DIFF_ID_ANNOTATION)),
        "{err:?}"
    );

    // A media type the inscription does not claim.
    let mut mistyped = manifest;
    mistyped.layers[0].media_type = INDEX_MEDIA_TYPE.to_owned();
    let err = read_manifest(&mistyped, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("in the manifest")),
        "{err:?}"
    );
}

/// A manifest whose config is another inscription is not this sequence's, even
/// when every layer it lists matches: a tip manifest left over from another
/// publish at the same tag is refused rather than read as this one's tip.
#[test]
fn a_manifest_of_another_inscription_is_refused() {
    let (inscription, _, tip) = manifests();

    let mut other = inscription.clone();
    other.position = json!({"chapter": 3, "shelf": "west"});

    let err = read_manifest(&tip, &other, Array::Tip).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m)
            if m.contains("the tip manifest's config is")
                && m.contains(&other.digest().unwrap().to_string())),
        "{err:?}"
    );
}

/// The envelope is checked before anything inside it is trusted, and a missing
/// `artifactType` fails closed.
#[test]
fn a_manifest_that_is_not_a_snapshots_own_is_refused() {
    let (inscription, manifests, _) = manifests();
    let manifest = manifests.diff;

    let mut stripped = manifest.clone();
    stripped.artifact_type = None;
    let err = read_manifest(&stripped, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("no artifactType")),
        "{err:?}"
    );

    let mut foreign = manifest.clone();
    foreign.artifact_type = Some("application/vnd.acme.thing.v1".to_owned());
    let err = read_manifest(&foreign, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("artifactType is")),
        "{err:?}"
    );

    // The envelope earlier releases wrote is foreign now: no alias reads it.
    let mut retired = manifest.clone();
    retired.artifact_type = Some(RETIRED_ENVELOPE.to_owned());
    let err = read_manifest(&retired, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m)
            if m.contains("artifactType is") && m.contains(RETIRED_ENVELOPE)),
        "{err:?}"
    );

    let mut wrong_config = manifest;
    wrong_config.config.media_type = "application/vnd.oci.image.config.v1+json".to_owned();
    let err = read_manifest(&wrong_config, &inscription, Array::Diff).unwrap_err();
    assert!(
        matches!(&err, Error::ManifestMismatch(m) if m.contains("config blob is")),
        "{err:?}"
    );
}

/// What the 4 MiB ceiling refuses, stated as a test rather than as a comment.
///
/// A descriptor and its three annotations run to roughly 350 bytes, so a
/// manifest reaches the ceiling somewhere around twelve thousand layers —
/// nearly seven times a mainnet snapshot's ~1,816 (Dolos ADR-004's ~600 epochs
/// × three per-epoch kinds, plus sixteen state shards). Nothing is expected to
/// meet it. The point is that when something does, the refusal names the
/// document and the layer count instead of arriving as a registry's `413`.
#[test]
fn a_manifest_past_the_size_ceiling_is_refused() {
    /// Layers of a mainnet snapshot, by Dolos ADR-004's own sizing.
    const MAINNET: usize = 600 * 3 + 16;
    /// Comfortably past the ceiling: nothing here depends on where exactly it
    /// falls, only that it is far above anything a profile would publish.
    const TOO_MANY: usize = 16_000;

    let (template, written_template) = {
        let (inscription, layers) = fixture();
        (inscription.diff[0].clone(), layers[1].clone())
    };

    let build = |count: usize| {
        let (mut inscription, _) = fixture();
        inscription.diff.clear();
        inscription.tip.clear();

        let mut layers = Vec::with_capacity(count);

        for index in 0..count as u32 {
            let mut described = template.clone();

            // Distinct identities, so nothing collapses into one descriptor.
            let mut bytes = [0u8; 32];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            described.diff_id = Digest::from_bytes(bytes);
            described.scope = json!({"chapter": index});

            let mut layer = written_template.clone();
            layer.descriptor = described.clone();
            layer.digests.diff_id = described.diff_id;

            inscription.diff.push(described);
            layers.push(layer);
        }

        build_manifests(&inscription, &layers).unwrap().0.diff
    };

    let err = manifest_bytes(&build(TOO_MANY)).unwrap_err();

    assert!(
        matches!(err, Error::ManifestTooLarge { layers, .. } if layers == TOO_MANY),
        "{err:?}"
    );

    // A mainnet-sized snapshot passes it with room to spare, which is the claim
    // Dolos ADR-004 sized the format against.
    let body = manifest_bytes(&build(MAINNET)).unwrap();

    println!(
        "manifest: {} bytes for {MAINNET} layers ({} bytes per layer), \
         ceiling {} bytes",
        body.len(),
        body.len() / MAINNET,
        stelae::MANIFEST_SIZE_LIMIT,
    );

    assert!(
        body.len() < stelae::MANIFEST_SIZE_LIMIT / 2,
        "a mainnet-sized manifest is {} bytes",
        body.len(),
    );
}

/// The annotation map is a `BTreeMap`, so the golden's canonical JSON is not
/// hostage to insertion order. Cheap to state, and the kind of thing that only
/// breaks in the diff of an unrelated change.
#[test]
fn annotation_keys_are_ordered() {
    let (_, manifests, _) = manifests();
    let manifest = manifests.diff;

    let annotations: &BTreeMap<String, String> = manifest.layers[0].annotations.as_ref().unwrap();
    let keys: Vec<&str> = annotations.keys().map(String::as_str).collect();

    assert_eq!(
        keys,
        vec![DIFF_ID_ANNOTATION, KIND_ANNOTATION, SCOPE_ANNOTATION]
    );
}
