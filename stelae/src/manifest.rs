//! A snapshot's manifests: the OCI image manifests it is published under,
//! whichever transport holds them.
//!
//! A sequence is published under two (decision 0080). The diff manifest lists
//! the inscription's `diff`, the tip manifest lists its `tip`, and both carry
//! the same inscription as their config. A sequence whose `tip` is empty has
//! no tip manifest.
//!
//! A registry serves these documents and a directory can hold the same ones,
//! so their types, their bytes and the reading of them belong to the protocol
//! rather than to a transport. The types mirror the OCI image manifest and
//! descriptor field for field; [`crate::oci`] converts to and from its
//! client's types at its own boundary and calls the functions here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    inscription::{canonical_json, Inscription, LayerDescriptor},
    transport::{BlobIndex, WrittenLayer},
    Digest, Error, ARTIFACT_TYPE, INSCRIPTION_MEDIA_TYPE, MANIFEST_SIZE_LIMIT,
};

/// Media type of an OCI image manifest, the document a snapshot is.
pub const IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

/// Annotation naming a layer's profile-defined kind.
///
/// The three annotation keys below are the specification's: SPEC.md's "OCI
/// layout" section names them, reverse-DNS under
/// `stelae.store`, a domain TxPipe owns. They are transport metadata and sit
/// outside the inscription, so they are outside a snapshot's identity — but
/// only two of them are informational. [`DIFF_ID_ANNOTATION`] is normative,
/// because it is load-bearing on the way back: it *is* the identity→blob map,
/// in a registry and in a directory alike. The golden freezes all three.
pub const KIND_ANNOTATION: &str = "store.stelae.layer.kind";

/// Annotation carrying a layer's `diffId` — its identity, and the key of the
/// map a pull reads off the manifest.
pub const DIFF_ID_ANNOTATION: &str = "store.stelae.layer.diffId";

/// Annotation carrying the canonical JSON of a layer's profile-owned scope.
///
/// Informational: a human or a generic tool reading the manifest can see which
/// epoch or shard a blob covers without fetching the config blob.
pub const SCOPE_ANNOTATION: &str = "store.stelae.layer.scope";

/// An OCI image manifest.
///
/// Every field the OCI image specification defines, so a manifest read from
/// anywhere converts without loss; unknown fields are ignored, as the
/// specification requires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// Always `2`.
    pub schema_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// The inscription, as the config blob.
    pub config: Descriptor,
    /// One per layer of the one array this manifest lists, in its order.
    pub layers: Vec<Descriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<Descriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
}

/// An OCI content descriptor: a blob's media type, digest and size.
///
/// `digest` stays a string and `size` a signed integer, as the document spells
/// them: a malformed value is refused where it is read, not where it is parsed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: String,
    pub digest: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub urls: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,
}

/// Which of an inscription's two arrays a manifest lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Array {
    /// `diff`, listed by the manifest under the sequence's tag.
    Diff,
    /// `tip`, listed by the manifest under that tag plus [`crate::TIP_SUFFIX`].
    Tip,
}

impl Array {
    /// This array of `inscription`.
    pub fn of(self, inscription: &Inscription) -> &[LayerDescriptor] {
        match self {
            Array::Diff => &inscription.diff,
            Array::Tip => &inscription.tip,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Array::Diff => "diff",
            Array::Tip => "tip",
        }
    }
}

/// The manifests one sequence is published under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifests {
    /// Lists `diff`. Always written, so a sequence's tag exists exactly when
    /// its diff was published.
    pub diff: Manifest,
    /// Lists `tip`; `None` when the inscription's `tip` is empty.
    pub tip: Option<Manifest>,
}

/// Build the manifests for a snapshot whose layers are already written.
///
/// Returns them together with the canonical inscription bytes, so the config
/// descriptor both manifests carry and the blob pushed under it cannot be
/// computed from two different encodings of the same document.
///
/// Each manifest lists its array in **inscription order**, matched by
/// `diffId`: the canonical document fixes the order, and the manifests follow
/// it rather than the order the sinks happened to finish in. A layer the
/// inscription describes and nobody wrote, or one written and described by
/// neither array, is a refusal.
///
/// Pure, so the shape of the artifact is frozen by a golden that needs no
/// network.
pub fn build_manifests(
    inscription: &Inscription,
    layers: &[WrittenLayer],
) -> Result<(Manifests, Vec<u8>), Error> {
    let config = inscription.canonicalize()?;

    let mut taken = vec![false; layers.len()];
    let diff = claim(&inscription.diff, layers, &mut taken)?;
    let tip = claim(&inscription.tip, layers, &mut taken)?;

    if let Some(orphan) = taken.iter().position(|used| !used) {
        let layer = &layers[orphan].descriptor;

        return Err(Error::ManifestMismatch(format!(
            "a {:?} layer ({}) was written but the inscription does not describe it; \
             a blob nothing attests would be published",
            layer.kind, layer.diff_id,
        )));
    }

    let config_descriptor = Descriptor {
        media_type: INSCRIPTION_MEDIA_TYPE.to_owned(),
        digest: Digest::compute(&config).to_string(),
        size: config.len() as i64,
        ..Default::default()
    };

    let manifest = |layers: Vec<Descriptor>| Manifest {
        schema_version: 2,
        media_type: Some(IMAGE_MANIFEST_MEDIA_TYPE.to_owned()),
        artifact_type: Some(ARTIFACT_TYPE.to_owned()),
        config: config_descriptor.clone(),
        layers,
        subject: None,
        annotations: None,
    };

    let manifests = Manifests {
        diff: manifest(diff),
        tip: (!tip.is_empty()).then(|| manifest(tip)),
    };

    Ok((manifests, config))
}

/// The descriptors of one array, each matched to a written layer not yet
/// claimed by an earlier one.
fn claim(
    described: &[LayerDescriptor],
    layers: &[WrittenLayer],
    taken: &mut [bool],
) -> Result<Vec<Descriptor>, Error> {
    let mut descriptors = Vec::with_capacity(described.len());

    for described in described {
        // Matched by identity, and the first unclaimed one wins: two layers
        // with the same `diffId` are the same bytes, so which of them a
        // descriptor points at cannot be observed.
        let found = layers
            .iter()
            .enumerate()
            .find(|(index, layer)| !taken[*index] && layer.descriptor.diff_id == described.diff_id);

        let Some((index, layer)) = found else {
            return Err(Error::ManifestMismatch(format!(
                "the inscription describes a {:?} layer ({}) that was never written",
                described.kind, described.diff_id,
            )));
        };

        taken[index] = true;
        descriptors.push(layer_descriptor(described, layer)?);
    }

    Ok(descriptors)
}

fn layer_descriptor(
    described: &LayerDescriptor,
    written: &WrittenLayer,
) -> Result<Descriptor, Error> {
    let scope = String::from_utf8(canonical_json(&described.scope)?)
        .map_err(|e| Error::Canonicalization(e.to_string()))?;

    let annotations = BTreeMap::from([
        (KIND_ANNOTATION.to_owned(), described.kind.clone()),
        (DIFF_ID_ANNOTATION.to_owned(), described.diff_id.to_string()),
        (SCOPE_ANNOTATION.to_owned(), scope),
    ]);

    Ok(Descriptor {
        media_type: described.media_type.clone(),
        digest: written.digests.blob_digest.to_string(),
        size: written.digests.compressed_size as i64,
        annotations: Some(annotations),
        ..Default::default()
    })
}

/// The exact bytes of a manifest, canonicalized and held to the size ceiling.
///
/// RFC 8785 through the same canonicalizer the inscription uses, so this crate
/// has one answer to "what are the bytes of this JSON document" rather than two
/// that agree until they do not.
///
/// The ceiling is [`MANIFEST_SIZE_LIMIT`], held to each manifest of a sequence
/// on its own. What it refuses is a manifest listing too many layers: at
/// roughly 350 bytes of descriptor and annotations apiece,
/// a manifest reaches 4 MiB somewhere around twelve thousand of them — nearly
/// seven times a mainnet snapshot's ~1,816. The comparison is in layers because
/// layers are what the ceiling counts; Dolos ADR-004's ~600 is a count of
/// *epochs*, and a mainnet snapshot carries three layers per epoch plus sixteen
/// state shards. It is not a limit anything is expected to reach; it is the
/// limit that turns "the registry answered 413" into a refusal that names the
/// document and the number of layers in it.
///
/// `a_manifest_past_the_size_ceiling_is_refused` in `tests/manifest.rs`
/// measures those figures rather than asserting them; keep the two in step.
pub fn manifest_bytes(manifest: &Manifest) -> Result<Vec<u8>, Error> {
    let body = canonical_json(&serde_json::to_value(manifest)?)?;

    if body.len() > MANIFEST_SIZE_LIMIT {
        return Err(Error::ManifestTooLarge {
            size: body.len(),
            layers: manifest.layers.len(),
        });
    }

    Ok(body)
}

/// Check that a manifest is a Stelae snapshot's before anything inside it is
/// trusted.
pub(crate) fn check_envelope(manifest: &Manifest) -> Result<(), Error> {
    match manifest.artifact_type.as_deref() {
        Some(ARTIFACT_TYPE) => {}
        Some(other) => {
            return Err(Error::ManifestMismatch(format!(
                "artifactType is {other:?}, not {ARTIFACT_TYPE:?}"
            )))
        }
        // Fail closed. A registry that strips `artifactType` — the OCI 1.1
        // field this artifact is discovered by — has published something this
        // client cannot recognise as a snapshot, and reading it anyway would make
        // the discovery contract advisory.
        None => {
            return Err(Error::ManifestMismatch(format!(
                "no artifactType; a Stelae snapshot's manifest carries {ARTIFACT_TYPE:?}"
            )))
        }
    }

    if manifest.config.media_type != INSCRIPTION_MEDIA_TYPE {
        return Err(Error::ManifestMismatch(format!(
            "config blob is {:?}, not the inscription's {INSCRIPTION_MEDIA_TYPE:?}",
            manifest.config.media_type,
        )));
    }

    check_config_size(manifest.config.size)
}

/// Refuse a config blob whose claimed size is negative or past
/// [`MANIFEST_SIZE_LIMIT`], before a reader reserves memory for it.
///
/// The config is the inscription, which lists every layer once, so it stays
/// the same order of size as the manifest that carries it.
pub(crate) fn check_config_size(size: i64) -> Result<(), Error> {
    if !(0..=MANIFEST_SIZE_LIMIT as i64).contains(&size) {
        return Err(Error::ManifestMismatch(format!(
            "config blob claims {size} bytes, outside the 0..={MANIFEST_SIZE_LIMIT}-byte bound \
             for an inscription"
        )));
    }

    Ok(())
}

/// The inscription a manifest's config blob holds, held to the digest the
/// manifest addresses it by.
///
/// The bytes must be the canonical encoding, and the inscription's own digest
/// must be the config digest: the one place identity and transport are held
/// against each other, which stops a manifest from pointing at a document
/// nobody signed.
pub(crate) fn inscription_of(manifest: &Manifest, raw: &[u8]) -> Result<Inscription, Error> {
    let inscription = Inscription::parse(raw)?;

    if inscription.canonicalize()? != raw {
        return Err(Error::NonCanonicalInscription);
    }

    let config_digest = manifest.config.digest.parse::<Digest>()?;
    let identity = inscription.digest()?;

    if identity != config_digest {
        return Err(Error::DigestMismatch {
            subject: "inscription".to_owned(),
            expected: config_digest.to_string(),
            actual: identity.to_string(),
        });
    }

    Ok(inscription)
}

/// Read the identity→blob map off one manifest, holding it against the
/// array of the inscription it lists.
///
/// The one source of that map, for a registry and a directory alike, so no
/// reader has to decompress a blob to learn which layer it holds.
///
/// Three correspondences are checked, and they are not the same check. The
/// config names this inscription, so a tip manifest of another sequence is not
/// read as this one's. The `diffId` annotation is what the map is *built* from.
/// Positional correspondence with `array` proves the manifest describes this
/// document's layers and not some other snapshot's: a manifest that carried
/// the right blobs in the wrong order would pass the second and fail the
/// third.
///
/// Pure, so the parsing half of the artifact is frozen by the same golden as
/// the building half.
pub fn read_manifest(
    manifest: &Manifest,
    inscription: &Inscription,
    array: Array,
) -> Result<BlobIndex, Error> {
    check_envelope(manifest)?;

    let name = array.name();
    let identity = inscription.digest()?.to_string();

    if manifest.config.digest != identity {
        return Err(Error::ManifestMismatch(format!(
            "the {name} manifest's config is {}, and the inscription is {identity}",
            manifest.config.digest,
        )));
    }

    let described = array.of(inscription);

    if manifest.layers.len() != described.len() {
        return Err(Error::ManifestMismatch(format!(
            "the {name} manifest carries {} layer(s) and the inscription's {name} describes {}",
            manifest.layers.len(),
            described.len(),
        )));
    }

    let mut blobs = BlobIndex::default();

    for (position, (layer, described)) in manifest.layers.iter().zip(described).enumerate() {
        let annotation = layer
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(DIFF_ID_ANNOTATION))
            .ok_or_else(|| {
                Error::ManifestMismatch(format!(
                    "{name} layer {position} carries no {DIFF_ID_ANNOTATION} annotation, \
                     so nothing says which layer it holds"
                ))
            })?;

        let diff_id = annotation.parse::<Digest>()?;

        if diff_id != described.diff_id {
            return Err(Error::ManifestMismatch(format!(
                "{name} layer {position} is annotated {diff_id} and the inscription describes \
                 {} there",
                described.diff_id,
            )));
        }

        if layer.media_type != described.media_type {
            return Err(Error::ManifestMismatch(format!(
                "{name} layer {position} is {:?} in the manifest and {:?} in the inscription",
                layer.media_type, described.media_type,
            )));
        }

        blobs.insert(diff_id, layer.digest.parse::<Digest>()?);
    }

    Ok(blobs)
}
