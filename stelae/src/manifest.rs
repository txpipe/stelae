//! A snapshot's manifest: the OCI image manifest it is published under,
//! whichever transport holds it.
//!
//! A registry serves this document and a directory can hold the same one, so
//! its types, its bytes and the reading of it belong to the protocol rather
//! than to a transport. The types mirror the OCI image manifest and descriptor
//! field for field; [`crate::oci`] converts to and from its client's types at
//! its own boundary and calls the functions here.

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
/// because it is load-bearing on the way back: it *is* the identity→blob map a
/// directory has to rebuild by decompressing everything. The golden freezes
/// all three.
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
    /// One per layer, in inscription order: `diff`, then `tip`.
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

/// Build the manifest for a snapshot whose layers are already written.
///
/// Returns it together with the canonical inscription bytes, so the config
/// descriptor and the blob that is pushed under it cannot be computed from two
/// different encodings of the same document.
///
/// Layers are listed in **inscription order**, `diff` then `tip`, matched by
/// `diffId`: the canonical document fixes the order, and the manifest follows
/// it rather than the order the sinks happened to finish in. Anything that does
/// not match both ways is a refusal.
///
/// Pure, so the shape of the artifact is frozen by a golden that needs no
/// network.
pub fn build_manifest(
    inscription: &Inscription,
    layers: &[WrittenLayer],
) -> Result<(Manifest, Vec<u8>), Error> {
    let config = inscription.canonicalize()?;

    let mut taken = vec![false; layers.len()];
    let mut descriptors = Vec::with_capacity(inscription.diff.len() + inscription.tip.len());

    for described in inscription.layers() {
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

    if let Some(orphan) = taken.iter().position(|used| !used) {
        let layer = &layers[orphan].descriptor;

        return Err(Error::ManifestMismatch(format!(
            "a {:?} layer ({}) was written but the inscription does not describe it; \
             a blob nothing attests would be published",
            layer.kind, layer.diff_id,
        )));
    }

    let manifest = Manifest {
        schema_version: 2,
        media_type: Some(IMAGE_MANIFEST_MEDIA_TYPE.to_owned()),
        artifact_type: Some(ARTIFACT_TYPE.to_owned()),
        config: Descriptor {
            media_type: INSCRIPTION_MEDIA_TYPE.to_owned(),
            digest: Digest::compute(&config).to_string(),
            size: config.len() as i64,
            ..Default::default()
        },
        layers: descriptors,
        subject: None,
        annotations: None,
    };

    Ok((manifest, config))
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
/// The ceiling is [`MANIFEST_SIZE_LIMIT`]. What it refuses is a snapshot with
/// too many layers: at roughly 350 bytes of descriptor and annotations apiece,
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

    Ok(())
}

/// Read the identity→blob map off a manifest, holding it against the
/// inscription.
///
/// The registry's answer to the question [`crate::dir::BLOB_INDEX_FILE`]
/// answers for a directory, and the reason a registry restore has always read
/// every blob once instead of twice.
///
/// Both correspondences are checked, and they are not the same check: the
/// `diffId` annotation is what the map is *built* from, and positional
/// correspondence with the inscription's layers, `diff` then `tip`, is what
/// proves the manifest describes this document's layers and not some other
/// snapshot's. A manifest that carried the right blobs in the wrong order
/// would pass the first and fail the second.
///
/// Pure, so the parsing half of the artifact is frozen by the same golden as
/// the building half.
pub fn read_manifest(manifest: &Manifest, inscription: &Inscription) -> Result<BlobIndex, Error> {
    check_envelope(manifest)?;

    let described = inscription.diff.len() + inscription.tip.len();

    if manifest.layers.len() != described {
        return Err(Error::ManifestMismatch(format!(
            "the manifest carries {} layer(s) and the inscription describes {described}",
            manifest.layers.len(),
        )));
    }

    let mut blobs = BlobIndex::default();

    for (position, (layer, described)) in
        manifest.layers.iter().zip(inscription.layers()).enumerate()
    {
        let annotation = layer
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(DIFF_ID_ANNOTATION))
            .ok_or_else(|| {
                Error::ManifestMismatch(format!(
                    "layer {position} carries no {DIFF_ID_ANNOTATION} annotation, \
                     so nothing says which layer it holds"
                ))
            })?;

        let diff_id = annotation.parse::<Digest>()?;

        if diff_id != described.diff_id {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is annotated {diff_id} and the inscription describes \
                 {} there",
                described.diff_id,
            )));
        }

        if layer.media_type != described.media_type {
            return Err(Error::ManifestMismatch(format!(
                "layer {position} is {:?} in the manifest and {:?} in the inscription",
                layer.media_type, described.media_type,
            )));
        }

        blobs.insert(diff_id, layer.digest.parse::<Digest>()?);
    }

    Ok(blobs)
}
