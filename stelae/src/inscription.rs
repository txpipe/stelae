//! The inscription: a snapshot's canonical, signable document.
//!
//! The inscription is the OCI config blob of a snapshot and the anchor of the
//! whole protocol. Its sha256, taken over its RFC 8785 canonical JSON encoding,
//! is the snapshot's identity: what independent publishers reproduce, what
//! signatures cover, and what the next inscription's `previous` names, so the
//! newest signed inscription transitively attests every earlier one.
//!
//! An inscription lists only its own layers, in two arrays. `diff` holds the
//! layers every later snapshot inherits; `tip` holds the layers that belong to
//! this snapshot alone. Which array a layer goes to is the profile's decision.
//!
//! Three of its fields — `position`, `parameters` and each layer's `scope` —
//! are the profile's, and this module never looks inside them. They are held as
//! [`serde_json::Value`], canonicalized like every other key, and hashed. That
//! is what lets determinism hold without the protocol knowing a profile's
//! vocabulary.
//!
//! ## Two rules that make the digest reproducible
//!
//! **Every number is an integer within ±(2^53 − 1).** RFC 8785 serializes
//! numbers per ECMAScript, so past that magnitude a `u64` renders as a rounded
//! double and two implementations diverge *silently*. The protocol refuses the
//! value instead ([`check_safe_numbers`]). Nothing in the schema needs the
//! range: an `uncompressedSize` for a mainnet epoch is around 4×10^10.
//!
//! **An invalid inscription has no digest.** [`Inscription::canonicalize`]
//! validates first, so it is not possible to obtain canonical bytes — and
//! therefore an identity — for a document that violates the schema, the naming
//! rules, the link invariant or the one-layer-per-scope rule.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    profile::{checked_layer_media_type, validate_profile_name, MediaType, Profile},
    Digest, Error, MAX_SAFE_INTEGER, SCHEMA_VERSION,
};

/// The profile a snapshot belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProfileRef {
    /// Reverse-DNS, vendor-owned, e.g. `io.txpipe.dolos.cardano`.
    pub name: String,
    /// Profile major version. A client refuses a value above the one it
    /// implements.
    pub version: u64,
}

/// Compression parameters. Transport policy, pinned so that blobs dedupe across
/// publishers in practice; identity never depends on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Compression {
    pub algo: String,
    pub level: i64,
}

/// The inscription a snapshot follows in its lineage.
///
/// One link and not a list: the inscription it names carries its own link, so
/// the newest inscription attests the whole chain back to the lineage's first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Link {
    pub sequence: u64,
    pub inscription_digest: Digest,
}

/// One layer of a snapshot, as the inscription describes it.
///
/// Note what is absent: the compressed digest and the compressed size. Those
/// are transport facts and live in the OCI manifest. The inscription carries
/// only what is identity — the uncompressed digest — so it stays reproducible
/// by a party that compressed differently, or not at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LayerDescriptor {
    /// Profile-defined layer kind.
    pub kind: String,
    /// Profile-owned payload media type.
    pub media_type: String,
    /// sha256 over the uncompressed CBOR sequence — the layer's identity.
    pub diff_id: Digest,
    /// Number of records, header record included.
    pub records: u64,
    /// Uncompressed byte length. Summed across planned layers, this is the
    /// restore-time disk preflight.
    pub uncompressed_size: u64,
    /// Profile-owned, opaque to the protocol.
    pub scope: serde_json::Value,
}

/// The canonical, signable document of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Inscription {
    /// Inscription schema version. Verifiers fail closed on anything but
    /// [`SCHEMA_VERSION`].
    pub schema: u64,
    pub profile: ProfileRef,
    /// The protocol's monotonic ordering key. A profile decides what it counts
    /// (the Dolos profile sets it to the Cardano epoch).
    pub sequence: u64,
    /// The inscription this one follows. Omitted, never `null`, in the first
    /// inscription of a lineage, which may start at any sequence.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_link"
    )]
    pub previous: Option<Link>,
    /// Profile-owned, opaque: where in its source domain this snapshot stands.
    pub position: serde_json::Value,
    /// Profile-owned, opaque: the parameters a reader needs to interpret the
    /// layers.
    pub parameters: serde_json::Value,
    pub compression: Compression,
    /// The layers every later snapshot of the lineage inherits.
    pub diff: Vec<LayerDescriptor>,
    /// The layers that belong to this snapshot alone. Always present, and may
    /// be empty.
    pub tip: Vec<LayerDescriptor>,
}

/// A `previous` key that is present must hold a link: `null` is refused rather
/// than read as the start of a lineage, which only omitting the key says.
fn present_link<'de, D>(deserializer: D) -> Result<Option<Link>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Link::deserialize(deserializer).map(Some)
}

impl Inscription {
    /// An inscription at the current schema version, with no link and no
    /// layers yet.
    pub fn new(
        profile: &dyn Profile,
        sequence: u64,
        position: serde_json::Value,
        parameters: serde_json::Value,
        compression: Compression,
    ) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            profile: ProfileRef {
                name: profile.name().to_owned(),
                version: profile.version(),
            },
            sequence,
            previous: None,
            position,
            parameters,
            compression,
            diff: Vec::new(),
            tip: Vec::new(),
        }
    }

    /// The RFC 8785 canonical JSON encoding of this inscription.
    ///
    /// Validates first: canonical bytes, and therefore an identity, exist only
    /// for a well-formed inscription.
    pub fn canonicalize(&self) -> Result<Vec<u8>, Error> {
        let value = serde_json::to_value(self)?;
        self.validate_structure()?;
        check_safe_numbers(&value)?;
        canonical_json(&value)
    }

    /// The snapshot's identity: sha256 of [`Inscription::canonicalize`].
    pub fn digest(&self) -> Result<Digest, Error> {
        Ok(Digest::compute(self.canonicalize()?))
    }

    /// Parse untrusted inscription bytes, fail-closed.
    ///
    /// Rejects an unknown generic key anywhere in the document (extension
    /// happens only inside `position`, `parameters` and `scope`), a schema
    /// version this implementation does not implement, a number outside the
    /// JCS-safe integer range, a malformed profile name or payload media type,
    /// a violation of the link invariant, and a kind and scope listed twice.
    ///
    /// It deliberately does *not* check which profile the inscription belongs
    /// to — that needs the profile implementation, and is
    /// [`Inscription::check_profile`].
    ///
    /// Nor does it reject a document with a repeated JSON key, which JSON
    /// itself leaves undefined and `serde_json` resolves last-wins. That is not
    /// a hole in practice: canonicalizing the result yields different bytes
    /// from the input, so any check that compares an inscription against its
    /// digest — which is every use of one — refuses it. Both transports make
    /// the comparison explicit when they read an inscription.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;

        // Checked on the raw document, so that numbers inside the profile's
        // opaque objects are covered too.
        check_safe_numbers(&value)?;

        let inscription: Self = serde_json::from_value(value)?;
        inscription.validate_structure()?;

        Ok(inscription)
    }

    /// Full validation of a locally built inscription.
    pub fn validate(&self) -> Result<(), Error> {
        let value = serde_json::to_value(self)?;
        self.validate_structure()?;
        check_safe_numbers(&value)
    }

    /// Every layer this inscription lists: `diff`, then `tip`.
    pub fn layers(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.diff.iter().chain(&self.tip)
    }

    /// Refuse an inscription this profile implementation cannot read.
    ///
    /// Two ways it fails, both clean refusals rather than a partial restore:
    /// the inscription belongs to a different profile, or it needs a profile
    /// major version above the one implemented here. A layer of a kind this
    /// implementation does not define is *not* one of them — see
    /// [`Inscription::unknown_layers`] for why, and
    /// [`Inscription::check_profile_strict`] for the side that still refuses
    /// it.
    pub fn check_profile(&self, profile: &dyn Profile) -> Result<(), Error> {
        if self.profile.name != profile.name() {
            return Err(Error::UnknownProfile {
                found: self.profile.name.clone(),
                expected: profile.name().to_owned(),
            });
        }

        if self.profile.version > profile.version() {
            return Err(Error::UnsupportedProfileVersion {
                name: self.profile.name.clone(),
                found: self.profile.version,
                supported: profile.version(),
            });
        }

        let kinds = profile.kinds();

        for layer in self.layers() {
            // Left to `unknown_layers`. Its media type is still checked:
            // `validate_structure` has already established that it parses and
            // does not squat the reserved `stelae` vendor.
            if !kinds.contains(&layer.kind.as_str()) {
                continue;
            }

            // Pins the profile's own answer to the naming rules before it is
            // used as the yardstick.
            let defined = checked_layer_media_type(profile, &layer.kind)?;
            let expected = MediaType::parse(&defined)?;
            let found = MediaType::parse(&layer.media_type)?;

            // Compared on vendor and kind, not on the whole string. Those two
            // are what make a descriptor unambiguously this profile's layer;
            // the version and codec are transport detail a profile may move
            // within one major, and freezing them here would refuse an
            // inscription this implementation can otherwise read.
            //
            // `validate_structure` already established that `layer.media_type`
            // parses and does not squat the reserved vendor. What it cannot
            // know, without a profile in hand, is whether the name belongs to
            // the profile the inscription claims — that is this check.
            if found.vendor != expected.vendor || found.kind != expected.kind {
                return Err(Error::InvalidMediaType {
                    value: layer.media_type.clone(),
                    reason: format!(
                        "profile {:?} names layer kind {:?} {defined:?}",
                        profile.name(),
                        layer.kind,
                    ),
                });
            }
        }

        Ok(())
    }

    /// The layers whose kind `profile` does not define, `diff` then `tip`.
    ///
    /// The client half of an additive change. A profile that gains a kind
    /// publishes it as a new media type on a new layer, and an older reader
    /// cannot store what it does not model — but refusing the whole snapshot
    /// over it would make every additive change break every deployed
    /// reader. So the protocol reports the layers rather than refusing
    /// them, and the profile decides during planning what skipping each one
    /// costs.
    ///
    /// **Nothing here reads a scope.** Whether a layer is one a reader may skip
    /// is a profile question, answered from the profile-owned `scope` the
    /// descriptor carries, and the protocol interprets no field of it.
    pub fn unknown_layers<'a>(&'a self, profile: &dyn Profile) -> Vec<&'a LayerDescriptor> {
        let kinds = profile.kinds();

        self.layers()
            .filter(|layer| !kinds.contains(&layer.kind.as_str()))
            .collect()
    }

    /// [`Inscription::check_profile`], plus a refusal of any unknown kind.
    ///
    /// What a publisher checks, and the asymmetry is the point: a reader
    /// consumes the layers it understands, while a publisher *attests* every
    /// layer it lists — its inscription is the document independent parties
    /// reproduce and, later, sign. Chaining onto a snapshot carrying a kind
    /// this binary cannot build means either dropping that layer from the
    /// new snapshot or attesting bytes it never read, and both are worse
    /// than stopping.
    pub fn check_profile_strict(&self, profile: &dyn Profile) -> Result<(), Error> {
        self.check_profile(profile)?;

        if let Some(layer) = self.unknown_layers(profile).first() {
            return Err(Error::UnknownLayerKind {
                profile: profile.name().to_owned(),
                kind: layer.kind.clone(),
            });
        }

        Ok(())
    }

    /// Layers of a given kind, `diff` then `tip`.
    pub fn layers_of_kind<'a>(
        &'a self,
        kind: &'a str,
    ) -> impl Iterator<Item = &'a LayerDescriptor> + 'a {
        self.layers().filter(move |l| l.kind == kind)
    }

    /// Total uncompressed bytes across every layer this inscription lists —
    /// the restore-time disk preflight, in the one place that knows the sizes.
    pub fn uncompressed_size(&self) -> u64 {
        self.layers().map(|l| l.uncompressed_size).sum()
    }

    fn validate_structure(&self) -> Result<(), Error> {
        if self.schema != SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema { found: self.schema });
        }

        validate_profile_name(&self.profile.name)?;

        if self.profile.version == 0 {
            return Err(Error::InvalidProfileName {
                value: self.profile.name.clone(),
                reason: "profile major version must be at least 1".to_owned(),
            });
        }

        if self.compression.algo.is_empty() {
            return Err(Error::Canonicalization(
                "compression.algo must not be empty".to_owned(),
            ));
        }

        for layer in self.layers() {
            if layer.kind.is_empty() {
                return Err(Error::Canonicalization(
                    "a layer descriptor has an empty kind".to_owned(),
                ));
            }

            // Enforces the vendor-namespacing rules on whatever the publisher
            // wrote, including that `vnd.stelae.*` is never a payload type.
            MediaType::parse(&layer.media_type)?;
        }

        self.validate_link()?;
        self.validate_one_layer_per_scope()
    }

    /// `previous`, when present, names a sequence below this one.
    ///
    /// Below and not exactly one below: how a lineage numbers its snapshots
    /// is its publisher's schedule, so the universal part is a chain that only
    /// runs backwards. [`link_for`] is the stricter rule a publisher builds by.
    fn validate_link(&self) -> Result<(), Error> {
        match self.previous {
            Some(previous) if previous.sequence >= self.sequence => {
                Err(Error::LinkInvariant(format!(
                    "previous names sequence {} which is not below this snapshot's sequence {}",
                    previous.sequence, self.sequence
                )))
            }
            _ => Ok(()),
        }
    }

    /// No `(kind, scope)` pair appears twice across `diff` and `tip`.
    ///
    /// Scopes are compared by their canonical encoding, the bytes the digest
    /// covers. A pair listed twice is two layers claiming one place in the
    /// dataset, and nothing in the document says which of them is meant.
    fn validate_one_layer_per_scope(&self) -> Result<(), Error> {
        let mut seen = BTreeSet::new();

        for layer in self.layers() {
            let scope = String::from_utf8(canonical_json(&layer.scope)?)
                .map_err(|e| Error::Canonicalization(e.to_string()))?;

            let pair = (layer.kind.as_str(), scope);

            if seen.contains(&pair) {
                return Err(Error::DuplicateLayer {
                    kind: layer.kind.clone(),
                    scope: pair.1,
                });
            }

            seen.insert(pair);
        }

        Ok(())
    }
}

/// The link a snapshot at `sequence` carries when it follows `previous`.
///
/// The constructive half of the invariant [`Inscription::validate`] checks, and
/// the stricter one: a publisher extends its lineage one sequence at a time.
///
/// The two legal readings of what came before, and the one refusal:
///
/// - **nothing there** — no link, which the protocol permits at any sequence.
///   The first snapshot of a lineage carries none, and so does a publisher
///   deliberately starting one at sequence 500;
/// - **the snapshot before this one** — a link naming its sequence and its
///   inscription digest;
/// - **anything else** — refused, naming both sequences and, for a gap, the
///   distance between them. A gap means a publisher skipped sequences, an equal
///   sequence means it is republishing one, and a higher one means the
///   repository is ahead of this node. All three are operational faults with
///   different fixes, so the message says which.
///
/// Whether a deliberate gap ever gets a policy is not this function's to
/// invent; there is no flag here that overrides the refusal.
///
/// It lives beside the invariant rather than beside a transport because a
/// verifier reaches it without one, and a rule this load-bearing should not be
/// compiled out of a build that still has to reproduce a linked digest.
pub fn link_for(previous: Option<&Inscription>, sequence: u64) -> Result<Option<Link>, Error> {
    let Some(previous) = previous else {
        return Ok(None);
    };

    let latest = previous.sequence;

    let reason = match latest.checked_add(1) {
        Some(next) if next == sequence => {
            return Ok(Some(Link {
                sequence: latest,
                inscription_digest: previous.digest()?,
            }));
        }
        _ if latest >= sequence => {
            "this snapshot is at or behind the repository's latest; a republish would restart the \
             chain rather than extend it"
                .to_owned()
        }
        _ => format!(
            "this node is {} sequences ahead, and a publish must follow the repository's latest \
             snapshot: this one would leave a gap no later snapshot could close",
            sequence - latest,
        ),
    };

    Err(Error::LinkBreak {
        latest,
        publishing: sequence,
        reason,
    })
}

/// RFC 8785 canonical JSON encoding of an arbitrary value.
///
/// Exposed so the conformance vectors exercise the same code path the
/// inscription digest depends on, rather than a parallel one.
pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    serde_jcs::to_vec(value).map_err(|e| Error::Canonicalization(e.to_string()))
}

/// Assert that every number in `value` is an integer RFC 8785 renders exactly.
///
/// Walks into arrays and objects, so a profile's opaque `position`,
/// `parameters` and `scope` are held to the same rule as the generic keys. See
/// the module documentation for why the range matters.
pub fn check_safe_numbers(value: &serde_json::Value) -> Result<(), Error> {
    check_numbers_at(value, "$")
}

fn check_numbers_at(value: &serde_json::Value, path: &str) -> Result<(), Error> {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(unsigned) = number.as_u64() {
                if unsigned > MAX_SAFE_INTEGER as u64 {
                    return Err(Error::UnsafeInteger {
                        path: path.to_owned(),
                        value: number.to_string(),
                    });
                }
                Ok(())
            } else if let Some(signed) = number.as_i64() {
                if signed < -MAX_SAFE_INTEGER {
                    return Err(Error::UnsafeInteger {
                        path: path.to_owned(),
                        value: number.to_string(),
                    });
                }
                Ok(())
            } else {
                Err(Error::NonIntegerNumber {
                    path: path.to_owned(),
                    value: number.to_string(),
                })
            }
        }
        serde_json::Value::Array(items) => items
            .iter()
            .enumerate()
            .try_for_each(|(index, item)| check_numbers_at(item, &format!("{path}[{index}]"))),
        serde_json::Value::Object(entries) => entries
            .iter()
            .try_for_each(|(key, item)| check_numbers_at(item, &format!("{path}.{key}"))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Toy;

    impl Profile for Toy {
        fn name(&self) -> &str {
            "dev.example.toy"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.example.stelae.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("note-{sequence}"))
        }
    }

    fn digest_of(byte: u8) -> Digest {
        Digest::from_bytes([byte; 32])
    }

    fn notes(diff_id: u8, scope: serde_json::Value) -> LayerDescriptor {
        LayerDescriptor {
            kind: "notes".to_owned(),
            media_type: "application/vnd.example.stelae.notes.v1+zstd".to_owned(),
            diff_id: digest_of(diff_id),
            records: 5,
            uncompressed_size: 128,
            scope,
        }
    }

    /// A linked inscription with one layer in each array.
    fn sample() -> Inscription {
        let mut inscription = at(
            3,
            Some(Link {
                sequence: 2,
                inscription_digest: digest_of(2),
            }),
        );

        inscription.position = json!({"chapter": 3, "label": "third"});
        inscription.diff = vec![notes(3, json!({"chapter": 3}))];
        inscription.tip = vec![notes(4, json!({"draft": 4}))];

        inscription
    }

    /// An inscription at `sequence` carrying `previous` and no layers, for the
    /// chain rules below.
    fn at(sequence: u64, previous: Option<Link>) -> Inscription {
        let mut inscription = Inscription::new(
            &Toy,
            sequence,
            json!({"chapter": sequence}),
            json!({"noteWidth": 40}),
            Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
        );

        inscription.previous = previous;
        inscription
    }

    fn reparse(value: &serde_json::Value) -> Result<Inscription, Error> {
        Inscription::parse(&serde_json::to_vec(value).unwrap())
    }

    /// The first snapshot of a lineage carries no link, at any sequence.
    #[test]
    fn an_empty_repository_starts_a_lineage() {
        assert_eq!(link_for(None, 0).unwrap(), None);
        assert_eq!(link_for(None, 500).unwrap(), None);
    }

    #[test]
    fn a_publish_that_follows_latest_links_to_it() {
        let previous = sample();

        let link = link_for(Some(&previous), 4).unwrap();

        assert_eq!(
            link,
            Some(Link {
                sequence: 3,
                inscription_digest: previous.digest().unwrap(),
            }),
            "the sequence and the identity of the snapshot it came from"
        );

        // The invariant holds by construction rather than by inspection: a
        // document built on this link validates.
        at(4, link).validate().unwrap();
    }

    /// All three refusals name both sequences, because which of the three it is
    /// decides what the publisher does about it.
    #[test]
    fn a_publish_that_does_not_follow_latest_is_refused() {
        let previous = at(497, None);

        for publishing in [500, 497, 496] {
            let err = link_for(Some(&previous), publishing).unwrap_err();
            let message = err.to_string();

            assert!(
                matches!(err, Error::LinkBreak { .. }),
                "{publishing}: {err:?}"
            );

            assert!(message.contains("497"), "{publishing}: {message}");
            assert!(
                message.contains(&publishing.to_string()),
                "{publishing}: {message}"
            );
        }
    }

    #[test]
    fn a_gap_and_a_republish_are_told_apart() {
        let previous = at(497, None);

        assert!(link_for(Some(&previous), 500)
            .unwrap_err()
            .to_string()
            .contains("gap"));

        assert!(link_for(Some(&previous), 497)
            .unwrap_err()
            .to_string()
            .contains("republish"));

        assert!(link_for(Some(&previous), 496)
            .unwrap_err()
            .to_string()
            .contains("republish"));
    }

    /// A gap says how far. "The repository is at 497 and you are at 500" is a
    /// different incident from being one epoch out, and the operator reading
    /// the message should not have to subtract to find out which they have.
    #[test]
    fn a_gap_names_the_distance_alongside_both_sequences() {
        let previous = at(497, None);

        let message = link_for(Some(&previous), 500).unwrap_err().to_string();

        assert!(message.contains("497"), "{message}");
        assert!(message.contains("500"), "{message}");
        assert!(message.contains("3 sequences ahead"), "{message}");
    }

    #[test]
    fn canonical_form_is_stable_and_parses_back() {
        let inscription = sample();
        let canonical = inscription.canonicalize().unwrap();

        // Canonical JSON is sorted, minimal and free of insignificant
        // whitespace, so the bytes are the same on any run.
        assert_eq!(canonical, inscription.canonicalize().unwrap());
        let text = String::from_utf8(canonical.clone()).unwrap();
        assert!(text.starts_with(r#"{"compression":{"algo":"zstd","level":9},"diff":[{"#));
        assert!(text.contains(r#""previous":{"inscriptionDigest":"sha256:"#));
        assert!(text.contains(r#""tip":[{"#));
        assert!(!text.contains(' '));

        let parsed = Inscription::parse(&canonical).unwrap();
        assert_eq!(parsed, inscription);
        assert_eq!(parsed.digest().unwrap(), inscription.digest().unwrap());
    }

    /// The first inscription of a lineage has no `previous` key at all, and
    /// `tip` is written even when it is empty.
    #[test]
    fn a_first_inscription_omits_previous_and_keeps_an_empty_tip() {
        let mut first = sample();
        first.previous = None;
        first.tip.clear();

        let text = String::from_utf8(first.canonicalize().unwrap()).unwrap();
        assert!(!text.contains("previous"), "{text}");
        assert!(text.contains(r#""tip":[]"#), "{text}");

        assert_eq!(Inscription::parse(text.as_bytes()).unwrap(), first);
    }

    /// Only omission starts a lineage. A `null` link is not a spelling of it,
    /// and neither array may be left out.
    #[test]
    fn rejects_a_null_previous_and_a_missing_array() {
        let mut value = serde_json::to_value(sample()).unwrap();
        value["previous"] = serde_json::Value::Null;

        let err = reparse(&value).unwrap_err();
        assert!(
            matches!(&err, Error::Json(e) if e.to_string().contains("invalid type: null")),
            "{err:?}"
        );

        for missing in ["diff", "tip"] {
            let mut value = serde_json::to_value(sample()).unwrap();
            value.as_object_mut().unwrap().remove(missing);

            let err = reparse(&value).unwrap_err();
            assert!(
                matches!(&err, Error::Json(e) if e.to_string().contains("missing field")),
                "{missing}: {err:?}"
            );
        }
    }

    /// The single layer list and the full history are not read as anything:
    /// a document in that shape is refused, like any other unknown key.
    #[test]
    fn rejects_the_single_layer_list_and_the_history() {
        for (old, replaces) in [("layers", "diff"), ("history", "previous")] {
            let mut value = serde_json::to_value(sample()).unwrap();
            let object = value.as_object_mut().unwrap();
            let moved = object.remove(replaces).unwrap();
            object.insert(old.to_owned(), json!([moved]));

            let err = reparse(&value).unwrap_err();
            assert!(
                matches!(&err, Error::Json(e) if e.to_string().contains("unknown field")),
                "{old}: {err:?}"
            );
        }
    }

    #[test]
    fn digest_is_sha256_of_the_canonical_bytes() {
        let inscription = sample();
        assert_eq!(
            inscription.digest().unwrap(),
            Digest::compute(inscription.canonicalize().unwrap())
        );
    }

    /// Key order in the source document must not reach the digest — that is the
    /// whole point of canonicalization.
    #[test]
    fn digest_ignores_source_key_order_and_whitespace() {
        let inscription = sample();
        let canonical = inscription.canonicalize().unwrap();

        let pretty = serde_json::to_vec_pretty(&inscription).unwrap();
        let reparsed = Inscription::parse(&pretty).unwrap();

        assert_ne!(pretty, canonical);
        assert_eq!(reparsed.canonicalize().unwrap(), canonical);
    }

    #[test]
    fn rejects_an_unknown_generic_key() {
        let inscription = sample();
        let mut value = serde_json::to_value(&inscription).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".to_owned(), json!(1));

        let err = reparse(&value).unwrap_err();
        assert!(
            matches!(&err, Error::Json(e) if e.to_string().contains("unknown field")),
            "{err:?}"
        );
    }

    /// Unknown keys are refused inside the generic *nested* objects too;
    /// extension happens only inside the three opaque ones.
    #[test]
    fn rejects_an_unknown_key_in_a_nested_generic_object() {
        for pointer in ["/profile", "/compression", "/previous", "/diff/0", "/tip/0"] {
            let inscription = sample();
            let mut value = serde_json::to_value(&inscription).unwrap();
            value
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("extra".to_owned(), json!(1));

            let err = reparse(&value).unwrap_err();
            assert!(
                matches!(&err, Error::Json(e) if e.to_string().contains("unknown field")),
                "{pointer}: {err:?}"
            );
        }
    }

    /// The opaque objects, by contrast, take anything — that is the extension
    /// point.
    #[test]
    fn opaque_objects_accept_arbitrary_shapes() {
        for value in [
            json!({"deeply": {"nested": [1, 2, {"three": true}]}}),
            json!([1, 2, 3]),
            json!("a string"),
            json!(null),
            json!(0),
        ] {
            let mut inscription = sample();
            inscription.position = value.clone();
            inscription.parameters = value.clone();
            inscription.diff[0].scope = value.clone();

            let canonical = inscription.canonicalize().unwrap();
            let parsed = Inscription::parse(&canonical).unwrap();
            assert_eq!(parsed.position, value);
            assert_eq!(parsed.diff[0].scope, value);
        }
    }

    #[test]
    fn rejects_an_unsupported_schema_version() {
        let mut inscription = sample();
        inscription.schema = 2;

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(err, Error::UnsupportedSchema { found: 2 }),
            "{err:?}"
        );

        // And it must not be possible to obtain a digest for it.
        assert!(inscription.canonicalize().is_err());
    }

    #[test]
    fn rejects_an_unknown_profile_and_a_higher_major_version() {
        let mut inscription = sample();
        inscription.profile.name = "com.acme.other".to_owned();
        let err = inscription.check_profile(&Toy).unwrap_err();
        assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");

        let mut inscription = sample();
        inscription.profile.version = 2;
        let err = inscription.check_profile(&Toy).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedProfileVersion {
                    found: 2,
                    supported: 1,
                    ..
                }
            ),
            "{err:?}"
        );

        // An older major version is readable by a newer implementation.
        let inscription = sample();
        inscription.check_profile(&Toy).unwrap();

        // A layer kind the profile does not define is readable — skippable, and
        // reported — but not publishable, in either array.
        let mut inscription = sample();
        inscription.diff[0].kind = "blocks".to_owned();
        inscription.tip[0].kind = "state".to_owned();
        inscription.check_profile(&Toy).unwrap();

        assert_eq!(
            inscription
                .unknown_layers(&Toy)
                .iter()
                .map(|l| l.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["blocks", "state"],
        );

        let err = inscription.check_profile_strict(&Toy).unwrap_err();
        assert!(matches!(err, Error::UnknownLayerKind { .. }), "{err:?}");
    }

    /// A media type outside the profile's vendor is refused in `tip` as it is
    /// in `diff`.
    #[test]
    fn a_tip_layer_is_held_to_the_profile_media_type() {
        let mut inscription = sample();
        inscription.tip[0].media_type = "application/vnd.acme.stelae.notes.v1+zstd".to_owned();

        let err = inscription.check_profile(&Toy).unwrap_err();
        assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
    }

    #[test]
    fn rejects_a_malformed_profile_name() {
        let mut inscription = sample();
        inscription.profile.name = "toy".to_owned();
        assert!(matches!(
            inscription.validate().unwrap_err(),
            Error::InvalidProfileName { .. }
        ));
    }

    #[test]
    fn rejects_a_payload_media_type_in_the_reserved_namespace() {
        for array in [0, 1] {
            let mut inscription = sample();
            let layer = match array {
                0 => &mut inscription.diff[0],
                _ => &mut inscription.tip[0],
            };
            layer.media_type = "application/vnd.stelae.stelae.notes.v1+zstd".to_owned();

            let err = inscription.validate().unwrap_err();
            assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
        }
    }

    #[test]
    fn rejects_an_empty_kind_in_either_array() {
        for array in [0, 1] {
            let mut inscription = sample();
            let layer = match array {
                0 => &mut inscription.diff[0],
                _ => &mut inscription.tip[0],
            };
            layer.kind.clear();

            let err = inscription.validate().unwrap_err();
            assert!(
                matches!(&err, Error::Canonicalization(m) if m.contains("empty kind")),
                "{err:?}"
            );
        }
    }

    /// The link may name any earlier sequence: the protocol holds a chain to
    /// running backwards, and leaves a lineage's numbering to its publisher.
    #[test]
    fn the_link_names_an_earlier_sequence() {
        sample().validate().unwrap();

        let mut skipped = sample();
        skipped.previous = Some(Link {
            sequence: 1,
            inscription_digest: digest_of(1),
        });
        skipped.validate().unwrap();

        // A lineage may start at any sequence, the very first included.
        at(0, None).validate().unwrap();
        at(500, None).validate().unwrap();
    }

    #[test]
    fn the_link_rejects_this_sequence_and_a_later_one() {
        for named in [3, 4] {
            let mut inscription = sample();
            inscription.previous = Some(Link {
                sequence: named,
                inscription_digest: digest_of(9),
            });

            let err = inscription.validate().unwrap_err();
            assert!(
                matches!(&err, Error::LinkInvariant(m) if m.contains("not below")),
                "{named}: {err:?}"
            );

            // An invalid link has no identity either.
            assert!(inscription.canonicalize().is_err());
        }
    }

    /// One `(kind, scope)` pair, one layer: within an array and across the two.
    #[test]
    fn rejects_a_kind_and_scope_listed_twice() {
        let mut across = sample();
        across.tip = vec![notes(4, json!({"chapter": 3}))];

        let mut in_diff = sample();
        in_diff.diff.push(notes(5, json!({"chapter": 3})));

        let mut in_tip = sample();
        in_tip.tip.push(notes(5, json!({"draft": 4})));

        // The same bytes twice are still two layers for one scope.
        let mut identical = sample();
        identical.diff.push(identical.diff[0].clone());

        for (case, inscription) in [
            ("across", across),
            ("in diff", in_diff),
            ("in tip", in_tip),
            ("identical", identical),
        ] {
            let err = inscription.validate().unwrap_err();
            assert!(
                matches!(&err, Error::DuplicateLayer { kind, .. } if kind == "notes"),
                "{case}: {err:?}"
            );
            assert!(err.to_string().contains("more than once"), "{case}: {err}");
        }
    }

    /// Scopes are compared as canonical JSON, so key order cannot hide a
    /// duplicate, and the kind is half of the pair.
    #[test]
    fn a_pair_is_a_kind_and_a_canonical_scope() {
        let mut reordered = sample();
        reordered.diff = vec![notes(3, json!({"chapter": 3, "shelf": "east"}))];
        reordered.tip = vec![notes(4, json!({"shelf": "east", "chapter": 3}))];

        let err = reordered.validate().unwrap_err();
        assert!(
            matches!(&err, Error::DuplicateLayer { scope, .. } if scope == r#"{"chapter":3,"shelf":"east"}"#),
            "{err:?}"
        );

        // The same scope under another kind is another layer.
        let mut other_kind = sample();
        let mut index = notes(5, json!({"chapter": 3}));
        index.kind = "index".to_owned();
        index.media_type = "application/vnd.example.stelae.index.v1+zstd".to_owned();
        other_kind.tip.push(index);
        other_kind.validate().unwrap();
    }

    /// A parsed document is held to the pair rule too, not only a built one.
    #[test]
    fn parse_rejects_a_kind_and_scope_listed_twice() {
        let mut value = serde_json::to_value(sample()).unwrap();
        value["tip"][0]["scope"] = json!({"chapter": 3});

        let err = reparse(&value).unwrap_err();
        assert!(matches!(err, Error::DuplicateLayer { .. }), "{err:?}");
    }

    /// Document text an error repeats is escaped, so a hostile key or scope
    /// cannot write a terminal escape sequence into the report.
    #[test]
    fn errors_escape_control_characters_from_the_document() {
        // ESC, a C1 CSI and a bidi override: JSON escapes only the first.
        let key = "\u{1b}[2J\u{9b}\u{202e}";

        let unsafe_integer = check_safe_numbers(&json!({ key: MAX_SAFE_INTEGER as u64 + 1 }));
        let non_integer = check_safe_numbers(&json!({ key: 1.5 }));

        let mut duplicate = sample();
        duplicate.diff = vec![notes(3, json!({ key: 3 }))];
        duplicate.tip = vec![notes(4, json!({ key: 3 }))];

        for err in [
            unsafe_integer.unwrap_err(),
            non_integer.unwrap_err(),
            duplicate.validate().unwrap_err(),
        ] {
            let message = err.to_string();
            assert!(
                !message.contains(['\u{1b}', '\u{9b}', '\u{202e}']),
                "{message:?}"
            );
            assert!(message.contains("\\u{202e}"), "{message}");
        }
    }

    #[test]
    fn rejects_numbers_outside_the_jcs_safe_integer_range() {
        let safe = MAX_SAFE_INTEGER as u64;

        let mut inscription = sample();
        inscription.diff[0].uncompressed_size = safe;
        inscription.canonicalize().unwrap();

        let mut inscription = sample();
        inscription.diff[0].uncompressed_size = safe + 1;
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.diff[0].uncompressedSize"),
            "{err:?}"
        );

        let mut inscription = sample();
        inscription.tip[0].records = safe + 1;
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.tip[0].records"),
            "{err:?}"
        );

        let mut inscription = sample();
        inscription.sequence = safe + 1;
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.sequence"),
            "{err:?}"
        );

        // Including inside the profile's opaque objects, which is where a
        // vendor would most plausibly put a raw u64.
        let mut inscription = sample();
        inscription.position = json!({"raw": u64::MAX});
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.position.raw"),
            "{err:?}"
        );

        let mut inscription = sample();
        inscription.parameters = json!({"nested": [0, -9_007_199_254_740_992i64]});
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.parameters.nested[1]"),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_non_integer_numbers() {
        let mut inscription = sample();
        inscription.parameters = json!({"ratio": 0.5});

        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::NonIntegerNumber { path, .. } if path == "$.parameters.ratio"),
            "{err:?}"
        );

        // `56.0` is an integral value but a JSON float, and RFC 8785 renders it
        // through the float path. The rule is about the encoding, not the value.
        let mut inscription = sample();
        inscription.parameters = json!({"whole": 56.0});
        assert!(matches!(
            inscription.canonicalize().unwrap_err(),
            Error::NonIntegerNumber { .. }
        ));
    }

    #[test]
    fn parse_rejects_unsafe_numbers_in_raw_bytes() {
        let raw = br#"{"schema":1,"profile":{"name":"dev.example.toy","version":1},"sequence":1,"position":{"n":18446744073709551615},"parameters":{},"compression":{"algo":"zstd","level":9},"diff":[],"tip":[]}"#;

        let err = Inscription::parse(raw).unwrap_err();
        assert!(matches!(err, Error::UnsafeInteger { .. }), "{err:?}");
    }

    #[test]
    fn layers_run_diff_then_tip() {
        let mut inscription = sample();
        inscription.diff.push(notes(5, json!({"chapter": 4})));

        assert_eq!(
            inscription.layers().map(|l| l.diff_id).collect::<Vec<_>>(),
            vec![digest_of(3), digest_of(5), digest_of(4)],
        );

        assert_eq!(inscription.uncompressed_size(), 3 * 128);
        assert_eq!(inscription.layers_of_kind("notes").count(), 3);
        assert_eq!(inscription.layers_of_kind("blocks").count(), 0);
    }
}
