//! A snapshot resolved through its chain.
//!
//! An inscription lists only its own layers and links to the one before it
//! (decision 0080), so no single document holds a snapshot's layer set. The
//! set at sequence N is the `diff` of every inscription of the lineage up to
//! N, in ascending sequence, then the `tip` of N. [`Chain::walk`] follows the
//! links from N to the inscription that has none, checks each one, and builds
//! that set.
//!
//! How a linked inscription is fetched is the transport's business. The walk
//! asks a closure for the inscription a [`Link`] names. A registry answers
//! from documents it reads ahead, several sequences at a time; see
//! [`crate::oci::Registry::resolve`].
//!
//! ## What each link must prove
//!
//! For every link, in this order:
//!
//! 1. the transport holds a document for it, or the walk stops with
//!    [`Error::MissingLink`], which is not an absent snapshot;
//! 2. that document is not one the chain already holds, which would loop;
//! 3. its digest is the link's `inscriptionDigest`;
//! 4. its sequence is the link's, and so below the inscription that links to
//!    it;
//! 5. it is an inscription of the profile being read.
//!
//! The walk ends at the first inscription without a link, at any sequence: a
//! lineage may start late (umbrella ruling 2).
//!
//! ## One layer per kind and scope, across the chain
//!
//! The layer set may list a `(kind, scope)` pair once. An earlier tip is not
//! part of the set, so a layer a tip carried while it was unsettled may
//! appear later in a `diff`, in its final form.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    inscription::{canonical_json, Inscription, LayerDescriptor, Link},
    manifest::Array,
    profile::Profile,
    Digest, Error,
};

/// One layer of a resolved snapshot, and where its chain lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainLayer {
    pub descriptor: LayerDescriptor,
    /// The sequence of the inscription that lists this layer. That
    /// inscription's `parameters` and `compression` are the ones that
    /// describe it; [`Chain::origin`] returns it.
    pub sequence: u64,
    /// `diff`, or the head's `tip`.
    pub array: Array,
}

/// The inscriptions of one snapshot, from its lineage's first to its head,
/// and the layer set they make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    /// Ascending by sequence; the last is the head.
    inscriptions: Vec<Inscription>,
    identity: Digest,
    layers: Vec<ChainLayer>,
}

impl Chain {
    /// Follow `head`'s links back to the start of its lineage.
    ///
    /// `fetch` answers a link with the inscription the transport holds for
    /// it, or `None` when it holds none there. Its errors end the walk as
    /// they are. See the module documentation for the checks.
    pub fn walk<F>(profile: &dyn Profile, head: Inscription, mut fetch: F) -> Result<Self, Error>
    where
        F: FnMut(&Link) -> Result<Option<Inscription>, Error>,
    {
        head.check_profile(profile)?;

        let identity = head.digest()?;
        let mut seen = BTreeSet::from([identity]);
        let mut inscriptions = vec![head];

        loop {
            let current = &inscriptions[inscriptions.len() - 1];
            let (Some(link), from) = (current.previous, current.sequence) else {
                break;
            };

            let Some(found) = fetch(&link)? else {
                return Err(Error::MissingLink {
                    from,
                    sequence: link.sequence,
                });
            };

            let digest = found.digest()?;

            if seen.contains(&digest) {
                return Err(Error::LinkInvariant(format!(
                    "the inscription at sequence {from} links to sequence {}, and the document \
                     found there is the inscription at sequence {}, already in the chain: the \
                     links loop",
                    link.sequence, found.sequence,
                )));
            }

            if digest != link.inscription_digest {
                return Err(Error::DigestMismatch {
                    subject: format!(
                        "the inscription at sequence {}, linked from sequence {from}",
                        link.sequence,
                    ),
                    expected: link.inscription_digest.to_string(),
                    actual: digest.to_string(),
                });
            }

            // Below `from` follows: the inscription at `from` was validated
            // when its digest was taken, and a link names a lower sequence.
            if found.sequence != link.sequence {
                return Err(Error::LinkInvariant(format!(
                    "the inscription at sequence {from} links to sequence {}, and the inscription \
                     that digest names is at sequence {}",
                    link.sequence, found.sequence,
                )));
            }

            found.check_profile(profile)?;

            seen.insert(digest);
            inscriptions.push(found);
        }

        inscriptions.reverse();

        let layers = layer_set(&inscriptions)?;

        Ok(Self {
            inscriptions,
            identity,
            layers,
        })
    }

    /// The inscription the snapshot was resolved at.
    pub fn head(&self) -> &Inscription {
        &self.inscriptions[self.inscriptions.len() - 1]
    }

    /// The head's digest: the snapshot's identity.
    pub fn identity(&self) -> Digest {
        self.identity
    }

    /// The first inscription of the lineage, the one with no link.
    pub fn first(&self) -> &Inscription {
        &self.inscriptions[0]
    }

    /// Every inscription of the chain, in ascending sequence.
    pub fn inscriptions(&self) -> &[Inscription] {
        &self.inscriptions
    }

    /// The inscription of the chain at `sequence`.
    pub fn inscription(&self, sequence: u64) -> Option<&Inscription> {
        self.inscriptions
            .binary_search_by_key(&sequence, |inscription| inscription.sequence)
            .ok()
            .map(|index| &self.inscriptions[index])
    }

    /// The inscription that lists `layer`, whose `parameters` and
    /// `compression` describe it. `None` for a layer of another chain.
    pub fn origin(&self, layer: &ChainLayer) -> Option<&Inscription> {
        self.inscription(layer.sequence)
    }

    /// The layer set: every `diff` in ascending sequence, then the head's
    /// `tip`.
    pub fn layers(&self) -> &[ChainLayer] {
        &self.layers
    }

    /// The layer set's descriptors, in the same order.
    pub fn descriptors(&self) -> impl Iterator<Item = &LayerDescriptor> {
        self.layers.iter().map(|layer| &layer.descriptor)
    }

    /// Total uncompressed bytes across the layer set: the restore-time disk
    /// preflight of the whole snapshot.
    pub fn uncompressed_size(&self) -> u64 {
        self.descriptors()
            .map(|layer| layer.uncompressed_size)
            .sum()
    }
}

/// The layer set of `inscriptions`, ascending, refusing a `(kind, scope)`
/// pair listed twice in it.
fn layer_set(inscriptions: &[Inscription]) -> Result<Vec<ChainLayer>, Error> {
    let head = &inscriptions[inscriptions.len() - 1];

    let listed = inscriptions
        .iter()
        .flat_map(|inscription| {
            inscription
                .diff
                .iter()
                .map(move |layer| (layer, inscription.sequence, Array::Diff))
        })
        .chain(
            head.tip
                .iter()
                .map(|layer| (layer, head.sequence, Array::Tip)),
        );

    let mut first: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut layers = Vec::new();

    for (descriptor, sequence, array) in listed {
        let scope = String::from_utf8(canonical_json(&descriptor.scope)?)
            .map_err(|e| Error::Canonicalization(e.to_string()))?;

        if let Some(earlier) = first.insert((descriptor.kind.clone(), scope.clone()), sequence) {
            return Err(Error::DuplicateInChain {
                kind: descriptor.kind.clone(),
                scope,
                first: earlier,
                again: sequence,
            });
        }

        layers.push(ChainLayer {
            descriptor: descriptor.clone(),
            sequence,
            array,
        });
    }

    Ok(layers)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::{inscription::link_for, Compression};

    struct Toy;

    impl Profile for Toy {
        fn name(&self) -> &str {
            "dev.example.toy"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes", "index"]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.example.stelae.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("chapter-{sequence}"))
        }
    }

    fn layer(kind: &str, chapter: u64, byte: u8) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: format!("application/vnd.example.stelae.{kind}.v1+zstd"),
            diff_id: Digest::from_bytes([byte; 32]),
            records: 2,
            uncompressed_size: 100 + chapter,
            scope: json!({"chapter": chapter}),
        }
    }

    /// Chapter `sequence`, linked to `previous`: its notes in `diff` and its
    /// index in `tip`, with parameters of its own.
    fn chapter(sequence: u64, previous: Option<&Inscription>) -> Inscription {
        let mut inscription = Inscription::new(
            &Toy,
            sequence,
            json!({"chapter": sequence}),
            json!({"noteWidth": 40 + sequence}),
            Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
        );

        inscription.previous = link_for(previous, sequence).unwrap();
        inscription.diff = vec![layer("notes", sequence, sequence as u8)];
        inscription.tip = vec![layer("index", sequence, 0x80 + sequence as u8)];
        inscription
    }

    /// Chapters `first..=last`, each linked to the one before.
    fn lineage(first: u64, last: u64) -> Vec<Inscription> {
        let mut chapters: Vec<Inscription> = Vec::new();

        for sequence in first..=last {
            let next = chapter(sequence, chapters.last());
            chapters.push(next);
        }

        chapters
    }

    /// What a transport holding `chapters` answers, by sequence.
    fn holding(chapters: &[Inscription]) -> BTreeMap<u64, Inscription> {
        chapters
            .iter()
            .map(|inscription| (inscription.sequence, inscription.clone()))
            .collect()
    }

    fn walk(head: &Inscription, held: &BTreeMap<u64, Inscription>) -> Result<Chain, Error> {
        Chain::walk(&Toy, head.clone(), |link| {
            Ok(held.get(&link.sequence).cloned())
        })
    }

    #[test]
    fn the_layer_set_is_every_diff_then_the_head_tip() {
        let chapters = lineage(1, 5);
        let held = holding(&chapters);

        let chain = walk(&chapters[4], &held).unwrap();

        assert_eq!(chain.inscriptions(), &chapters[..]);
        assert_eq!(chain.head(), &chapters[4]);
        assert_eq!(chain.first(), &chapters[0]);
        assert_eq!(chain.identity(), chapters[4].digest().unwrap());

        let expected: Vec<(LayerDescriptor, u64, Array)> = chapters
            .iter()
            .map(|c| (c.diff[0].clone(), c.sequence, Array::Diff))
            .chain([(chapters[4].tip[0].clone(), 5, Array::Tip)])
            .collect();
        let found: Vec<(LayerDescriptor, u64, Array)> = chain
            .layers()
            .iter()
            .map(|l| (l.descriptor.clone(), l.sequence, l.array))
            .collect();
        assert_eq!(found, expected);

        // Each layer reaches the inscription that lists it, and with it that
        // inscription's own parameters.
        for layer in chain.layers() {
            let origin = chain.origin(layer).unwrap();
            assert_eq!(origin.sequence, layer.sequence);
            assert_eq!(origin.parameters, json!({"noteWidth": 40 + layer.sequence}));
        }

        assert_eq!(
            chain.uncompressed_size(),
            (1..=5).map(|c| 100 + c).sum::<u64>() + 105
        );
        assert!(chain.inscription(6).is_none());
    }

    /// A lineage may start at any sequence: the walk ends at the first
    /// inscription without a link, and never asks for one below it.
    #[test]
    fn a_lineage_that_starts_late_ends_at_its_first_inscription() {
        let chapters = lineage(100, 104);
        let held = holding(&chapters);
        let mut asked = Vec::new();

        let chain = Chain::walk(&Toy, chapters[4].clone(), |link| {
            asked.push(link.sequence);
            Ok(held.get(&link.sequence).cloned())
        })
        .unwrap();

        assert_eq!(asked, vec![103, 102, 101, 100]);
        assert_eq!(chain.first().sequence, 100);
        assert_eq!(chain.layers().len(), 6);

        // A lineage of one is its own chain.
        let alone = walk(&chapters[0], &BTreeMap::new()).unwrap();
        assert_eq!(alone.inscriptions(), &chapters[..1]);
        assert_eq!(alone.layers().len(), 2);
    }

    #[test]
    fn a_tampered_digest_is_refused() {
        let chapters = lineage(1, 5);
        let mut held = holding(&chapters);

        held.get_mut(&3).unwrap().position = json!({"chapter": 3, "edited": true});

        let err = walk(&chapters[4], &held).unwrap_err();
        assert!(
            matches!(&err, Error::DigestMismatch { subject, expected, .. }
                if subject.contains("sequence 3, linked from sequence 4")
                    && *expected == chapters[2].digest().unwrap().to_string()),
            "{err:?}"
        );
    }

    /// A transport that answers a link with a document the chain already holds
    /// is refused, rather than walked round again.
    #[test]
    fn a_loop_is_refused() {
        let chapters = lineage(1, 5);
        let mut held = holding(&chapters);

        held.insert(2, chapters[4].clone());

        let err = walk(&chapters[4], &held).unwrap_err();
        assert!(
            matches!(&err, Error::LinkInvariant(why) if why.contains("loop")
                && why.contains("sequence 3 links to sequence 2")),
            "{err:?}"
        );
    }

    /// A link whose digest names an inscription at another sequence, or a
    /// head whose own link names a sequence not below it, is refused.
    #[test]
    fn a_sequence_that_does_not_descend_is_refused() {
        let later = chapter(7, None);

        let mut head = chapter(6, None);
        head.previous = Some(Link {
            sequence: 4,
            inscription_digest: later.digest().unwrap(),
        });

        let err = Chain::walk(&Toy, head, |_| Ok(Some(later.clone()))).unwrap_err();
        assert!(
            matches!(&err, Error::LinkInvariant(why) if why.contains("is at sequence 7")),
            "{err:?}"
        );

        let mut upward = chapter(6, None);
        upward.previous = Some(Link {
            sequence: 6,
            inscription_digest: later.digest().unwrap(),
        });

        let err =
            Chain::walk(&Toy, upward, |_| panic!("asked to follow an upward link")).unwrap_err();
        assert!(
            matches!(&err, Error::LinkInvariant(why) if why.contains("not below")),
            "{err:?}"
        );
    }

    /// A document missing inside the chain is its own error, and not absence.
    #[test]
    fn a_missing_link_names_both_sequences() {
        let chapters = lineage(1, 5);
        let mut held = holding(&chapters);

        held.remove(&2);

        let err = walk(&chapters[4], &held).unwrap_err();
        assert!(
            matches!(
                err,
                Error::MissingLink {
                    from: 3,
                    sequence: 2
                }
            ),
            "{err:?}"
        );
        assert!(!err.is_absent());
    }

    #[test]
    fn a_link_to_another_profile_is_refused() {
        let mut other = chapter(1, None);
        other.profile.name = "com.acme.other".to_owned();

        let head = chapter(2, Some(&other));

        let err = Chain::walk(&Toy, head, |_| Ok(Some(other.clone()))).unwrap_err();
        assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");
    }

    /// A kind and scope listed by two diffs, or by a diff and the head's tip,
    /// is refused, naming both sequences.
    #[test]
    fn a_kind_and_scope_twice_in_the_layer_set_is_refused() {
        let mut chapters = lineage(1, 3);
        chapters[1].diff.push(layer("notes", 1, 0x41));
        chapters[2] = chapter(3, Some(&chapters[1]));
        let held = holding(&chapters);

        let err = walk(&chapters[2], &held).unwrap_err();
        assert!(
            matches!(&err, Error::DuplicateInChain { kind, first: 1, again: 2, .. } if kind == "notes"),
            "{err:?}"
        );

        let mut chapters = lineage(1, 3);
        chapters[2].tip = vec![layer("notes", 1, 0x42)];
        let held = holding(&chapters);

        let err = walk(&chapters[2], &held).unwrap_err();
        assert!(
            matches!(
                &err,
                Error::DuplicateInChain {
                    first: 1,
                    again: 3,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    /// An earlier tip is not part of the set, so a layer it carried may appear
    /// later in a diff, settled.
    #[test]
    fn an_earlier_tip_is_not_part_of_the_layer_set() {
        let mut chapters = lineage(1, 3);
        chapters[2].diff.push(layer("index", 1, 0x43));
        let held = holding(&chapters);

        let chain = walk(&chapters[2], &held).unwrap();

        let index: Vec<(u64, Array)> = chain
            .layers()
            .iter()
            .filter(|l| l.descriptor.kind == "index")
            .map(|l| (l.sequence, l.array))
            .collect();
        assert_eq!(index, vec![(3, Array::Diff), (3, Array::Tip)]);
    }
}
