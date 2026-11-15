//! What the lifecycle has to ask a profile.

use stelae::inscription::{Inscription, LayerDescriptor};

use crate::Error;

/// The array of its inscription a layer is listed in.
///
/// `Diff` is a layer whose content is final: every later snapshot of the
/// lineage inherits it, and no later inscription lists it again. `Tip` is a
/// layer that belongs to the snapshot being written alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Diff,
    Tip,
}

/// The questions the publish and restore lifecycle asks of a profile.
///
/// [`stelae::Profile`] answers what the *protocol* needs — naming, kinds, tags,
/// the record ceiling — and deliberately has no hook for anything
/// dataset-shaped. The lifecycle needs a little more than that and strictly
/// less than a dataset: which kinds an epoch produces, which kinds carry the
/// state, which array a layer is listed in, and whether two documents describe
/// the same dataset at all. None of those answers reaches into a store, a chain
/// or a node, which is why they can live on a companion trait here rather than
/// growing the protocol's.
///
/// Everything crossing this boundary in either direction is opaque: a `scope`
/// and a `position` are [`serde_json::Value`], composed by the profile and
/// never composed here. The driver reads a document's shape only through the
/// implementor.
pub trait DriverProfile: stelae::Profile {
    /// The kinds a closed window always produces a layer for, plus the sparse
    /// ones it may.
    fn epoch_kinds(&self) -> &[&str];

    /// The subset of [`DriverProfile::epoch_kinds`] a window produces
    /// unconditionally.
    ///
    /// Split from the whole because the layer arithmetic is made of the two
    /// arities: the dense kinds multiply out by the number of windows, and the
    /// sparse ones have to be counted against the data.
    fn dense_epoch_kinds(&self) -> &[&str];

    /// Whether `kind` carries the dataset's state rather than one window of its
    /// history.
    ///
    /// Asked for one reason: the staging arithmetic sizes the two halves of a
    /// snapshot differently. State is rewritten whole by every publish, so all
    /// of it is staged together and every such layer sums; anything else is
    /// staged a few at a time and only the largest few count.
    /// [`DriverProfile::role`] cannot stand in for this: a tip may hold layers
    /// that are not state.
    fn is_state_kind(&self, kind: &str) -> bool;

    /// The array a layer of `kind` at `scope` is listed in by the snapshot at
    /// `sequence`, asked of a layer about to be written.
    ///
    /// A layer goes to [`Role::Diff`] once its content is final, and until then
    /// each snapshot carries its current form in [`Role::Tip`]. So the answer
    /// may depend on the scope and the sequence as well as the kind.
    fn role(&self, kind: &str, scope: &serde_json::Value, sequence: u64) -> Result<Role, Error>;

    /// Refuse a predecessor that describes a different dataset than the one
    /// being published.
    ///
    /// `previous` is the snapshot being chained to; `position` is the document
    /// the new snapshot will carry. Both halves are the profile's own
    /// shape, so what "the same dataset" means is the profile's to decide —
    /// the driver only knows that a repository holding two of them is a
    /// fault, and refuses before anything is built.
    fn check_same_dataset(
        &self,
        previous: &Inscription,
        position: &serde_json::Value,
    ) -> Result<(), Error>;
}

/// Put a snapshot's layers into its inscription, each in the array
/// [`DriverProfile::role`] names at the inscription's sequence, each array in
/// the order given.
///
/// A layer the profile cannot place fails the whole placement and leaves the
/// inscription's arrays as they were.
pub fn place_layers(
    profile: &dyn DriverProfile,
    inscription: &mut Inscription,
    layers: impl IntoIterator<Item = LayerDescriptor>,
) -> Result<(), Error> {
    let mut diff = Vec::new();
    let mut tip = Vec::new();

    for layer in layers {
        match profile.role(&layer.kind, &layer.scope, inscription.sequence)? {
            Role::Diff => diff.push(layer),
            Role::Tip => tip.push(layer),
        }
    }

    inscription.diff = diff;
    inscription.tip = tip;

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stelae::inscription::Compression;

    use super::*;

    /// A profile whose `state` kind is its tip, and whose `notes` for a shard
    /// stay in the tip until a sequence after the shard's own.
    struct Shelf;

    impl stelae::Profile for Shelf {
        fn name(&self) -> &str {
            "dev.example.shelf"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes", "state"]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
            Ok(format!("application/vnd.example.stelae.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
            Ok(format!("shelf-{sequence}"))
        }
    }

    impl DriverProfile for Shelf {
        fn epoch_kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn dense_epoch_kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn is_state_kind(&self, kind: &str) -> bool {
            kind == "state"
        }

        fn role(
            &self,
            kind: &str,
            scope: &serde_json::Value,
            sequence: u64,
        ) -> Result<Role, Error> {
            match (kind, scope["shard"].as_u64()) {
                ("state", _) => Ok(Role::Tip),
                ("notes", Some(shard)) if shard < sequence => Ok(Role::Diff),
                ("notes", Some(_)) => Ok(Role::Tip),
                _ => Err(Error::Unplaced {
                    kind: kind.to_owned(),
                    reason: "its scope names no shard".to_owned(),
                }),
            }
        }

        fn check_same_dataset(
            &self,
            _previous: &Inscription,
            _position: &serde_json::Value,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn layer(kind: &str, shard: u64) -> LayerDescriptor {
        LayerDescriptor {
            kind: kind.to_owned(),
            media_type: format!("application/vnd.example.stelae.{kind}.v1+zstd"),
            diff_id: stelae::Digest::compute(format!("{kind}-{shard}")),
            records: 1,
            uncompressed_size: 1,
            scope: json!({"shard": shard}),
        }
    }

    fn inscription(sequence: u64) -> Inscription {
        Inscription::new(
            &Shelf,
            sequence,
            json!({}),
            json!({}),
            Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
        )
    }

    /// The role decides the array at the inscription's own sequence, so one
    /// kind lands in either array depending on its scope.
    #[test]
    fn each_layer_goes_to_the_array_its_role_names_in_order() {
        let mut inscription = inscription(7);

        place_layers(
            &Shelf,
            &mut inscription,
            [
                layer("notes", 5),
                layer("state", 0),
                layer("notes", 7),
                layer("notes", 6),
                layer("state", 1),
            ],
        )
        .unwrap();

        assert_eq!(inscription.diff, vec![layer("notes", 5), layer("notes", 6)]);
        assert_eq!(
            inscription.tip,
            vec![layer("state", 0), layer("notes", 7), layer("state", 1)]
        );
        inscription.validate().unwrap();
    }

    #[test]
    fn a_layer_the_profile_cannot_place_is_refused() {
        let mut inscription = inscription(7);

        let mut unscoped = layer("notes", 0);
        unscoped.scope = json!({});

        let err =
            place_layers(&Shelf, &mut inscription, [layer("notes", 1), unscoped]).unwrap_err();

        assert!(matches!(err, Error::Unplaced { .. }), "{err:?}");
        assert!(inscription.diff.is_empty());
        assert!(inscription.tip.is_empty());
    }

    /// A placement replaces what the inscription listed, and a refused one
    /// leaves it as it was.
    #[test]
    fn a_placement_replaces_the_arrays_and_a_refused_one_keeps_them() {
        let mut inscription = inscription(7);
        inscription.diff = vec![layer("notes", 1)];
        inscription.tip = vec![layer("state", 9)];

        place_layers(
            &Shelf,
            &mut inscription,
            [layer("notes", 2), layer("state", 0)],
        )
        .unwrap();

        assert_eq!(inscription.diff, vec![layer("notes", 2)]);
        assert_eq!(inscription.tip, vec![layer("state", 0)]);

        let mut unscoped = layer("notes", 0);
        unscoped.scope = json!({});

        place_layers(&Shelf, &mut inscription, [layer("notes", 3), unscoped]).unwrap_err();

        assert_eq!(inscription.diff, vec![layer("notes", 2)]);
        assert_eq!(inscription.tip, vec![layer("state", 0)]);
    }
}
