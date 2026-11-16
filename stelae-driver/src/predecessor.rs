//! The publish a publish follows: the link to it, and what an interrupted
//! attempt of this publish already uploaded.
//!
//! The seam an export walks against: one trait, and the implementation that
//! needs nothing but a document. The transport-shaped one — the publish this
//! one follows *in a repository* — is [`crate::publish::Chained`].

use stelae::inscription::{LayerDescriptor, Link};

use crate::Error;

/// The publish this one follows.
///
/// It supplies the link the new inscription carries, and nothing of the
/// predecessor's layers: a publish uploads and lists only the layers its
/// profile assigns to its own sequence, and the predecessor's stay in the
/// predecessor's inscription.
///
/// What it does hold is **this publish, interrupted**. A publish that never
/// got sealed left layers behind in the transport, and a restart may carry
/// those forward rather than build them again: [`Predecessor::landed`] notes
/// each layer as it lands, and [`Predecessor::resume`] answers for a layer a
/// note names. Where the notes are written is the implementor's.
///
/// `Sync`, because an export drives its layer producers from a pool of
/// threads and each producer asks these questions for its own layers. An
/// implementor that keeps state — a resumption record, a counter — keeps it
/// behind its own synchronization.
pub trait Predecessor: Sync {
    /// The link the new inscription carries: the snapshot it follows, or
    /// `None` where it starts a lineage.
    ///
    /// Making it is the implementor's business, and the protocol holds it to
    /// the invariant when the document is validated (`stelae::inscription`).
    fn previous(&self) -> Option<Link>;

    /// The descriptor of a layer of `kind` at `scope` that an interrupted
    /// attempt of this publish already uploaded, or `None` to build it.
    ///
    /// An implementation that answers `Some` **has already arranged for the
    /// transport to carry the layer's blob**; all that is left for an export
    /// is to not walk the store. That ordering is why this returns a descriptor
    /// rather than a boolean: an export that reused a descriptor whose blob
    /// nothing carried would publish a manifest with a hole in it.
    ///
    /// The default resumes nothing, which is what makes [`First`] one line.
    /// A registry carries a resumed blob with `Registry::adopt_carried`, and
    /// a directory with `SnapshotDir::adopt_carried`.
    fn resume(
        &self,
        kind: &str,
        scope: &serde_json::Value,
    ) -> Result<Option<LayerDescriptor>, Error> {
        let _ = (kind, scope);

        Ok(None)
    }

    /// Note that `descriptor`'s layer is in the transport and will be in the
    /// manifest, whether it was built here or resumed.
    ///
    /// Called once per layer, the moment it lands, so an implementor writing
    /// it down leaves a record that means "this layer is up" rather than "this
    /// layer was attempted" — the same boundary a restore's checkpoint records
    /// on its side. Layers land from concurrent producers, so calls
    /// interleave; a record is keyed by kind and scope, never by arrival
    /// order. Which layers are worth writing down is the implementor's: one
    /// that follows a profile notes only what [`crate::DriverProfile::role`]
    /// places in `diff`, since a tip describes the moment of the cut and a
    /// restart must rebuild it.
    ///
    /// A failure here **fails the publish**. Recording is not a courtesy: a
    /// record that silently stopped being written would cost the hours it
    /// exists to save, at the moment nobody is watching.
    ///
    /// The default does nothing, which is what a publish with no host behind it
    /// — a directory, a reproduction — wants.
    fn landed(&self, descriptor: &LayerDescriptor) -> Result<(), Error> {
        let _ = descriptor;

        Ok(())
    }
}

/// The first snapshot of a lineage: no link, nothing to resume.
///
/// The protocol lets a lineage start at any sequence, so this is not only the
/// very first publish — it is every publish into a directory, which has no way
/// to be asked what it already holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct First;

impl Predecessor for First {
    fn previous(&self) -> Option<Link> {
        None
    }
}
