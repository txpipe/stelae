//! The Cardano integration for the Stelae publisher.
//!
//! This crate owns application orchestration. It uses Dolos's supported
//! headless engine and snapshot facades, but does not invoke a Dolos binary or
//! its legacy backfill driver. The protocol and generic lifecycle remain in
//! `stelae` and `stelae-driver`, which have no Cardano dependency.

pub mod backfill;
pub mod batch;
pub mod blocks;
pub mod config;
pub mod initialize;
pub mod publisher;
pub mod rebuild;
pub mod snapshot;
#[cfg(feature = "testing")]
pub mod testing;

pub use dolos::core::{config::RootConfig, Genesis};
pub use dolos_mithril::MithrilSource;
pub use dolos_snapshot::{
    facade::{Point, Repository, RestoreInput, Selection, SnapshotRepository, SnapshotSource},
    node::{registry_auth, RegistryAuth},
    registry::Auth,
    restore::Source as RestoreSource,
};

/// The Dolos revision this publisher reads its configuration and stores with:
/// the profile that exports the diff chain and names its blocks snapshot
/// (txpipe/dolos PR #1610 through PR #1625), under the `v2-beta.1` store
/// contract (PR #1646).
pub const DOLOS_REVISION: &str = "c31f215cfcc48e5a1f57176914eefd932af273ee";

#[cfg(test)]
mod dependency_identity {
    use super::*;

    /// `DOLOS_REVISION` is what published inscriptions and batch records
    /// name, so it has to be the revision every Dolos package resolves to.
    #[test]
    fn dolos_revision_is_the_locked_dolos_revision() {
        const SOURCE: &str = "source = \"git+https://github.com/txpipe/dolos";

        // Every Dolos git source, however it is requested, by the commit it
        // resolved to: the fragment after `#`.
        let lock = include_str!("../../Cargo.lock");
        let revisions: Vec<&str> = lock
            .lines()
            .filter(|line| line.starts_with(SOURCE))
            .map(|line| {
                let (_, resolved) = line.rsplit_once('#').expect("a resolved git source");
                resolved.trim_end_matches('"')
            })
            .collect();

        assert!(!revisions.is_empty());
        for revision in revisions {
            assert_eq!(revision, DOLOS_REVISION);
        }
    }

    // The workspace's `stelae` and `stelae-driver` must be the ones the Dolos
    // profile was built against, or these would not compile.
    fn accepts_host_profile<P: stelae_driver::DriverProfile>() {}

    #[test]
    fn dolos_profile_implements_the_workspace_driver_trait() {
        accepts_host_profile::<dolos_snapshot::DolosProfile>();
    }

    #[test]
    fn facade_registry_types_are_the_protocol_types_used_by_this_workspace() {
        fn accepts_repository(_: &stelae::oci::Repository) {}
        let repository: Repository = "oci://example.invalid/cardano/preview".parse().unwrap();
        accepts_repository(&repository);
    }
}
