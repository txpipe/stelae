//! The Cardano integration for the Stelae publisher.
//!
//! This crate owns application orchestration. It uses Dolos's supported
//! headless engine and snapshot facades, but does not invoke a Dolos binary or
//! its legacy backfill driver. The protocol and generic lifecycle remain in
//! `stelae` and `stelae-driver`, which have no Cardano dependency.

pub mod backfill;
pub mod batch;
pub mod config;
pub mod initialize;
pub mod publisher;
pub mod snapshot;
#[cfg(feature = "testing")]
pub mod testing;

pub use dolos::core::{config::RootConfig, Genesis};
pub use dolos_mithril::MithrilSource;
pub use dolos_snapshot::{
    facade::{Point, Repository, RestoreInput, Selection, SnapshotRepository, SnapshotSource},
    node::{registry_auth, RegistryAuth},
    planning::EpochRange,
    registry::Auth,
    restore::Source as RestoreSource,
    RetainedEpochs,
};

/// The Dolos revision this publisher reads its configuration and stores with:
/// the build of fjall from the txpipe fork (txpipe/dolos PR #1599), over the
/// boundary preparation facade of PR #1564. The v2 profile was frozen earlier,
/// at e39a38a3 (PR #1548).
pub const DOLOS_REVISION: &str = "a95ecdebe8666ddde29578d7a734af5ce8f16375";

#[cfg(test)]
mod dependency_identity {
    use super::*;

    /// `DOLOS_REVISION` is what published inscriptions and batch records
    /// name, so it has to be the revision every Dolos package resolves to.
    #[test]
    fn dolos_revision_is_the_locked_dolos_revision() {
        const SOURCE: &str = "git+https://github.com/txpipe/dolos?rev=";

        let lock = include_str!("../../Cargo.lock");
        let revisions: Vec<&str> = lock
            .lines()
            .filter_map(|line| line.split_once(SOURCE))
            .map(|(_, rest)| rest.split('#').next().unwrap())
            .collect();

        assert!(!revisions.is_empty());
        for revision in revisions {
            assert_eq!(revision, DOLOS_REVISION);
        }
    }

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
