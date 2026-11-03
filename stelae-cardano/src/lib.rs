//! The Cardano integration for the Stelae publisher.
//!
//! This crate owns application orchestration. It uses Dolos's supported
//! headless engine and snapshot facades, but does not invoke a Dolos binary or
//! its legacy backfill driver. The protocol and generic lifecycle remain in
//! `stelae` and `stelae-driver`, which have no Cardano dependency.

pub mod backfill;
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
/// the Stelae snapshot envelope adoption (txpipe/dolos PR #1553). The v2
/// profile was frozen earlier, at e39a38a3 (PR #1548).
pub const DOLOS_REVISION: &str = "1db3e41fd04bca64bf245f50b54ca656719764ca";

#[cfg(test)]
mod dependency_identity {
    use super::*;

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
