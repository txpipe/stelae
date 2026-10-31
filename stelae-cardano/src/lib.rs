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

/// The Dolos revision this publisher reads its configuration and stores with.
pub const DOLOS_REVISION: &str = "e184a965644bcd7a4317097eda94206bd50a35d7";

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
