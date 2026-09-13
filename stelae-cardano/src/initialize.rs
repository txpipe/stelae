//! Fail-closed cold-start initialization for the publisher host.

use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use dolos::engine::ReplayWorkspace;
use dolos_core::{seed_wal_from_state, Genesis, WalSeed};
use dolos_snapshot::{
    facade::{self, Point, RestoreInput, SnapshotRepository},
    node,
    registry::{self, Repository},
    restore::{Restoring, Source, Target},
};
use serde::{Deserialize, Serialize};
use stelae::progress::Observer;

const MARKER_FILE: &str = ".stelae-publisher-initialized.json";

type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct Options {
    pub source: Source,
    pub point: Point,
    pub insecure: bool,
    pub scratch_dir: Option<PathBuf>,
    pub skip_space_check: bool,
    pub resume: bool,
    pub allow_genesis: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Existing,
    Restored { sequence: u64 },
    Genesis,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Marker {
    schema: u8,
    storage_path: PathBuf,
    network_magic: u64,
    source: String,
    mode: String,
}

fn source_name(source: &Source) -> String {
    match source {
        Source::Dir(path) => format!("file://{}", path.display()),
        Source::Repo(repository) => repository.to_string(),
    }
}

fn validate_storage_path(path: &Path) -> Result<(), AnyError> {
    if !path.is_absolute() {
        return Err(format!("storage path {} is not absolute", path.display()).into());
    }
    if path.parent().is_none() || path.file_name().is_none() {
        return Err("refusing to initialize or clear a filesystem root".into());
    }
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(format!("storage path {} contains `..`", path.display()).into());
    }
    Ok(())
}

fn expected_marker(
    config: &crate::RootConfig,
    magic: u64,
    options: &Options,
    mode: &str,
) -> Marker {
    Marker {
        schema: 1,
        storage_path: config.storage.path.clone(),
        network_magic: magic,
        source: source_name(&options.source),
        mode: mode.to_owned(),
    }
}

fn write_marker(path: &Path, marker: &Marker) -> Result<(), AnyError> {
    let target = path.join(MARKER_FILE);
    let temporary = path.join(format!("{MARKER_FILE}.tmp"));
    let bytes = serde_json::to_vec_pretty(marker)?;
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

fn read_marker(path: &Path) -> Result<Option<Marker>, AnyError> {
    let path = path.join(MARKER_FILE);
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn close_restore_stores(
    archive: &dolos::adapters::ArchiveStoreBackend,
    state: &dolos::adapters::StateStoreBackend,
) -> Result<(), AnyError> {
    archive.shutdown()?;
    state.shutdown()?;
    Ok(())
}

fn seed_wal(config: &crate::RootConfig) -> Result<(), AnyError> {
    let state = dolos::storage::open_state_store(config)?;
    let wal: dolos::storage::WalStoreBackend<dolos_cardano::CardanoDelta> =
        dolos::storage::open_wal_store(config)?;
    match seed_wal_from_state(&state, &wal)? {
        WalSeed::NoCursor => return Err("restore completed without a state cursor".into()),
        WalSeed::Seeded(_) => {}
    }
    state.shutdown()?;
    wal.shutdown()?;
    Ok(())
}

fn restore(
    config: &crate::RootConfig,
    genesis: &Genesis,
    options: &Options,
    observer: &Observer,
) -> Result<u64, AnyError> {
    let archive = dolos::storage::open_archive_store(config)?;
    let state = dolos::storage::open_state_store(config)?;
    let restoring = Restoring {
        network_magic: u64::from(genesis.network_magic()),
        max_history: config.sync.max_history,
        storage_path: &config.storage.path,
        resume: options.resume,
        skip_space_check: options.skip_space_check,
    };
    let target = Target::new(&archive, &state);

    let outcome = match &options.source {
        Source::Dir(path) => facade::restore(
            RestoreInput::Directory(path),
            restoring,
            target,
            Some(observer),
        ),
        Source::Repo(repository) => {
            let auth = node::registry_auth(&config.stelae)?;
            let scratch = node::scratch_dir(&config.storage, options.scratch_dir.as_deref());
            let repository = SnapshotRepository::open(
                repository,
                options.insecure,
                auth,
                scratch,
                registry::Tuning::default(),
            )?;
            facade::restore(
                RestoreInput::Repository {
                    repository: &repository,
                    point: options.point,
                },
                restoring,
                target,
                Some(observer),
            )
        }
    };

    let close = close_restore_stores(&archive, &state);
    let outcome = outcome.map_err(|error| -> AnyError { Box::new(error) });
    let outcome = match (outcome, close) {
        (Ok(value), Ok(())) => value,
        (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
        (Err(operation), Err(close)) => {
            return Err(format!(
                "restore failed ({operation}); closing stores also failed ({close})"
            )
            .into())
        }
    };
    drop(archive);
    drop(state);
    seed_wal(config)?;

    // Prove the restored/reconciled stores can be opened through the engine
    // facade before the marker makes this initialization authoritative.
    ReplayWorkspace::open(config, Arc::new(genesis.clone()))?.finish()?;
    Ok(outcome.plan.sequence)
}

/// Initialize empty publisher storage from its own latest stele.
///
/// A failed restore clears partial stores and fails. Genesis is selected only
/// when the caller explicitly opts in. An existing initialized dataset is
/// never cleared or silently replaced.
pub fn run(
    config: &crate::RootConfig,
    genesis: &Genesis,
    options: &Options,
    observer: &Observer,
) -> Result<Outcome, AnyError> {
    validate_storage_path(&config.storage.path)?;
    std::fs::create_dir_all(&config.storage.path)?;

    if dolos::storage::has_existing_data(config)? {
        if let Some(marker) = read_marker(&config.storage.path)? {
            let expected = expected_marker(
                config,
                u64::from(genesis.network_magic()),
                options,
                marker.mode.as_str(),
            );
            if marker != expected || !matches!(marker.mode.as_str(), "restore" | "genesis") {
                return Err(format!(
                    "initialization marker does not match configured storage/source contract: {marker:?}"
                ).into());
            }
        }
        return Ok(Outcome::Existing);
    }

    if let Some(marker) = read_marker(&config.storage.path)? {
        let expected = expected_marker(
            config,
            u64::from(genesis.network_magic()),
            options,
            marker.mode.as_str(),
        );
        if marker == expected && marker.mode == "genesis" && options.allow_genesis {
            return Ok(Outcome::Genesis);
        }
        return Err("initialization marker exists but storage has no committed cursor".into());
    }

    match restore(config, genesis, options, observer) {
        Ok(sequence) => {
            let marker = expected_marker(
                config,
                u64::from(genesis.network_magic()),
                options,
                "restore",
            );
            write_marker(&config.storage.path, &marker)?;
            Ok(Outcome::Restored { sequence })
        }
        Err(error) => {
            dolos::storage::clear_storage(&config.storage.path)?;
            if !options.allow_genesis {
                return Err(format!(
                    "latest stele restore failed; refusing genesis fallback: {error}"
                )
                .into());
            }
            let marker = expected_marker(
                config,
                u64::from(genesis.network_magic()),
                options,
                "genesis",
            );
            write_marker(&config.storage.path, &marker)?;
            Ok(Outcome::Genesis)
        }
    }
}

/// Parse the source type without exposing Dolos's profile module to the host.
pub fn parse_source(raw: &str) -> Result<Source, String> {
    raw.parse()
}

/// Parse a repository using the wire-level `oci://` identity.
pub fn parse_repository(raw: &str) -> Result<Repository, String> {
    raw.parse()
        .map_err(|error: stelae::Error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_targets_must_be_absolute_and_below_a_root() {
        assert!(validate_storage_path(Path::new("relative/db")).is_err());
        assert!(validate_storage_path(Path::new("/")).is_err());
        assert!(validate_storage_path(Path::new("/data/../db")).is_err());
        assert!(validate_storage_path(Path::new("/data/db")).is_ok());
    }

    #[test]
    fn marker_roundtrip_preserves_the_storage_contract() {
        let dir = tempfile::tempdir().unwrap();
        let marker = Marker {
            schema: 1,
            storage_path: dir.path().join("db"),
            network_magic: 2,
            source: "oci://registry.invalid/cardano/preview".to_owned(),
            mode: "restore".to_owned(),
        };
        write_marker(dir.path(), &marker).unwrap();
        assert_eq!(read_marker(dir.path()).unwrap(), Some(marker));
    }
}
