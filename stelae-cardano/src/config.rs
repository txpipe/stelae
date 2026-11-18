//! What the publisher reads before it opens a store.
//!
//! The node's lasting description is Dolos's own configuration file, read by
//! Dolos's own loader. What Dolos takes from an invocation instead — the
//! Mithril aggregator to trust — arrives here as values and is resolved the
//! way Dolos resolves it.

use std::path::Path;

use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::{
    config::{ConfigError, HistoryMode, RootConfig},
    ChainError, Genesis,
};
use dolos_mithril::MithrilSource;

/// Load the configuration as Dolos does — `path`, else `./dolos.toml`, with
/// `DOLOS_` and `__` environment overrides — and refuse a network name the
/// catalog does not know.
// Dolos's own error, returned as its loader returns it.
#[allow(clippy::result_large_err)]
pub fn load(path: Option<&Path>) -> Result<RootConfig, ConfigError> {
    dolos_core::config::load_checked(path, |config| {
        ResolvedNetwork::resolve(&config.chain)
            .map(|_| ())
            .map_err(|error| ConfigError::invalid(["chain.network"], error.to_string()))
    })
    .map_err(|error| match error {
        ConfigError::MovedSections(moved) => {
            ConfigError::MovedSections(moved.into_iter().map(publisher_destination).collect())
        }
        error => error,
    })
}

/// Where the publisher takes each section Dolos moved out of the file. A
/// section Dolos removed outright keeps Dolos's words.
const MOVED_TO_ARGUMENTS: [(&str, &str); 2] = [
    (
        "mithril",
        "`--mithril-aggregator` / `DOLOS_MITHRIL_AGGREGATOR` and `--mithril-genesis-vkey` / \
         `DOLOS_MITHRIL_GENESIS_VKEY`",
    ),
    (
        "stelae",
        "`--registry-user` / `DOLOS_STELAE_USER`, `DOLOS_STELAE_PASSWORD`, `DOLOS_STELAE_TOKEN` \
         and `--registry-credentials-file`",
    ),
];

/// Rewrites Dolos's remedy, which names `dolos` commands, as this
/// executable's; a section it does not know keeps Dolos's text.
fn publisher_destination(moved: String) -> String {
    MOVED_TO_ARGUMENTS
        .iter()
        .find(|(section, _)| moved.starts_with(&format!("[{section}]")))
        .map(|(section, destination)| format!("[{section}] moved to {destination}"))
        .unwrap_or(moved)
}

/// The chain's genesis: embedded for a known network, read from the
/// configured files and checked against the magic for a custom one.
pub fn genesis(config: &RootConfig) -> Result<Genesis, ChainError> {
    ResolvedNetwork::resolve(&config.chain)?.genesis()
}

/// A publisher configured to keep less than the full archive.
#[derive(Debug, thiserror::Error)]
#[error(
    "a stele publisher needs `history.mode = \"full\"`, but this configuration keeps {kept}: \
     Dolos publishes only from an archive that starts at the origin, so the first prune \
     would stall the lineage"
)]
pub struct PartialHistory {
    kept: &'static str,
}

/// Refuse a configuration whose archive does not keep every block.
///
/// Dolos refuses to cut a boundary from an archive whose first block is past
/// the origin, so a rolling window publishes until its first prune and then
/// fails every later publish.
pub fn require_full_history(config: &RootConfig) -> Result<(), PartialHistory> {
    match config.history.mode {
        HistoryMode::Full => Ok(()),
        HistoryMode::Rolling => Err(PartialHistory {
            kept: "a rolling window",
        }),
        HistoryMode::None => Err(PartialHistory { kept: "no archive" }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error(
        "a custom network has no default Mithril {what}; pass --mithril-{flag} (or set {env})"
    )]
    MissingMithril {
        what: &'static str,
        flag: &'static str,
        env: &'static str,
    },
}

/// The aggregator the invocation names, else the known network's.
pub fn mithril_aggregator(
    config: &RootConfig,
    aggregator: Option<String>,
) -> Result<String, InputError> {
    aggregator
        .or_else(|| {
            let network = ResolvedNetwork::resolve(&config.chain).ok()?;
            Some(network.known()?.mithril.aggregator.to_owned())
        })
        .ok_or(InputError::MissingMithril {
            what: "aggregator",
            flag: "aggregator",
            env: "DOLOS_MITHRIL_AGGREGATOR",
        })
}

/// The aggregator a backfill trusts: what the invocation names, else the
/// known network's.
pub fn mithril_source(
    config: &RootConfig,
    aggregator: Option<String>,
    genesis_vkey: Option<String>,
) -> Result<MithrilSource, InputError> {
    let catalog = ResolvedNetwork::resolve(&config.chain)
        .ok()
        .and_then(|network| network.known())
        .map(|network| &network.mithril);

    let aggregator = mithril_aggregator(config, aggregator)?;

    let genesis_vkey = genesis_vkey
        .or_else(|| catalog.map(|mithril| mithril.genesis_vkey.to_owned()))
        .ok_or(InputError::MissingMithril {
            what: "genesis key",
            flag: "genesis-vkey",
            env: "DOLOS_MITHRIL_GENESIS_VKEY",
        })?;

    Ok(MithrilSource {
        aggregator,
        genesis_vkey,
    })
}

#[cfg(test)]
mod tests {
    use dolos_cardano::networks::KnownNetwork;

    use super::*;

    fn config(document: &str) -> (tempfile::TempDir, Result<RootConfig, ConfigError>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");
        std::fs::write(&path, document).unwrap();
        let loaded = load(Some(&path));
        (dir, loaded)
    }

    #[test]
    fn a_known_network_loads_with_its_embedded_genesis() {
        let (_dir, loaded) = config(
            "compatibility = \"v2-alpha.2\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n",
        );
        let config = loaded.unwrap();
        assert_eq!(genesis(&config).unwrap().network_magic(), 2);
    }

    #[test]
    fn only_full_history_publishes() {
        let (_dir, loaded) = config(
            "compatibility = \"v2-alpha.2\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n",
        );
        require_full_history(&loaded.unwrap()).unwrap();

        for (history, kept) in [
            (
                "mode = \"rolling\"\nwindow = { unit = \"slot\", value = 432000 }",
                "a rolling window",
            ),
            ("mode = \"none\"", "no archive"),
        ] {
            let (_dir, loaded) = config(&format!(
                "compatibility = \"v2-alpha.2\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n[history]\n{history}\n"
            ));
            let error = require_full_history(&loaded.unwrap())
                .unwrap_err()
                .to_string();
            assert!(error.contains(kept), "{error}");
        }
    }

    #[test]
    fn an_unknown_network_is_refused_at_load() {
        let (_dir, loaded) = config(
            "compatibility = \"v2-alpha.2\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"nowhere\"\n",
        );
        let error = loaded.unwrap_err().to_string();
        assert!(error.contains("unknown network `nowhere`"), "{error}");
    }

    #[test]
    fn a_moved_section_names_the_publisher_input() {
        let (_dir, loaded) = config(
            "compatibility = \"v2-alpha.2\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n[snapshot]\nstate_epochs = [1]\n[mithril]\naggregator = \"http://aggregator\"\n[stelae]\nuser = \"publisher\"\n",
        );
        let error = loaded.unwrap_err().to_string();
        assert!(error.contains("[snapshot] is gone"), "{error}");
        assert!(error.contains("--mithril-aggregator"), "{error}");
        for input in [
            "--registry-user",
            "DOLOS_STELAE_USER",
            "DOLOS_STELAE_PASSWORD",
            "DOLOS_STELAE_TOKEN",
            "--registry-credentials-file",
        ] {
            assert!(error.contains(input), "{input}: {error}");
        }
        assert!(!error.contains("dolos "), "{error}");
    }

    #[test]
    fn the_image_smoke_inputs_load() {
        let image = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/image");
        let config = load(Some(&image.join("preview-smoke.toml"))).unwrap();
        assert_eq!(genesis(&config).unwrap().network_magic(), 2);
    }

    #[test]
    fn mithril_defaults_from_the_catalog_and_the_invocation_wins() {
        let config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        let preview = KnownNetwork::by_name("preview").unwrap();

        let defaulted = mithril_source(&config, None, None).unwrap();
        assert_eq!(defaulted.aggregator, preview.mithril.aggregator);
        assert_eq!(defaulted.genesis_vkey, preview.mithril.genesis_vkey);

        let named = mithril_source(&config, Some("http://mirror".to_owned()), None).unwrap();
        assert_eq!(named.aggregator, "http://mirror");
        assert_eq!(named.genesis_vkey, preview.mithril.genesis_vkey);
    }

    #[test]
    fn a_custom_network_names_its_mithril_inputs() {
        let mut config = RootConfig::new(dolos_core::config::ChainConfig::known("preview"));
        config.chain.network = dolos_core::config::ChainConfig::CUSTOM.to_owned();

        let error = mithril_source(&config, None, None).unwrap_err().to_string();
        assert!(error.contains("--mithril-aggregator"), "{error}");

        let error = mithril_source(&config, Some("http://aggregator".to_owned()), None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("--mithril-genesis-vkey"), "{error}");
    }
}
