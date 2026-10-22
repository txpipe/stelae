//! What the publisher reads before it opens a store.
//!
//! The node's lasting description is Dolos's own configuration file, read by
//! Dolos's own loader. What Dolos takes from an invocation instead — the
//! Mithril aggregator to trust and the epochs a publisher retains state dumps
//! of — arrives here as values and is resolved the way Dolos resolves it.

use std::path::Path;

use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::{
    config::{ConfigError, RootConfig},
    ChainError, Genesis,
};
use dolos_mithril::MithrilSource;
use dolos_snapshot::{planning, RetainedEpochs};

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
}

/// The chain's genesis: embedded for a known network, read from the
/// configured files and checked against the magic for a custom one.
pub fn genesis(config: &RootConfig) -> Result<Genesis, ChainError> {
    ResolvedNetwork::resolve(&config.chain)?.genesis()
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
    #[error("reading the state epochs file {path}")]
    ReadStateEpochs {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("line {line} of the state epochs file {path} is not an epoch number: {text:?}")]
    MalformedStateEpoch {
        path: String,
        line: usize,
        text: String,
    },
    #[error("the state epochs file {path} is not a list of retained epochs")]
    InvalidStateEpochs {
        path: String,
        #[source]
        source: dolos_snapshot::Error,
    },
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

    let aggregator = aggregator
        .or_else(|| catalog.map(|mithril| mithril.aggregator.to_owned()))
        .ok_or(InputError::MissingMithril {
            what: "aggregator",
            flag: "aggregator",
            env: "DOLOS_MITHRIL_AGGREGATOR",
        })?;

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

/// The epochs this publisher retains a state dump of, one per line.
///
/// Signed input (decision 0026): the list is echoed into every inscription's
/// parameters, so it is validated before any store opens. Blank lines are
/// skipped; an empty file is a publisher that retains the tip alone.
pub fn read_state_epochs(path: &Path) -> Result<RetainedEpochs, InputError> {
    let name = || path.display().to_string();

    let text = std::fs::read_to_string(path).map_err(|source| InputError::ReadStateEpochs {
        path: name(),
        source,
    })?;

    let epochs =
        parse_state_epochs(&text).map_err(|(line, text)| InputError::MalformedStateEpoch {
            path: name(),
            line,
            text,
        })?;

    planning::retained_epochs(&epochs).map_err(|source| InputError::InvalidStateEpochs {
        path: name(),
        source,
    })
}

fn parse_state_epochs(text: &str) -> Result<Vec<u64>, (usize, String)> {
    text.lines()
        .enumerate()
        .map(|(index, line)| (index + 1, line.trim()))
        .filter(|(_, line)| !line.is_empty())
        .map(|(number, line)| line.parse().map_err(|_| (number, line.to_owned())))
        .collect()
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
            "compatibility = \"v2-alpha.1\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n",
        );
        let config = loaded.unwrap();
        assert_eq!(genesis(&config).unwrap().network_magic(), 2);
    }

    #[test]
    fn an_unknown_network_is_refused_at_load() {
        let (_dir, loaded) = config(
            "compatibility = \"v2-alpha.1\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"nowhere\"\n",
        );
        let error = loaded.unwrap_err().to_string();
        assert!(error.contains("unknown network `nowhere`"), "{error}");
    }

    #[test]
    fn the_image_smoke_inputs_load() {
        let image = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/image");
        let config = load(Some(&image.join("preview-smoke.toml"))).unwrap();
        assert_eq!(genesis(&config).unwrap().network_magic(), 2);
        let retained = read_state_epochs(&image.join("preview-smoke.state-epochs")).unwrap();
        assert_eq!(retained.as_slice(), &[1]);
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

    #[test]
    fn state_epochs_are_one_per_line_and_validated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state-epochs");

        std::fs::write(&path, "3\n200\n\n646\n").unwrap();
        assert_eq!(read_state_epochs(&path).unwrap().as_slice(), &[3, 200, 646]);

        std::fs::write(&path, "").unwrap();
        assert!(read_state_epochs(&path).unwrap().as_slice().is_empty());

        std::fs::write(&path, "3\nfour\n").unwrap();
        let error = read_state_epochs(&path).unwrap_err().to_string();
        assert!(error.contains("line 2"), "{error}");

        std::fs::write(&path, "200\n3\n").unwrap();
        assert!(matches!(
            read_state_epochs(&path),
            Err(InputError::InvalidStateEpochs { .. })
        ));

        let missing = dir.path().join("missing");
        assert!(matches!(
            read_state_epochs(&missing),
            Err(InputError::ReadStateEpochs { .. })
        ));
    }
}
