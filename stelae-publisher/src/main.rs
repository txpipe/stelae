use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use clap::{Args, Parser, Subcommand};
use miette::{Context as _, IntoDiagnostic as _};
use stelae::progress::{Event, Observer, Progress};
use stelae_cardano::{
    backfill, config, initialize,
    publisher::{self, Destination, PublishOutcome},
    Auth, EpochRange, Genesis, Point, RegistryAuth, Repository, RestoreSource, RetainedEpochs,
    RootConfig, Selection,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(name = "stelae-publisher", version, about)]
struct Cli {
    /// The Dolos configuration file; defaults to ./dolos.toml.
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,
    #[command(flatten)]
    credentials: CredentialArgs,
    #[command(subcommand)]
    command: Command,
}

/// The registry identity, from the invocation and never from dolos.toml.
///
/// The names are Dolos's own: `--registry-user` / `DOLOS_STELAE_USER`, and the
/// secret from `DOLOS_STELAE_PASSWORD` or `DOLOS_STELAE_TOKEN`, never an
/// argument, where it would land in process listings.
#[derive(Debug, Args, Clone)]
struct CredentialArgs {
    /// The user to authenticate to the registry as.
    #[arg(long, global = true, env = "DOLOS_STELAE_USER", value_name = "USER")]
    registry_user: Option<String>,
}

const PASSWORD_ENV: &str = "DOLOS_STELAE_PASSWORD";
const TOKEN_ENV: &str = "DOLOS_STELAE_TOKEN";

impl CredentialArgs {
    fn auth(&self) -> miette::Result<Auth> {
        self.auth_from(|name| std::env::var(name).ok())
    }

    fn auth_from(&self, env: impl Fn(&str) -> Option<String>) -> miette::Result<Auth> {
        // An empty value is no secret, as in Dolos.
        let secret = |name| env(name).filter(|value: &String| !value.is_empty());
        let inputs = RegistryAuth::from_inputs(
            self.registry_user.clone().filter(|user| !user.is_empty()),
            secret(PASSWORD_ENV),
            secret(TOKEN_ENV),
        );
        stelae_cardano::registry_auth(&inputs).into_diagnostic()
    }
}

/// The epochs this publisher retains a state dump of: signed input, so every
/// command that plans a publish requires it.
#[derive(Debug, Args, Clone)]
struct StateEpochsArgs {
    /// File listing the retained state-dump epochs, one per line, ascending.
    #[arg(long, value_name = "PATH")]
    state_epochs_file: PathBuf,
}

impl StateEpochsArgs {
    fn read(&self) -> miette::Result<RetainedEpochs> {
        config::read_state_epochs(&self.state_epochs_file).into_diagnostic()
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize empty stores from a stele, failing closed by default.
    Initialize(InitializeArgs),
    /// Publish the currently committed dataset once.
    Publish(PublishArgs),
    /// Replay Mithril history and publish every reached epoch boundary.
    Backfill(BackfillArgs),
    /// Initialize and then run the backfill state machine.
    Run(RunArgs),
}

#[derive(Debug, Args, Clone)]
struct InitializeArgs {
    /// Own stele to restore: file://DIR or oci://HOST/PATH.
    #[arg(long)]
    source: RestoreSource,
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    insecure: bool,
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
    #[arg(long, action)]
    skip_space_check: bool,
    /// Resume a prior restore using its layer journal.
    #[arg(long, alias = "continue", action)]
    resume: bool,
    /// Start empty only when restore fails; intended solely for first publish.
    #[arg(long, action)]
    allow_genesis: bool,
}

impl InitializeArgs {
    fn options(&self, auth: Auth) -> initialize::Options {
        initialize::Options {
            source: self.source.clone(),
            point: self.point,
            auth,
            insecure: self.insecure,
            scratch_dir: self.scratch_dir.clone(),
            skip_space_check: self.skip_space_check,
            resume: self.resume,
            allow_genesis: self.allow_genesis,
        }
    }
}

#[derive(Debug, Args, Clone)]
struct BackfillArgs {
    /// OCI repository to extend.
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,
    #[command(flatten)]
    state_epochs: StateEpochsArgs,
    /// The Mithril aggregator to replay from; defaults to the configured
    /// network's, and is required for a custom network.
    #[arg(long, env = "DOLOS_MITHRIL_AGGREGATOR", value_name = "URL")]
    mithril_aggregator: Option<String>,
    /// The aggregator's genesis verification key, JSON-hex encoded; defaults
    /// to the configured network's, and is required for a custom network.
    #[arg(long, env = "DOLOS_MITHRIL_GENESIS_VKEY", value_name = "KEY")]
    mithril_genesis_vkey: Option<String>,
    #[arg(long, action)]
    insecure: bool,
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
    #[arg(long)]
    concurrency: Option<NonZeroUsize>,
    #[arg(long, action)]
    verify_carried: bool,
    #[arg(long)]
    download_dir: Option<PathBuf>,
    #[arg(long, default_value = "40")]
    window: u64,
    #[arg(long)]
    until_epoch: Option<u64>,
    #[arg(long, action)]
    skip_validation: bool,
}

impl BackfillArgs {
    /// Every backfill input, resolved and validated before a store opens.
    fn options(&self, config: &RootConfig, auth: Auth) -> miette::Result<backfill::Options> {
        if self.window == 0 {
            return Err(miette::miette!("--window must be at least 1"));
        }
        let mithril = config::mithril_source(
            config,
            self.mithril_aggregator.clone(),
            self.mithril_genesis_vkey.clone(),
        )
        .into_diagnostic()?;
        Ok(backfill::Options {
            repository: self.repo.clone(),
            auth,
            insecure: self.insecure,
            scratch_dir: self.scratch_dir.clone(),
            concurrency: self.concurrency,
            verify_carried: self.verify_carried,
            download_dir: self.download_dir.clone(),
            window: self.window,
            until_epoch: self.until_epoch,
            skip_validation: self.skip_validation,
            mithril,
            retained: self.state_epochs.read()?,
        })
    }
}

#[derive(Debug, Args)]
struct RunArgs {
    #[command(flatten)]
    backfill: BackfillArgs,
    /// Restore source; defaults to --repo.
    #[arg(long)]
    restore_source: Option<RestoreSource>,
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    skip_space_check: bool,
    #[arg(long, action)]
    resume: bool,
    #[arg(long, action)]
    allow_genesis: bool,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("destination").required(true).args(["output_dir", "repo"])))]
struct PublishArgs {
    #[arg(long)]
    output_dir: Option<PathBuf>,
    #[arg(long, value_name = "OCI_URL")]
    repo: Option<Repository>,
    #[command(flatten)]
    state_epochs: StateEpochsArgs,
    #[arg(long)]
    epochs: Option<EpochRange>,
    #[arg(long)]
    index_band: Option<NonZeroUsize>,
    #[arg(long)]
    producers: Option<NonZeroUsize>,
    #[arg(long, action, conflicts_with = "output_dir")]
    rebuild: bool,
    #[arg(long, action, conflicts_with = "output_dir")]
    insecure: bool,
    #[arg(long, conflicts_with = "output_dir")]
    scratch_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "output_dir")]
    concurrency: Option<NonZeroUsize>,
    #[arg(long, action, conflicts_with = "output_dir")]
    verify_carried: bool,
    #[arg(long, action)]
    dry_run: bool,
    #[arg(long, action, conflicts_with = "output_dir")]
    require_new: bool,
}

#[derive(Default)]
struct ConsoleProgress;

impl Progress for ConsoleProgress {
    fn on(&self, event: Event<'_>) {
        match event {
            Event::LayerStarted {
                index, total, kind, ..
            } => {
                eprintln!("layer {}/{total}: {kind}", index + 1);
            }
            Event::Retry {
                attempt,
                remaining,
                reason,
            } => {
                eprintln!("retry after attempt {attempt} ({remaining} left): {reason}");
            }
            _ => {}
        }
    }
}

fn load_genesis(config: &RootConfig) -> miette::Result<Genesis> {
    config::genesis(config)
        .into_diagnostic()
        .context("loading genesis")
}

fn spawn_exit_watcher() -> miette::Result<CancellationToken> {
    let cancel = CancellationToken::new();
    let hooked = cancel.clone();
    std::thread::Builder::new()
        .name("exit-signal".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("building signal runtime");
            runtime.block_on(async move {
                #[cfg(unix)]
                {
                    let mut term =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("installing SIGTERM watcher");
                    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
                }
                #[cfg(not(unix))]
                tokio::signal::ctrl_c()
                    .await
                    .expect("installing interrupt watcher");
                tracing::warn!("shutdown requested; stopping at the next cancellation point");
                hooked.cancel();
            });
        })
        .into_diagnostic()
        .context("spawning exit-signal watcher")?;
    Ok(cancel)
}

fn run_initialize(
    config: &RootConfig,
    genesis: &Genesis,
    args: &InitializeArgs,
    auth: Auth,
    observer: &Observer,
) -> miette::Result<()> {
    match initialize::run(config, genesis, &args.options(auth), observer)
        .map_err(|error| miette::miette!("{error}"))?
    {
        initialize::Outcome::Existing => println!("publisher storage already initialized"),
        initialize::Outcome::Restored { sequence } => println!("restored sequence {sequence}"),
        initialize::Outcome::Genesis => {
            println!("restore unavailable; explicit genesis start selected")
        }
    }
    Ok(())
}

fn run_backfill(
    config: &RootConfig,
    genesis: &Genesis,
    options: &backfill::Options,
    observer: &Observer,
) -> miette::Result<()> {
    match backfill::run(config, genesis, options, spawn_exit_watcher()?, observer)
        .into_diagnostic()?
    {
        backfill::Outcome::UntilEpoch { sequence } => println!("sequence {sequence} published"),
        backfill::Outcome::UpToDate => println!("repository is up to date with Mithril"),
    }
    Ok(())
}

fn run_publish(
    config: &RootConfig,
    genesis: &Genesis,
    args: &PublishArgs,
    auth: Auth,
    observer: &Observer,
) -> miette::Result<()> {
    let retained = args.state_epochs.read()?;
    let selection = Selection {
        epochs: args.epochs,
        index_band: args.index_band,
        producers: args.producers,
    };
    let destination = match (&args.output_dir, &args.repo) {
        (Some(path), None) => Destination::Directory(path.clone()),
        (None, Some(repository)) => Destination::Repository {
            repository: repository.clone(),
            auth,
            insecure: args.insecure,
            scratch_dir: args.scratch_dir.clone(),
            rebuild: args.rebuild,
            concurrency: args.concurrency,
            verify_carried: args.verify_carried,
            require_new: args.require_new,
        },
        _ => unreachable!("clap enforces exactly one destination"),
    };
    let (report, outcome) = publisher::publish_once(
        config,
        genesis,
        &retained,
        selection,
        destination,
        args.dry_run,
        observer,
    )
    .map_err(|error| miette::miette!("{error}"))?;
    eprintln!("network: {} ({})", report.network, report.magic);
    eprintln!("cursor:  {}", report.cursor);
    match outcome {
        PublishOutcome::DryRun { sequence } => {
            println!("dry run: sequence {sequence}; nothing written")
        }
        PublishOutcome::Nothing(message) => println!("{message}"),
        PublishOutcome::Directory {
            sequence,
            identity,
            layers,
        } => {
            println!("wrote sequence {sequence}: {identity} ({layers} layers)")
        }
        PublishOutcome::Repository {
            sequence,
            identity,
            built,
            reused,
            transfer,
        } => {
            println!(
                "wrote sequence {sequence}: {identity} ({built} built, {reused} reused; {} bytes uploaded, {} skipped, {} reused)",
                transfer.bytes_uploaded, transfer.bytes_skipped, transfer.bytes_reused
            )
        }
    }
    Ok(())
}

fn main() -> miette::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt().with_target(false).init();
    let cli = Cli::parse();
    let config = config::load(cli.config.as_deref())
        .into_diagnostic()
        .context("parsing configuration")?;
    let genesis = load_genesis(&config)?;
    let auth = cli.credentials.auth()?;
    let observer = Observer::new(Arc::new(ConsoleProgress));

    match cli.command {
        Command::Initialize(args) => run_initialize(&config, &genesis, &args, auth, &observer),
        Command::Publish(args) => run_publish(&config, &genesis, &args, auth, &observer),
        Command::Backfill(args) => {
            let options = args.options(&config, auth)?;
            run_backfill(&config, &genesis, &options, &observer)
        }
        Command::Run(args) => {
            let options = args.backfill.options(&config, auth.clone())?;
            let initialize = InitializeArgs {
                source: args
                    .restore_source
                    .unwrap_or_else(|| RestoreSource::Repo(args.backfill.repo.clone())),
                point: args.point,
                insecure: args.backfill.insecure,
                scratch_dir: args.backfill.scratch_dir.clone(),
                skip_space_check: args.skip_space_check,
                resume: args.resume,
                allow_genesis: args.allow_genesis,
            };
            run_initialize(&config, &genesis, &initialize, auth, &observer)?;
            run_backfill(&config, &genesis, &options, &observer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("stelae-publisher").chain(args.iter().copied()))
    }

    #[test]
    fn every_publishing_command_requires_the_state_epochs_file() {
        let repo = ["--repo", "oci://registry.invalid/cardano/preview"];
        for command in ["publish", "backfill", "run"] {
            let args: Vec<&str> = std::iter::once(command).chain(repo).collect();
            let error = parse(&args).unwrap_err().to_string();
            assert!(error.contains("--state-epochs-file"), "{command}: {error}");

            let args: Vec<&str> = args
                .into_iter()
                .chain(["--state-epochs-file", "/etc/publisher/state-epochs"])
                .collect();
            parse(&args).unwrap();
        }
        parse(&["initialize", "--source", "file:///stele"]).unwrap();
    }

    #[test]
    fn registry_credentials_come_from_the_invocation() {
        let anonymous = CredentialArgs {
            registry_user: None,
        };
        assert_eq!(anonymous.auth_from(|_| None).unwrap(), Auth::Anonymous);

        let publisher = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
        };
        let password = |name: &str| (name == PASSWORD_ENV).then(|| "secret".to_owned());
        assert_eq!(
            publisher.auth_from(password).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "secret".to_owned(),
            }
        );

        let token = |name: &str| (name == TOKEN_ENV).then(|| "ghp_x".to_owned());
        assert_eq!(
            anonymous.auth_from(token).unwrap(),
            Auth::Bearer("ghp_x".to_owned())
        );
        assert!(publisher.auth_from(token).is_err());
        assert!(anonymous.auth_from(password).is_err());
    }
}
