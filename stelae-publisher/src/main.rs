use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Args, Parser, Subcommand};
use miette::{Context as _, IntoDiagnostic as _};
use stelae::progress::{Event, Observer, Progress};
use stelae_cardano::{
    backfill, initialize,
    publisher::{self, Destination, PublishOutcome},
    EpochRange, Genesis, Point, Repository, RestoreSource, RootConfig, Selection,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(name = "stelae-publisher", version, about)]
struct Cli {
    /// Explicit Dolos-compatible configuration file.
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
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
    fn options(&self) -> initialize::Options {
        initialize::Options {
            source: self.source.clone(),
            point: self.point,
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
    fn options(&self) -> backfill::Options {
        backfill::Options {
            repository: self.repo.clone(),
            insecure: self.insecure,
            scratch_dir: self.scratch_dir.clone(),
            concurrency: self.concurrency,
            verify_carried: self.verify_carried,
            download_dir: self.download_dir.clone(),
            window: self.window,
            until_epoch: self.until_epoch,
            skip_validation: self.skip_validation,
        }
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

fn load_config(explicit: Option<&Path>) -> Result<RootConfig, config::ConfigError> {
    let mut builder = config::Config::builder()
        .add_source(config::File::with_name("/etc/dolos/daemon.toml").required(false))
        .add_source(config::File::with_name("dolos.toml").required(false));
    if let Some(path) = explicit {
        let path = path.to_str().ok_or_else(|| {
            config::ConfigError::Message(format!(
                "configuration path is not valid UTF-8: {}",
                path.display()
            ))
        })?;
        builder = builder.add_source(config::File::with_name(path).required(true));
    }
    builder
        .add_source(config::Environment::with_prefix("DOLOS").separator("_"))
        .build()?
        .try_deserialize()
}

fn load_genesis(config: &RootConfig) -> miette::Result<Genesis> {
    Genesis::from_file_paths(
        &config.genesis.byron_path,
        &config.genesis.shelley_path,
        &config.genesis.alonzo_path,
        &config.genesis.conway_path,
        config.genesis.force_protocol,
    )
    .into_diagnostic()
    .context("loading genesis files")
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
    observer: &Observer,
) -> miette::Result<()> {
    match initialize::run(config, genesis, &args.options(), observer)
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
    args: &BackfillArgs,
    observer: &Observer,
) -> miette::Result<()> {
    if args.window == 0 {
        return Err(miette::miette!("--window must be at least 1"));
    }
    match backfill::run(
        config,
        genesis,
        &args.options(),
        spawn_exit_watcher()?,
        observer,
    )
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
    observer: &Observer,
) -> miette::Result<()> {
    let selection = Selection {
        epochs: args.epochs,
        index_band: args.index_band,
        producers: args.producers,
    };
    let destination = match (&args.output_dir, &args.repo) {
        (Some(path), None) => Destination::Directory(path.clone()),
        (None, Some(repository)) => Destination::Repository {
            repository: repository.clone(),
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
        } => {
            println!("wrote sequence {sequence}: {identity} ({built} built, {reused} reused)")
        }
    }
    Ok(())
}

fn main() -> miette::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt().with_target(false).init();
    let cli = Cli::parse();
    let config = load_config(cli.config.as_deref())
        .into_diagnostic()
        .context("parsing configuration")?;
    let genesis = load_genesis(&config)?;
    let observer = Observer::new(Arc::new(ConsoleProgress));

    match cli.command {
        Command::Initialize(args) => run_initialize(&config, &genesis, &args, &observer),
        Command::Publish(args) => run_publish(&config, &genesis, &args, &observer),
        Command::Backfill(args) => run_backfill(&config, &genesis, &args, &observer),
        Command::Run(args) => {
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
            run_initialize(&config, &genesis, &initialize, &observer)?;
            run_backfill(&config, &genesis, &args.backfill, &observer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn an_explicit_non_utf8_config_path_is_rejected() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt as _};

        let path = PathBuf::from(OsString::from_vec(vec![0xff]));
        let error = match load_config(Some(&path)) {
            Ok(_) => panic!("non-UTF-8 path was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not valid UTF-8"));
    }
}
