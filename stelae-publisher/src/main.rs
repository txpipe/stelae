use std::{
    io::Write as _,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use miette::{Context as _, IntoDiagnostic as _};
use stelae::progress::{Event, Observer, Progress};
use stelae_cardano::{
    backfill, config, initialize,
    publisher::{self, Destination, PublishOutcome},
    snapshot::{self, Boundary, PlanReport, Published, Verified},
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
/// secret from `DOLOS_STELAE_PASSWORD` or `DOLOS_STELAE_TOKEN`, or from
/// `--registry-credentials-file`, never an argument, where it would land in
/// process listings.
#[derive(Debug, Args, Clone, Default)]
struct CredentialArgs {
    /// The user to authenticate to the registry as; `verify` and `inspect`
    /// read the configured network's official repository as its published
    /// read-only user when none is given.
    #[arg(long, global = true, env = "DOLOS_STELAE_USER", value_name = "USER")]
    registry_user: Option<String>,
    /// A TOML file naming the registry secret as `password` or `token`, in
    /// place of DOLOS_STELAE_PASSWORD / DOLOS_STELAE_TOKEN.
    #[arg(long, global = true, value_name = "PATH")]
    registry_credentials_file: Option<PathBuf>,
}

const PASSWORD_ENV: &str = "DOLOS_STELAE_PASSWORD";
const TOKEN_ENV: &str = "DOLOS_STELAE_TOKEN";

/// Reads one environment variable; the process environment outside tests.
type Env<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Resolves the registry identity, called only by a command that reaches a
/// registry.
type ResolveAuth<'a> = &'a dyn Fn() -> miette::Result<Auth>;

/// Resolves the identity a command reads `repository` as.
type ResolveReadAuth<'a> = &'a dyn Fn(&Repository) -> miette::Result<Auth>;

/// The secret half of the credentials.
#[derive(Default)]
struct Secrets {
    password: Option<String>,
    token: Option<String>,
}

impl CredentialArgs {
    fn auth_from(&self, env: Env) -> miette::Result<Auth> {
        self.resolve(env, None)
    }

    /// As Dolos reads a stele: with neither a user nor a token, `official` is
    /// the user, and the official password comes with it.
    fn read_auth_from(&self, env: Env, official: Option<&str>) -> miette::Result<Auth> {
        self.resolve(env, official)
    }

    fn resolve(&self, env: Env, official: Option<&str>) -> miette::Result<Auth> {
        let secrets = match &self.registry_credentials_file {
            Some(path) => read_secrets(path)?,
            None => Secrets {
                password: env(PASSWORD_ENV),
                token: env(TOKEN_ENV),
            },
        };
        // An empty value is no secret, as in Dolos.
        let password = secrets.password.filter(|value| !value.is_empty());
        let token = secrets.token.filter(|value| !value.is_empty());
        let user = match (
            self.registry_user.clone().filter(|user| !user.is_empty()),
            &token,
        ) {
            (Some(user), _) => Some(user),
            (None, None) => official.map(str::to_owned),
            (None, Some(_)) => None,
        };
        let inputs = RegistryAuth::from_inputs(user, password, token);
        stelae_cardano::registry_auth(&inputs).into_diagnostic()
    }
}

/// Reads a credentials file, saying where it failed and never what it holds:
/// a parse error would quote the offending line, and that line is a secret.
fn read_secrets(path: &Path) -> miette::Result<Secrets> {
    let refused = || {
        miette::miette!(
            "{} is not a TOML file naming only `password` or `token`",
            path.display()
        )
    };
    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading the registry credentials in {}", path.display()))?;
    let table: toml::Table = text.parse().map_err(|_| refused())?;

    let mut secrets = Secrets::default();
    for (key, value) in table {
        let value = match value {
            toml::Value::String(value) => value,
            _ => return Err(refused()),
        };
        match key.as_str() {
            "password" => secrets.password = Some(value),
            "token" => secrets.token = Some(value),
            _ => return Err(refused()),
        }
    }
    Ok(secrets)
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
        read_state_epochs(&self.state_epochs_file)
    }
}

fn read_state_epochs(path: &Path) -> miette::Result<RetainedEpochs> {
    config::read_state_epochs(path).into_diagnostic()
}

/// How a command writes its result to stdout; progress and logs go to stderr.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
enum Format {
    #[default]
    Text,
    Json,
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
    /// Compute a stele's inscription and identity from the committed stores,
    /// standing on the first block of an epoch, without writing one.
    Digest(DigestArgs),
    /// Check a published stele's digests and, with --reproduce, rebuild every
    /// layer from the committed stores.
    Verify(VerifyArgs),
    /// Print what a published stele contains, without pulling a layer.
    Inspect(InspectArgs),
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
    fn options(&self, auth: ResolveAuth) -> miette::Result<initialize::Options> {
        let auth = match self.source {
            RestoreSource::Repo(_) => auth()?,
            RestoreSource::Dir(_) => Auth::Anonymous,
        };
        Ok(initialize::Options {
            source: self.source.clone(),
            point: self.point,
            auth,
            insecure: self.insecure,
            scratch_dir: self.scratch_dir.clone(),
            skip_space_check: self.skip_space_check,
            resume: self.resume,
            allow_genesis: self.allow_genesis,
        })
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
    /// Every backfill input, resolved before a store opens.
    fn options(&self, config: &RootConfig, auth: ResolveAuth) -> miette::Result<backfill::Options> {
        if self.window == 0 {
            return Err(miette::miette!("--window must be at least 1"));
        }
        let mithril = config::mithril_source(
            config,
            self.mithril_aggregator.clone(),
            self.mithril_genesis_vkey.clone(),
        )
        .into_diagnostic()?;
        let origin = match self.mithril_aggregator {
            Some(_) => "invocation",
            None => "network catalog",
        };
        tracing::info!(
            aggregator = %without_userinfo(&mithril.aggregator),
            from = origin,
            "Mithril aggregator"
        );
        Ok(backfill::Options {
            repository: self.repo.clone(),
            auth: auth()?,
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

/// The URL as logged: userinfo works as Basic auth, so it never reaches the
/// logs.
fn without_userinfo(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.to_string()
        }
        Err(_) => "(unparseable URL)".to_owned(),
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

impl RunArgs {
    /// The restore and backfill inputs, the backfill's resolved first so a bad
    /// one fails before the restore clears a store.
    fn options(
        &self,
        config: &RootConfig,
        auth: ResolveAuth,
    ) -> miette::Result<(initialize::Options, backfill::Options)> {
        let backfill = self.backfill.options(config, auth)?;
        let initialize = InitializeArgs {
            source: self
                .restore_source
                .clone()
                .unwrap_or_else(|| RestoreSource::Repo(self.backfill.repo.clone())),
            point: self.point,
            insecure: self.backfill.insecure,
            scratch_dir: self.backfill.scratch_dir.clone(),
            skip_space_check: self.skip_space_check,
            resume: self.resume,
            allow_genesis: self.allow_genesis,
        }
        .options(auth)?;
        Ok((initialize, backfill))
    }
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

#[derive(Debug, Args)]
struct DigestArgs {
    /// Epochs to build layers for, e.g. `500..520`, `500..=520`, `500..`,
    /// `..520` or `500`; defaults to every epoch below the cursor, and must
    /// match what the stele being reproduced was published with.
    #[arg(long, value_name = "RANGE")]
    epochs: Option<EpochRange>,
    /// Epochs whose index layers one archive traversal fills; changes nothing
    /// about the digest.
    #[arg(long, value_name = "EPOCHS")]
    index_band: Option<NonZeroUsize>,
    /// Layer producers run at once; changes nothing about the digest.
    #[arg(long, value_name = "N")]
    producers: Option<NonZeroUsize>,
    /// Canonical inscription of the stele this one follows, as `inspect
    /// --format json` or a published `inscription.json` holds it; without it
    /// the reproduction carries no history, which is a different digest.
    #[arg(long, value_name = "FILE")]
    chain_from: Option<PathBuf>,
    /// Write the canonical inscription to FILE instead of to stdout.
    #[arg(long, value_name = "FILE")]
    output: Option<PathBuf>,
    #[command(flatten)]
    state_epochs: StateEpochsArgs,
    /// The inscription is JSON in either format.
    #[arg(long, value_enum, default_value_t)]
    format: Format,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    /// Stele to verify: oci://HOST/PATH, or file://DIR for a directory stele.
    #[arg(long, value_name = "URL")]
    repo: RestoreSource,
    /// `latest`, or `epoch-N` for the stele published at the end of epoch N.
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    insecure: bool,
    /// Also rebuild every layer from the committed stores, which must stand on
    /// the first block of the stele's epoch, and require a byte-identical
    /// inscription. Costs what a publish costs.
    #[arg(long, action)]
    reproduce: bool,
    #[arg(long, value_name = "EPOCHS", requires = "reproduce")]
    index_band: Option<NonZeroUsize>,
    #[arg(long, value_name = "N", requires = "reproduce")]
    producers: Option<NonZeroUsize>,
    /// Epochs the reproduction builds layers for; must match what the stele
    /// was published with.
    #[arg(long, value_name = "RANGE", requires = "reproduce")]
    epochs: Option<EpochRange>,
    /// File listing the retained state-dump epochs the reproduction plans
    /// with, one per line, ascending; required by --reproduce.
    #[arg(
        long,
        value_name = "PATH",
        requires = "reproduce",
        required_if_eq("reproduce", "true")
    )]
    state_epochs_file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t)]
    format: Format,
}

#[derive(Debug, Args)]
struct InspectArgs {
    /// Stele to inspect: oci://HOST/PATH, or file://DIR for a directory stele.
    #[arg(long, value_name = "URL")]
    repo: RestoreSource,
    /// `latest`, or `epoch-N` for the stele published at the end of epoch N.
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    insecure: bool,
    /// `json` emits the canonical inscription verbatim, the bytes the identity
    /// is the sha256 of and what `digest --chain-from` takes.
    #[arg(long, value_enum, default_value_t)]
    format: Format,
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
    options: &initialize::Options,
    observer: &Observer,
) -> miette::Result<()> {
    match initialize::run(config, genesis, options, observer)
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
    auth: ResolveAuth,
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
            auth: auth()?,
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

/// The stele failed a verification that ran.
#[derive(Debug)]
struct Mismatch(snapshot::Error);

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Mismatch {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

impl miette::Diagnostic for Mismatch {}

/// The exit status of a [`Mismatch`], beside 1 for an error and 2 for a usage
/// error, as Dolos's commands exit.
const MISMATCH_EXIT: u8 = 3;

fn diagnose(error: snapshot::Error) -> miette::Report {
    if error.is_mismatch() {
        miette::Report::new(Mismatch(error))
    } else {
        miette::Report::from_err(error)
    }
}

fn source_name(source: &RestoreSource) -> String {
    match source {
        RestoreSource::Dir(path) => format!("file://{}", path.display()),
        RestoreSource::Repo(repository) => repository.to_string(),
    }
}

fn open_stele(
    config: &RootConfig,
    source: &RestoreSource,
    insecure: bool,
    auth: ResolveReadAuth,
) -> miette::Result<Published> {
    let auth = match source {
        RestoreSource::Repo(repository) => auth(repository)?,
        RestoreSource::Dir(_) => Auth::Anonymous,
    };
    Published::open(config, source, insecure, auth).map_err(diagnose)
}

/// Written rather than printed, and flushed: a canonical document carries no
/// trailing newline, and the flush at exit would discard a write error.
fn write_stdout(bytes: &[u8]) -> miette::Result<()> {
    let mut stdout = std::io::stdout();
    stdout
        .write_all(bytes)
        .into_diagnostic()
        .context("writing to stdout")?;
    stdout.flush().into_diagnostic().context("flushing stdout")
}

/// Where the stores stand and what the selection covers, on stderr before
/// anything slow runs.
fn report_plan(report: &PlanReport, boundary: &Boundary) {
    eprintln!("network:  {} ({})", report.network, report.magic);
    eprintln!("cursor:   {}", report.cursor);
    eprintln!("sequence: {} (tag {})", report.sequence, report.tag);
    eprintln!(
        "boundary: first block of epoch {}, after {}",
        boundary.epoch, boundary.parent,
    );
    eprintln!(
        "band:     {} epochs per index traversal ({} MiB budgeted)",
        report.band_epochs, report.band_budget_mib,
    );
    match report.epochs {
        Some(span) => eprintln!(
            "epochs:   {}..={} ({} of them, slots {}..={})",
            span.first, span.last, span.count, span.start_slot, span.end_slot,
        ),
        None => eprintln!("epochs:   none selected; the state tip only"),
    }
    eprintln!(
        "dumps:    {:?} retained ({} due at this sequence)",
        report.retained, report.dumps_due,
    );
}

fn run_digest(config: &RootConfig, genesis: &Genesis, args: &DigestArgs) -> miette::Result<()> {
    let selection = Selection {
        epochs: args.epochs,
        index_band: args.index_band,
        producers: args.producers,
    };
    let digested = snapshot::with_plan(
        config,
        genesis,
        args.state_epochs.read()?,
        selection,
        &report_plan,
        |source, plan| snapshot::digest(source, plan, args.chain_from.as_deref()),
    )
    .map_err(diagnose)?;
    let document = &digested.document;
    match digested.history {
        Some(entries) => eprintln!("history:  {entries} entries"),
        None => eprintln!("history:  none; this reproduction starts a chain"),
    }
    eprintln!("layers:   {}", document.layers);
    eprintln!(
        "size:     {} uncompressed bytes",
        document.uncompressed_size
    );
    eprintln!("identity: {}", document.identity);

    match &args.output {
        Some(path) => {
            std::fs::write(path, &document.canonical)
                .into_diagnostic()
                .with_context(|| format!("writing {}", path.display()))?;
            eprintln!("wrote {}", path.display());
            Ok(())
        }
        // The canonical document, which hashes to the identity above.
        None => match args.format {
            Format::Text | Format::Json => write_stdout(&document.canonical),
        },
    }
}

fn print_verified(name: &str, point: Point, verified: &Verified) {
    let inscription = &verified.inscription;
    println!("verified {name} at {point}");
    println!("sequence: {}", inscription.sequence);
    match inscription.history.as_slice() {
        [] => println!("history:  none; this stele starts a chain"),
        [first, ..] => println!(
            "history:  {} entries, contiguous back to sequence {}",
            inscription.history.len(),
            first.sequence,
        ),
    }
    println!(
        "layers:   {} streamed and checked ({} compressed bytes)",
        inscription.layers.len(),
        verified.compressed_bytes,
    );
    println!("identity: {}", verified.identity);
    println!("checked:  digests only; signatures and chain provenance are phase 5");
}

fn run_verify(
    config: &RootConfig,
    genesis: &Genesis,
    args: &VerifyArgs,
    auth: ResolveReadAuth,
) -> miette::Result<()> {
    let stele = open_stele(config, &args.repo, args.insecure, auth)?;
    let name = source_name(&args.repo);
    let text = args.format == Format::Text;
    let transport = || {
        let verified = stele.verify(args.point)?;
        if text {
            print_verified(&name, args.point, &verified);
        }
        Ok(verified)
    };

    let (verified, reproduced) = match &args.state_epochs_file {
        Some(path) if args.reproduce => {
            let selection = Selection {
                epochs: args.epochs,
                index_band: args.index_band,
                producers: args.producers,
            };
            // Planned before the transport check, so a selection these stores
            // cannot plan is refused before every blob has been streamed.
            snapshot::with_plan(
                config,
                genesis,
                read_state_epochs(path)?,
                selection,
                &report_plan,
                |source, plan| {
                    let verified = transport()?;
                    let reproduced = snapshot::reproduce(source, plan, &verified.inscription)?;
                    if text {
                        println!(
                            "reproduced: {} layers rebuilt from the local stores; the documents \
                             are byte-identical ({})",
                            reproduced.layers.len(),
                            verified.identity,
                        );
                    }
                    Ok((verified, Some(reproduced.layers.len())))
                },
            )
            .map_err(diagnose)?
        }
        _ => (transport().map_err(diagnose)?, None),
    };

    if !text {
        let inscription = &verified.inscription;
        let report = serde_json::json!({
            "repo": name,
            "point": args.point.to_string(),
            "sequence": inscription.sequence,
            "identity": verified.identity.to_string(),
            "history": inscription.history.first().map(|first| serde_json::json!({
                "entries": inscription.history.len(),
                "first_sequence": first.sequence,
            })),
            "layers": inscription.layers.len(),
            "compressed_bytes": verified.compressed_bytes,
            "checked": "digests",
            "reproduced": reproduced.map(|layers| serde_json::json!({ "layers": layers })),
        });
        write_stdout(format!("{report}\n").as_bytes())?;
    }
    Ok(())
}

fn run_inspect(
    config: &RootConfig,
    args: &InspectArgs,
    auth: ResolveReadAuth,
) -> miette::Result<()> {
    let inspected = open_stele(config, &args.repo, args.insecure, auth)?
        .inspect(args.point)
        .map_err(diagnose)?;
    let inscription = &inspected.inscription;

    if args.format == Format::Json {
        // The bytes, not a re-encoding: anything else hashes to an identity
        // nobody published.
        return write_stdout(&inscription.canonicalize().into_diagnostic()?);
    }

    let position = snapshot::read_position(&inscription.position)
        .into_diagnostic()
        .context("reading the stele's position")?;
    println!("{} at {}", source_name(&args.repo), args.point);
    println!("identity: {}", inspected.identity);
    println!(
        "profile:  {} v{}",
        inscription.profile.name, inscription.profile.version
    );
    println!("sequence: {}", inscription.sequence);
    println!(
        "position: {} (magic {}), epoch {}, {}",
        position.network.name(),
        position.network.magic(),
        position.epoch,
        position.point,
    );
    println!(
        "compression: {} level {}",
        inscription.compression.algo, inscription.compression.level
    );
    match (inscription.history.first(), inscription.history.last()) {
        (Some(first), Some(last)) if first.sequence != last.sequence => println!(
            "history:  {} entries, sequence {} ({}) through sequence {} ({})",
            inscription.history.len(),
            first.sequence,
            first.inscription_digest,
            last.sequence,
            last.inscription_digest,
        ),
        (Some(only), _) => println!(
            "history:  1 entry, sequence {} ({})",
            only.sequence, only.inscription_digest,
        ),
        _ => println!("history:  none; this stele starts a chain"),
    }
    println!("layers:   {}", inscription.layers.len());
    let mut records = 0u64;
    for (descriptor, compressed) in inscription.layers.iter().zip(&inspected.compressed) {
        records += descriptor.records;
        // A size the manifest cannot state prints as unknown.
        let compressed = match compressed {
            Some(bytes) => bytes.to_string(),
            None => "?".to_owned(),
        };
        println!(
            "  {:<8} {:>10} records {:>14} bytes {:>14} compressed  {}",
            descriptor.kind,
            descriptor.records,
            descriptor.uncompressed_size,
            compressed,
            descriptor.scope,
        );
    }
    // A total over sizes some of which are unknown would understate it.
    let total_compressed = match inspected.compressed.iter().all(Option::is_some) {
        true => inspected.total_compressed.to_string(),
        false => "?".to_owned(),
    };
    println!(
        "totals:   {} records, {} uncompressed bytes, {} compressed bytes",
        records,
        inscription.uncompressed_size(),
        total_compressed,
    );
    Ok(())
}

fn main() -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
    match execute(Cli::parse(), &|name| std::env::var(name).ok()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            eprintln!("Error: {report:?}");
            match report.downcast_ref::<Mismatch>() {
                Some(_) => ExitCode::from(MISMATCH_EXIT),
                None => ExitCode::FAILURE,
            }
        }
    }
}

fn execute(cli: Cli, env: Env) -> miette::Result<()> {
    let config = config::load(cli.config.as_deref())
        .into_diagnostic()
        .context("parsing configuration")?;
    let genesis = load_genesis(&config)?;
    let auth = || cli.credentials.auth_from(env);
    let read_auth = |repository: &Repository| {
        let official = snapshot::official_reader(&config, repository);
        cli.credentials.read_auth_from(env, official)
    };
    let observer = Observer::new(Arc::new(ConsoleProgress));

    match cli.command {
        Command::Initialize(args) => {
            run_initialize(&config, &genesis, &args.options(&auth)?, &observer)
        }
        Command::Publish(args) => run_publish(&config, &genesis, &args, &auth, &observer),
        Command::Backfill(args) => {
            let options = args.options(&config, &auth)?;
            run_backfill(&config, &genesis, &options, &observer)
        }
        Command::Run(args) => {
            let (initialize, backfill) = args.options(&config, &auth)?;
            run_initialize(&config, &genesis, &initialize, &observer)?;
            run_backfill(&config, &genesis, &backfill, &observer)
        }
        Command::Digest(args) => run_digest(&config, &genesis, &args),
        Command::Verify(args) => run_verify(&config, &genesis, &args, &read_auth),
        Command::Inspect(args) => run_inspect(&config, &args, &read_auth),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::CommandFactory as _;

    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("stelae-publisher").chain(args.iter().copied()))
    }

    /// A preview node whose stores live in `dir`, and its configuration file.
    fn node(dir: &Path) -> String {
        let path = dir.join("dolos.toml");
        let document = format!(
            "compatibility = \"v2-alpha.1\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n[storage]\npath = {:?}\n",
            dir.join("data").display().to_string(),
        );
        std::fs::write(&path, document).unwrap();
        path.display().to_string()
    }

    /// A password with no user: refused wherever credentials are resolved.
    fn orphan_password(name: &str) -> Option<String> {
        (name == PASSWORD_ENV).then(|| "secret".to_owned())
    }

    #[test]
    fn backfill_options_carry_the_state_epochs_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();
        let epochs = dir.path().join("state-epochs");
        std::fs::write(&epochs, "1\n200\n").unwrap();

        let cli = parse(&[
            "backfill",
            "--repo",
            "oci://registry.invalid/cardano/preview",
            "--state-epochs-file",
            epochs.to_str().unwrap(),
        ])
        .unwrap();
        let Command::Backfill(args) = cli.command else {
            panic!("parsed another command");
        };
        let options = args.options(&config, &|| Ok(Auth::Anonymous)).unwrap();
        assert_eq!(options.retained.as_slice(), &[1, 200]);
    }

    #[test]
    fn run_restores_with_its_inputs_and_the_registry_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();
        let epochs = dir.path().join("state-epochs");
        std::fs::write(&epochs, "1\n").unwrap();
        let scratch = dir.path().join("scratch");
        let basic = Auth::Basic {
            user: "publisher".to_owned(),
            password: "secret".to_owned(),
        };
        let auth = || Ok(basic.clone());

        let repo = ["--repo", "oci://registry.invalid/cardano/preview"];
        let epochs = ["--state-epochs-file", epochs.to_str().unwrap()];
        // One input per row, so a field read from another input's flag fails.
        let rows: [&[&str]; 8] = [
            &[],
            &["--restore-source", "oci://registry.invalid/cardano/other"],
            &["--point", "epoch-7"],
            &["--scratch-dir", scratch.to_str().unwrap()],
            &["--insecure"],
            &["--skip-space-check"],
            &["--resume"],
            &["--allow-genesis"],
        ];
        for restore in rows {
            let args: Vec<&str> = std::iter::once("run")
                .chain(repo)
                .chain(epochs)
                .chain(restore.iter().copied())
                .collect();
            let Command::Run(args) = parse(&args).unwrap().command else {
                panic!("parsed another command");
            };
            let (initialize, backfill) = args.options(&config, &auth).unwrap();
            let given = |flag: &str| restore.contains(&flag);
            let RestoreSource::Repo(source) = &initialize.source else {
                panic!("{restore:?} restores from a directory");
            };
            let expected = if given("--restore-source") {
                "cardano/other"
            } else {
                "cardano/preview"
            };
            assert_eq!(source.repository(), expected, "{restore:?}");
            let point = if given("--point") {
                Point::Epoch(7)
            } else {
                Point::Latest
            };
            assert_eq!(initialize.point, point, "{restore:?}");
            assert_eq!(
                initialize.scratch_dir,
                given("--scratch-dir").then(|| scratch.clone()),
                "{restore:?}"
            );
            assert_eq!(initialize.insecure, given("--insecure"), "{restore:?}");
            assert_eq!(
                initialize.skip_space_check,
                given("--skip-space-check"),
                "{restore:?}"
            );
            assert_eq!(initialize.resume, given("--resume"), "{restore:?}");
            assert_eq!(
                initialize.allow_genesis,
                given("--allow-genesis"),
                "{restore:?}"
            );
            assert_eq!(initialize.auth, basic, "{restore:?}");
            assert_eq!(backfill.auth, basic, "{restore:?}");
        }
    }

    #[test]
    fn run_checks_its_backfill_inputs_before_restoring() {
        let dir = tempfile::tempdir().unwrap();
        let config = node(dir.path());
        let epochs = dir.path().join("state-epochs");
        std::fs::write(&epochs, "four\n").unwrap();

        let cli = parse(&[
            "--config",
            &config,
            "run",
            "--repo",
            "oci://registry.invalid/cardano/preview",
            "--state-epochs-file",
            epochs.to_str().unwrap(),
            "--restore-source",
            "file:///missing-stele",
        ])
        .unwrap();
        let error = format!("{:?}", execute(cli, &|_| None).unwrap_err());
        assert!(error.contains("state epochs file"), "{error}");
        assert!(!dir.path().join("data").exists());
    }

    #[test]
    fn a_command_off_the_registry_ignores_its_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = node(dir.path());
        let epochs = dir.path().join("state-epochs");
        std::fs::write(&epochs, "1\n").unwrap();
        let output = dir.path().join("stele");

        let from_directory = parse(&[
            "--config",
            &config,
            "initialize",
            "--source",
            "file:///missing-stele",
            "--allow-genesis",
        ])
        .unwrap();
        execute(from_directory, &orphan_password).unwrap();

        let to_directory = parse(&[
            "--config",
            &config,
            "publish",
            "--output-dir",
            output.to_str().unwrap(),
            "--state-epochs-file",
            epochs.to_str().unwrap(),
        ])
        .unwrap();
        // Past the credentials, publishing reaches the store, empty since the
        // genesis start.
        let error = format!("{:?}", execute(to_directory, &orphan_password).unwrap_err());
        assert!(error.contains("has no cursor"), "{error}");
    }

    #[test]
    fn every_registry_command_refuses_bad_credentials_before_a_store_opens() {
        let dir = tempfile::tempdir().unwrap();
        let config = node(dir.path());
        let epochs = dir.path().join("state-epochs");
        std::fs::write(&epochs, "1\n").unwrap();
        let epochs = epochs.to_str().unwrap();
        let repo = "oci://registry.invalid/cardano/preview";

        let commands: [&[&str]; 6] = [
            &["initialize", "--source", repo],
            &["verify", "--repo", repo],
            &["inspect", "--repo", repo],
            &["publish", "--repo", repo, "--state-epochs-file", epochs],
            &["backfill", "--repo", repo, "--state-epochs-file", epochs],
            &[
                "run",
                "--repo",
                repo,
                "--state-epochs-file",
                epochs,
                "--restore-source",
                "file:///missing-stele",
            ],
        ];
        for command in commands {
            // The user is pinned empty so one in the caller's environment
            // cannot pair with the password.
            let args: Vec<&str> = ["--config", config.as_str(), "--registry-user", ""]
                .into_iter()
                .chain(command.iter().copied())
                .collect();
            let error = format!(
                "{:?}",
                execute(parse(&args).unwrap(), &orphan_password).unwrap_err()
            );
            assert!(
                error.contains("a password with no user"),
                "{}: {error}",
                command[0]
            );
            assert!(!dir.path().join("data").exists(), "{}", command[0]);
        }
    }

    #[test]
    fn a_logged_aggregator_drops_its_userinfo() {
        assert_eq!(
            without_userinfo("https://user:key@aggregator.invalid/aggregator"),
            "https://aggregator.invalid/aggregator"
        );
        assert_eq!(without_userinfo("not a url"), "(unparseable URL)");
    }

    #[test]
    fn the_chart_sets_the_credential_names_the_binary_reads() {
        let job = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../k8s/dolos-publisher/chart/templates/job.yaml"),
        )
        .unwrap();
        let command = Cli::command();
        let user = command
            .get_arguments()
            .find(|arg| arg.get_id() == "registry_user")
            .and_then(|arg| arg.get_env())
            .unwrap()
            .to_str()
            .unwrap();
        for name in [user, PASSWORD_ENV] {
            let entry = format!("- name: {name}");
            assert!(
                job.lines().any(|line| line.trim() == entry),
                "job.yaml does not set {name}"
            );
        }
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
        let anonymous = CredentialArgs::default();
        assert_eq!(anonymous.auth_from(&|_| None).unwrap(), Auth::Anonymous);

        let publisher = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..CredentialArgs::default()
        };
        let password = |name: &str| (name == PASSWORD_ENV).then(|| "secret".to_owned());
        assert_eq!(
            publisher.auth_from(&password).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "secret".to_owned(),
            }
        );

        let token = |name: &str| (name == TOKEN_ENV).then(|| "ghp_x".to_owned());
        assert_eq!(
            anonymous.auth_from(&token).unwrap(),
            Auth::Bearer("ghp_x".to_owned())
        );
        assert!(publisher.auth_from(&token).is_err());
        assert!(anonymous.auth_from(&password).is_err());
    }

    #[test]
    fn a_reader_takes_the_official_user_for_the_official_repository_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();
        let official: Repository = "oci://oci.stelae.store/cardano/preview".parse().unwrap();
        let other: Repository = "oci://registry.invalid/cardano/preview".parse().unwrap();
        let read = |args: &CredentialArgs, repository: &Repository, env: Env| {
            args.read_auth_from(env, snapshot::official_reader(&config, repository))
        };

        let anonymous = CredentialArgs::default();
        let Auth::Basic { user, password } = read(&anonymous, &official, &|_| None).unwrap() else {
            panic!("the official repository is read anonymously");
        };
        assert!(!user.is_empty() && !password.is_empty());
        assert_eq!(
            read(&anonymous, &other, &|_| None).unwrap(),
            Auth::Anonymous
        );

        let token = |name: &str| (name == TOKEN_ENV).then(|| "ghp_x".to_owned());
        assert_eq!(
            read(&anonymous, &official, &token).unwrap(),
            Auth::Bearer("ghp_x".to_owned())
        );
        let named = CredentialArgs {
            registry_user: Some("reader".to_owned()),
            ..CredentialArgs::default()
        };
        let password = |name: &str| (name == PASSWORD_ENV).then(|| "secret".to_owned());
        assert_eq!(
            read(&named, &official, &password).unwrap(),
            Auth::Basic {
                user: "reader".to_owned(),
                password: "secret".to_owned(),
            }
        );
    }

    #[test]
    fn a_credentials_file_takes_the_place_of_the_secret_variables() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("credentials.toml");
        let with_file = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            registry_credentials_file: Some(file.clone()),
        };
        let env_password = |name: &str| (name == PASSWORD_ENV).then(|| "from-env".to_owned());

        std::fs::write(&file, "password = \"from-file\"\n").unwrap();
        assert_eq!(
            with_file.auth_from(&env_password).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "from-file".to_owned(),
            }
        );

        let token_file = CredentialArgs {
            registry_credentials_file: Some(file.clone()),
            ..CredentialArgs::default()
        };
        std::fs::write(&file, "token = \"ghp_x\"\n").unwrap();
        assert_eq!(
            token_file.auth_from(&|_| None).unwrap(),
            Auth::Bearer("ghp_x".to_owned())
        );

        // Refused without quoting the line, which is a secret.
        for document in ["secret = \"hunter2\"\n", "password = hunter2\n"] {
            std::fs::write(&file, document).unwrap();
            let error = format!("{:?}", with_file.auth_from(&|_| None).unwrap_err());
            assert!(error.contains("naming only"), "{error}");
            assert!(!error.contains("hunter2"), "{error}");
        }
    }
}
