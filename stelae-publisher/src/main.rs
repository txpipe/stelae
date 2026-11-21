use std::{
    io::Write as _,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use miette::{Context as _, IntoDiagnostic as _};
use stelae::{
    inscription::Link,
    progress::{Event, Observer, Progress},
    Digest,
};
use stelae_cardano::{
    backfill,
    batch::Limits,
    config, initialize,
    publisher::{self, Destination, PublishOutcome},
    rebuild,
    snapshot::{self, Boundary, Inputs, PlanReport, Published, Verified},
    Auth, Genesis, Point, RegistryAuth, Repository, RestoreSource, RootConfig, Selection,
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

/// The secret half of the credentials. No `Debug`, so that it cannot be
/// printed.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Secrets {
    password: Option<String>,
    token: Option<String>,
}

impl CredentialArgs {
    fn auth_from(&self, env: Env) -> miette::Result<Auth> {
        self.resolve(env, None)
    }

    /// As Dolos reads a snapshot: with neither a user nor a token, `official`
    /// is the user, and the official password comes with it.
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

/// Reads a credentials file as Dolos does, so a file it takes is taken here,
/// saying where it failed and never what it holds: a parse error would quote
/// the offending line, and that line is a secret.
fn read_secrets(path: &Path) -> miette::Result<Secrets> {
    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading the registry credentials in {}", path.display()))?;

    ::config::Config::builder()
        .add_source(::config::File::from_str(&text, ::config::FileFormat::Toml))
        .build()
        .and_then(|file| file.try_deserialize())
        .map_err(|_| {
            miette::miette!(
                "{} is not a TOML file naming only `password` or `token`",
                path.display()
            )
        })
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
    /// Initialize empty stores from a snapshot, failing closed by default.
    Initialize(InitializeArgs),
    /// Publish the currently committed dataset once.
    Publish(PublishArgs),
    /// Replay Mithril history and publish every reached epoch boundary.
    Backfill(BackfillArgs),
    /// Initialize and then run the backfill state machine.
    Run(RunArgs),
    /// Compute a snapshot's inscription and identity from the committed stores,
    /// standing on the first block of an epoch, without writing one.
    Digest(DigestArgs),
    /// Check a published snapshot's digests and, with --reproduce, rebuild
    /// every layer from the committed stores.
    Verify(VerifyArgs),
    /// Print what a published snapshot contains, without pulling a layer.
    Inspect(InspectArgs),
    /// Plan the rebuild of a lineage published before the diff chain from
    /// its documents, self-checking every sequence and writing nothing to a
    /// registry; with --write, execute such a plan.
    Rebuild(RebuildArgs),
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
#[command(group(clap::ArgGroup::new("plan_input").multiple(true).args(["blocks_journal", "write"])))]
struct RebuildArgs {
    #[command(subcommand)]
    profile: Option<RebuildProfile>,
    /// The repository of the lineage before the diff chain:
    /// oci://HOST/cardano/NETWORK.
    #[arg(
        long,
        value_name = "OCI_URL",
        required_unless_present = "write",
        conflicts_with = "write"
    )]
    source: Option<Repository>,
    #[arg(long, action)]
    insecure: bool,
    /// The blocks lineage's journal: a line per sequence, its number and the
    /// blocks inscription digest. Without it only the assignment and the
    /// self-check run.
    #[arg(long, value_name = "PATH", conflicts_with = "write")]
    blocks_journal: Option<PathBuf>,
    /// Write the plan here, a JSON line per sequence after a header line;
    /// with --write, the plan to execute.
    #[arg(long, value_name = "PATH", requires = "plan_input")]
    plan: Option<PathBuf>,
    /// The epochs whose tips the registry keeps, one per line. The newest
    /// tip is always kept.
    #[arg(long, value_name = "PATH", conflicts_with = "write")]
    kept_tips_file: Option<PathBuf>,
    /// Print each sequence with its new inscription digest.
    #[arg(long, action, requires = "blocks_journal")]
    print_digests: bool,
    /// Write the plan's lineage to the `dolos` repository under the network
    /// root, resolve it back, and only then move `latest`. Nothing is
    /// written to the plan's source.
    #[arg(long, action, requires = "plan")]
    write: bool,
    /// The network root to write under; the plan's source by default.
    #[arg(long, value_name = "OCI_URL", requires = "write")]
    root: Option<Repository>,
    /// The registry's blob storage, so blobs are copied inside it rather
    /// than pulled and pushed.
    #[arg(long, value_name = "PATH", requires = "write")]
    direct_s3_config: Option<PathBuf>,
    /// Where a blob pulled from the source is staged on its way up.
    #[arg(long, value_name = "PATH", requires = "write")]
    scratch_dir: Option<PathBuf>,
    /// The largest layer, in MiB, uploaded as one request rather than in
    /// chunks; 100 MB by default, `0` streams every layer. A value above the
    /// upload memory budget is clamped to it.
    #[arg(long, value_name = "MIB", requires = "write")]
    monolithic_max_mib: Option<u64>,
}

#[derive(Debug, Subcommand)]
enum RebuildProfile {
    /// Re-encode the old lineage's closed-epoch blocks layers under the
    /// blocks profile, compare each immutable file with Mithril's digest list
    /// and seal the blocks lineage, appending each sequence to the journal.
    Blocks(RebuildBlocksArgs),
}

#[derive(Debug, Args)]
struct RebuildBlocksArgs {
    /// The repository of the lineage before the diff chain:
    /// oci://HOST/cardano/NETWORK.
    #[arg(long, value_name = "OCI_URL")]
    source: Repository,
    #[arg(long, action)]
    insecure: bool,
    /// A line per sequence, its number and inscription digest. A run resumes
    /// after the journal's last line and appends to it.
    #[arg(long, value_name = "PATH")]
    journal: PathBuf,
    /// Upload the lineage to the `blocks` repository of this network root,
    /// oci://HOST/cardano/NETWORK. Without it nothing is written to a
    /// registry.
    #[arg(long, value_name = "OCI_URL")]
    upload: Option<Repository>,
    /// The Mithril aggregator whose digest list the blocks are compared
    /// with; defaults to the configured network's.
    #[arg(long, env = "DOLOS_MITHRIL_AGGREGATOR", value_name = "URL")]
    mithril_aggregator: Option<String>,
    /// A digest list saved from the aggregator's digests route, compared
    /// with in place of fetching one.
    #[arg(long, value_name = "PATH")]
    mithril_digests: Option<PathBuf>,
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
    /// The largest layer, in MiB, uploaded as one request rather than in
    /// chunks; 100 MB by default, `0` streams every layer. A value above the
    /// upload memory budget is clamped to it.
    #[arg(long, value_name = "MIB")]
    monolithic_max_mib: Option<u64>,
}

#[derive(Debug, Args, Clone)]
struct InitializeArgs {
    /// Network root to restore from: file://DIR or oci://HOST/PATH, holding
    /// the `dolos` and `blocks` repositories.
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
    /// Direct blob uploads to Zot-compatible S3 storage; JSON config with
    /// credentials.
    #[arg(long, value_name = "PATH")]
    direct_s3_config: Option<PathBuf>,
    /// Network root to extend: its `dolos` repository, naming the snapshots
    /// its `blocks` repository holds.
    #[arg(long, value_name = "OCI_URL")]
    repo: Repository,
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
    /// The largest layer, in MiB, uploaded as one request rather than in
    /// chunks; 100 MB by default, `0` streams every layer. A value above the
    /// upload memory budget is clamped to it.
    #[arg(long, value_name = "MIB")]
    monolithic_max_mib: Option<u64>,
    #[arg(long)]
    download_dir: Option<PathBuf>,
    #[arg(long, default_value = "40")]
    window: u64,
    #[arg(long)]
    until_epoch: Option<u64>,
    #[arg(long, action)]
    skip_validation: bool,
    /// Boundaries prepared before they are published in order. Above 1 a
    /// sequence appears in the repository only when its batch closes.
    #[arg(long, default_value = "1", value_name = "EPOCHS")]
    batch_epochs: NonZeroU64,
    /// MiB a batch may hold on disk; required above one epoch.
    #[arg(long, value_name = "MIB")]
    batch_scratch_mib: Option<u64>,
    /// MiB kept free per prepared boundary; required above one epoch.
    #[arg(long, value_name = "MIB")]
    batch_reserve_mib: Option<u64>,
}

impl BackfillArgs {
    fn limits(&self) -> miette::Result<Limits> {
        const MIB: u64 = 1024 * 1024;

        match (self.batch_scratch_mib, self.batch_reserve_mib) {
            (None, None) if self.batch_epochs.get() == 1 => Ok(Limits::ONE),
            (Some(scratch), Some(reserve)) if self.batch_epochs.get() > 1 => Ok(Limits {
                max_epochs: self.batch_epochs,
                scratch: scratch.saturating_mul(MIB),
                reserve_per_epoch: reserve.saturating_mul(MIB),
            }),
            _ => Err(miette::miette!(
                "--batch-scratch-mib and --batch-reserve-mib go together, with a --batch-epochs \
                 above 1"
            )),
        }
    }

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
            aggregator = %config::without_userinfo(&mithril.aggregator),
            from = origin,
            "Mithril aggregator"
        );
        Ok(backfill::Options {
            direct_s3: read_direct_s3(self.direct_s3_config.as_deref())?,
            repository: self.repo.clone(),
            auth: auth()?,
            insecure: self.insecure,
            scratch_dir: self.scratch_dir.clone(),
            concurrency: self.concurrency,
            monolithic_max: monolithic_max(self.monolithic_max_mib),
            download_dir: self.download_dir.clone(),
            window: self.window,
            until_epoch: self.until_epoch,
            skip_validation: self.skip_validation,
            mithril,
            limits: self.limits()?,
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
    /// Direct blob uploads to Zot-compatible S3 storage; JSON config with
    /// credentials.
    #[arg(long, value_name = "PATH", requires = "repo")]
    direct_s3_config: Option<PathBuf>,
    /// Network root on disk: the blocks snapshot goes to its `blocks` image
    /// layout and the Dolos snapshot to its `dolos` one.
    #[arg(long)]
    output_dir: Option<PathBuf>,
    /// Network root in a registry: the Dolos snapshot goes to its `dolos`
    /// repository, naming the one its `blocks` repository holds.
    #[arg(long, value_name = "OCI_URL")]
    repo: Option<Repository>,
    #[arg(long)]
    producers: Option<NonZeroUsize>,
    #[arg(long, action, conflicts_with = "output_dir")]
    insecure: bool,
    #[arg(long)]
    scratch_dir: Option<PathBuf>,
    #[arg(long, conflicts_with = "output_dir")]
    concurrency: Option<NonZeroUsize>,
    #[arg(long, action)]
    dry_run: bool,
    #[arg(long, action, conflicts_with = "output_dir")]
    require_new: bool,
}

#[derive(Debug, Args)]
struct DigestArgs {
    /// Network root to read the Dolos snapshot before this one and the blocks
    /// snapshot at this sequence from: file://DIR or oci://HOST/PATH.
    #[arg(long, value_name = "URL")]
    repo: Option<RestoreSource>,
    #[arg(long, action, requires = "repo")]
    insecure: bool,
    /// Inscription digest of the snapshot before this one, in place of the one
    /// --repo holds; without either the reproduction starts a lineage.
    #[arg(long, value_name = "DIGEST")]
    previous: Option<Digest>,
    /// Inscription digest of the blocks snapshot at this sequence, in place of
    /// the one --repo holds; without either the snapshot names none.
    #[arg(long, value_name = "DIGEST")]
    blocks: Option<Digest>,
    /// Layer producers run at once; changes nothing about the digest.
    #[arg(long, value_name = "N")]
    producers: Option<NonZeroUsize>,
    /// Where the index runs spool while the digest is computed; defaults to
    /// the storage path's `scratch` directory.
    #[arg(long, value_name = "PATH")]
    scratch_dir: Option<PathBuf>,
    /// Write the canonical inscription to FILE instead of to stdout.
    #[arg(long, value_name = "FILE")]
    output: Option<PathBuf>,
    /// The inscription is JSON in either format.
    #[arg(long, value_enum, default_value_t)]
    format: Format,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    /// Network root whose `dolos` repository holds the snapshot:
    /// oci://HOST/PATH, or file://DIR for image layouts on disk.
    #[arg(long, value_name = "URL")]
    repo: RestoreSource,
    /// `latest`, or `epoch-N` for the snapshot published at the end of epoch N.
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    insecure: bool,
    /// Also rebuild every layer from the committed stores, which must stand on
    /// the first block of the snapshot's epoch, and require a byte-identical
    /// inscription. Costs what a publish costs.
    #[arg(long, action)]
    reproduce: bool,
    #[arg(long, value_name = "N", requires = "reproduce")]
    producers: Option<NonZeroUsize>,
    /// Where the index runs spool while the snapshot is reproduced; defaults to
    /// the storage path's `scratch` directory.
    #[arg(long, value_name = "PATH", requires = "reproduce")]
    scratch_dir: Option<PathBuf>,
    /// Inscription digest of the snapshot before this one, which the published
    /// one must follow; the repository's own is held to it in any case.
    #[arg(long, value_name = "DIGEST", requires = "reproduce")]
    previous: Option<Digest>,
    #[arg(long, value_enum, default_value_t)]
    format: Format,
}

#[derive(Debug, Args)]
struct InspectArgs {
    /// Network root whose `dolos` repository holds the snapshot:
    /// oci://HOST/PATH, or file://DIR for image layouts on disk.
    #[arg(long, value_name = "URL")]
    repo: RestoreSource,
    /// `latest`, or `epoch-N` for the snapshot published at the end of epoch N.
    #[arg(long, default_value = "latest")]
    point: Point,
    #[arg(long, action)]
    insecure: bool,
    /// `json` emits the canonical inscription verbatim, the bytes the identity
    /// is the sha256 of.
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
        backfill::Outcome::BatchReady { flush, frontiers } => println!(
            "batch closed ({flush:?}): prepared through {:?}, staged at {:?}, published through \
             {:?}",
            frontiers.prepared, frontiers.staged, frontiers.published,
        ),
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
    if args.output_dir.is_some() && args.direct_s3_config.is_some() {
        return Err(miette::miette!("--direct-s3-config requires --repo"));
    }
    let selection = Selection {
        producers: args.producers,
    };
    let destination = match (&args.output_dir, &args.repo) {
        (Some(path), None) => Destination::Directory {
            path: path.clone(),
            scratch_dir: args.scratch_dir.clone(),
        },
        (None, Some(repository)) => Destination::Repository {
            direct_s3: read_direct_s3(args.direct_s3_config.as_deref())?,
            repository: repository.clone(),
            auth: auth()?,
            insecure: args.insecure,
            scratch_dir: args.scratch_dir.clone(),
            concurrency: args.concurrency,
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

/// A check that ran and failed: a snapshot's verification, or a rebuild's
/// self-check.
#[derive(Debug)]
struct Mismatch(Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Mismatch {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl miette::Diagnostic for Mismatch {}

/// The exit status of a [`Mismatch`], beside 1 for an error and 2 for a usage
/// error, as Dolos's commands exit.
const MISMATCH_EXIT: u8 = 3;

fn diagnose(error: snapshot::Error) -> miette::Report {
    if error.is_mismatch() {
        miette::Report::new(Mismatch(Box::new(error)))
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

fn open_snapshot(
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
    match report.epochs {
        Some(span) => eprintln!(
            "epochs:   {}..={} ({} of them, slots {}..={})",
            span.first, span.last, span.count, span.start_slot, span.end_slot,
        ),
        None => eprintln!("epochs:   none; the state tip only"),
    }
}

/// A link as the reports print it.
fn link_text(link: Option<Link>, none: &str) -> String {
    match link {
        Some(link) => format!("sequence {} ({})", link.sequence, link.inscription_digest),
        None => none.to_owned(),
    }
}

fn run_digest(
    config: &RootConfig,
    genesis: &Genesis,
    args: &DigestArgs,
    auth: ResolveReadAuth,
) -> miette::Result<()> {
    let selection = Selection {
        producers: args.producers,
    };
    // Before a store opens, so a source that can't be read costs no walk.
    let published = args
        .repo
        .as_ref()
        .map(|source| open_snapshot(config, source, args.insecure, auth))
        .transpose()?;
    let digested = snapshot::with_plan(
        config,
        genesis,
        selection,
        args.scratch_dir.as_deref(),
        &report_plan,
        |source, plan| {
            // A flag replaces the root's input unread, so a part of the root
            // the operator stands in for cannot fail the digest.
            let previous = match (args.previous, &published) {
                (Some(digest), _) => Some(snapshot::previous_at(plan.sequence, digest)?),
                (None, Some(published)) => published.previous(plan)?,
                (None, None) => None,
            };
            let blocks = match (args.blocks, &published) {
                (Some(blocks), _) => Some(blocks),
                (None, Some(published)) => Some(published.blocks(config, plan)?),
                (None, None) => None,
            };
            snapshot::digest(source, plan, Inputs { previous, blocks })
        },
    )
    .map_err(diagnose)?;
    let document = &digested.document;
    eprintln!(
        "previous: {}",
        link_text(
            digested.inputs.previous,
            "none; this reproduction starts a lineage"
        )
    );
    match digested.inputs.blocks {
        Some(blocks) => eprintln!("blocks:   {blocks}"),
        None => eprintln!("blocks:   none named"),
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
    println!(
        "previous: {}",
        link_text(inscription.previous, "none; this snapshot starts a lineage")
    );
    println!(
        "chain:    {} inscriptions, back to sequence {}",
        verified.chain.inscriptions().len(),
        verified.chain.first().sequence,
    );
    println!(
        "layers:   {} streamed and checked ({} compressed bytes)",
        verified.chain.descriptors().count(),
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
    let snapshot = open_snapshot(config, &args.repo, args.insecure, auth)?;
    let name = source_name(&args.repo);
    let text = args.format == Format::Text;
    let transport = || {
        let verified = snapshot.verify(args.point)?;
        if text {
            print_verified(&name, args.point, &verified);
        }
        Ok(verified)
    };

    let (verified, reproduced) = match args.reproduce {
        true => {
            let selection = Selection {
                producers: args.producers,
            };
            // Planned before the transport check, so stores that cannot plan a
            // reproduction are refused before every blob has been streamed.
            snapshot::with_plan(
                config,
                genesis,
                selection,
                args.scratch_dir.as_deref(),
                &report_plan,
                |source, plan| {
                    let verified = transport()?;
                    // The published snapshot's sequence, not the stores': stores at
                    // another one are the reproduction's refusal to make.
                    let previous = args
                        .previous
                        .map(|digest| snapshot::previous_at(verified.inscription.sequence, digest))
                        .transpose()?;
                    let reproduced =
                        snapshot::reproduce(source, plan, &verified.inscription, previous)?;
                    let layers = reproduced.layers().count();
                    if text {
                        println!(
                            "reproduced: {layers} layers rebuilt from the local stores; the \
                             documents are byte-identical ({})",
                            verified.identity,
                        );
                    }
                    Ok((verified, Some(layers)))
                },
            )
            .map_err(diagnose)?
        }
        false => (transport().map_err(diagnose)?, None),
    };

    if !text {
        let inscription = &verified.inscription;
        let report = serde_json::json!({
            "repo": name,
            "point": args.point.to_string(),
            "sequence": inscription.sequence,
            "identity": verified.identity.to_string(),
            "previous": inscription.previous.map(|link| serde_json::json!({
                "sequence": link.sequence,
                "inscription_digest": link.inscription_digest.to_string(),
            })),
            "chain": serde_json::json!({
                "inscriptions": verified.chain.inscriptions().len(),
                "first_sequence": verified.chain.first().sequence,
            }),
            "layers": verified.chain.descriptors().count(),
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
    let inspected = open_snapshot(config, &args.repo, args.insecure, auth)?
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
        .context("reading the snapshot's position")?;
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
    println!(
        "previous: {}",
        link_text(inscription.previous, "none; this snapshot starts a lineage")
    );
    println!(
        "own:      {} diff and {} tip layers",
        inscription.diff.len(),
        inscription.tip.len(),
    );
    println!(
        "chain:    {} inscriptions, back to sequence {}",
        inspected.chain.inscriptions().len(),
        inspected.chain.first().sequence,
    );
    println!("layers:   {}", inspected.chain.descriptors().count());
    let mut records = 0u64;
    for (descriptor, compressed) in inspected.chain.descriptors().zip(&inspected.compressed) {
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
        inspected.chain.uncompressed_size(),
        total_compressed,
    );
    Ok(())
}

fn run_rebuild(genesis: &Genesis, args: &RebuildArgs, auth: ResolveReadAuth) -> miette::Result<()> {
    let Some(source) = &args.source else {
        unreachable!("clap requires --source without --write or a subcommand");
    };
    let read = |path: &Path| {
        std::fs::read_to_string(path)
            .into_diagnostic()
            .with_context(|| format!("reading {}", path.display()))
    };
    let blocks = match &args.blocks_journal {
        Some(path) => Some(rebuild::read_journal(&read(path)?).into_diagnostic()?),
        None => None,
    };
    let kept = match &args.kept_tips_file {
        Some(path) => rebuild::read_epochs(&read(path)?).into_diagnostic()?,
        None => Default::default(),
    };
    let options = rebuild::Options {
        source: source.clone(),
        insecure: args.insecure,
        auth: auth(source)?,
        blocks,
        kept,
    };

    // Written beside its name and moved there once whole, so a refused run
    // leaves no plan to execute.
    let partial = args
        .plan
        .as_ref()
        .map(|path| path.with_extension("partial"));
    let mut file = match &partial {
        Some(path) => Some(std::io::BufWriter::new(
            std::fs::File::create(path)
                .into_diagnostic()
                .with_context(|| format!("creating {}", path.display()))?,
        )),
        None => None,
    };
    let outcome = rebuild::run(
        &options,
        genesis,
        file.as_mut().map(|file| file as &mut dyn std::io::Write),
    )
    .and_then(|summary| {
        file.take()
            .map(|mut file| file.flush())
            .transpose()
            .map_err(rebuild::Error::Write)?;
        Ok(summary)
    });
    let summary = match (outcome, &partial, &args.plan) {
        (Ok(summary), Some(partial), Some(plan)) => {
            std::fs::rename(partial, plan)
                .into_diagnostic()
                .with_context(|| format!("moving the plan to {}", plan.display()))?;
            summary
        }
        (Ok(summary), _, _) => summary,
        (Err(error), partial, _) => {
            if let Some(partial) = partial {
                let _ = std::fs::remove_file(partial);
            }
            return Err(match error {
                rebuild::Error::SelfCheck { .. } => miette::Report::new(Mismatch(Box::new(error))),
                error => miette::Report::from_err(error),
            });
        }
    };

    // With the digests on stdout, the rest goes to stderr.
    let say = |line: String| match args.print_digests {
        true => eprintln!("{line}"),
        false => println!("{line}"),
    };
    say(format!(
        "{source}: epoch-{} to epoch-{}, every sequence self-checked",
        summary.first, summary.last
    ));
    say(format!(
        "layers: {} diff and {} tip; dropped {} closed-epoch blocks and {} retained dumps",
        summary.diff_layers, summary.tip_layers, summary.dropped_blocks, summary.dropped_dumps
    ));
    let kept: Vec<String> = summary.kept.iter().map(u64::to_string).collect();
    say(format!("kept tips: {}", kept.join(", ")));
    say(match &args.plan {
        Some(plan) => format!("plan: {}", plan.display()),
        None if args.blocks_journal.is_none() => {
            "no blocks journal: the assignment and the self-check only".to_owned()
        }
        None => "no plan written".to_owned(),
    });
    if args.print_digests {
        for (sequence, digest) in &summary.digests {
            println!("{sequence} {digest}");
        }
    }
    Ok(())
}

/// `--monolithic-max-mib` in bytes, as the transport takes it.
fn monolithic_max(mib: Option<u64>) -> Option<u64> {
    mib.map(|mib| mib.saturating_mul(1024 * 1024))
}

impl RebuildBlocksArgs {
    /// What `rebuild blocks` runs with, reading the source as `auth`.
    fn options(
        &self,
        auth: Auth,
        upload: Option<rebuild::blocks::Upload>,
    ) -> rebuild::blocks::Options {
        rebuild::blocks::Options {
            source: self.source.clone(),
            auth,
            insecure: self.insecure,
            upload,
            scratch_dir: self.scratch_dir.clone(),
            monolithic_max: monolithic_max(self.monolithic_max_mib),
        }
    }
}

impl RebuildArgs {
    /// What `rebuild --write` writes with.
    fn write_options(
        &self,
        network_magic: u64,
        auth: Auth,
        direct_s3: Option<Box<stelae::oci::s3_upload::Config>>,
    ) -> rebuild::write::Options {
        rebuild::write::Options {
            network_magic,
            root: self.root.clone(),
            insecure: self.insecure,
            auth,
            direct_s3,
            scratch_dir: self.scratch_dir.clone(),
            monolithic_max: monolithic_max(self.monolithic_max_mib),
        }
    }
}

fn run_rebuild_blocks(
    config: &RootConfig,
    genesis: &Genesis,
    args: &RebuildBlocksArgs,
    auth: ResolveAuth,
    read_auth: ResolveReadAuth,
) -> miette::Result<()> {
    use rebuild::blocks;

    let digests = match &args.mithril_digests {
        Some(path) => {
            let bytes = std::fs::read(path)
                .into_diagnostic()
                .with_context(|| format!("reading {}", path.display()))?;
            blocks::ChunkDigests::parse(&bytes).into_diagnostic()?
        }
        None => {
            let aggregator = config::mithril_aggregator(config, args.mithril_aggregator.clone())
                .into_diagnostic()?;
            blocks::fetch(&aggregator).into_diagnostic()?
        }
    };
    let journal = match std::fs::read_to_string(&args.journal) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(error)
                .into_diagnostic()
                .with_context(|| format!("reading {}", args.journal.display()))
        }
    };
    let upload = match &args.upload {
        Some(root) => Some(blocks::Upload {
            root: root.clone(),
            auth: auth()?,
        }),
        None => None,
    };
    let options = args.options(read_auth(&args.source)?, upload);
    let mut append = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.journal)
        .into_diagnostic()
        .with_context(|| format!("opening {}", args.journal.display()))?;

    let summary = blocks::run(&options, genesis, &digests, &journal, &mut append).map_err(
        |error| match error {
            blocks::Error::Mithril { .. } => miette::Report::new(Mismatch(Box::new(error))),
            error => miette::Report::from_err(error),
        },
    )?;

    println!(
        "{}: epoch-1 to epoch-{}, {} resumed from the journal, {} re-encoded",
        args.source, summary.last, summary.resumed, summary.epochs
    );
    println!(
        "compared with Mithril: {} immutable files, 0 mismatches",
        summary.files
    );
    println!(
        "layers: {} uncompressed bytes, {} compressed bytes",
        summary.uncompressed_bytes, summary.compressed_bytes
    );
    println!(
        "{}; journal: {}",
        match &args.upload {
            Some(root) => format!("uploaded to the blocks repository of {root}"),
            None => "nothing written to a registry".to_owned(),
        },
        args.journal.display()
    );
    Ok(())
}

fn run_rebuild_write(
    genesis: &Genesis,
    args: &RebuildArgs,
    auth: ResolveAuth,
) -> miette::Result<()> {
    let Some(path) = &args.plan else {
        unreachable!("clap requires --plan with --write");
    };
    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading {}", path.display()))?;
    let plan = rebuild::write::Plan::read(&text).into_diagnostic()?;
    let options = args.write_options(
        u64::from(genesis.network_magic()),
        auth()?,
        read_direct_s3(args.direct_s3_config.as_deref())?,
    );

    let written = rebuild::write::write(&plan, &options).map_err(|error| match error {
        rebuild::write::Error::Resolved { .. } => miette::Report::new(Mismatch(Box::new(error))),
        error => miette::Report::from_err(error),
    })?;

    println!(
        "{}: epoch-{} to epoch-{}, resolved as planned; latest names {}",
        written.target, written.first, written.last, written.identity
    );
    println!(
        "blobs: {} copied, {} pulled, {} already there",
        written.blobs_copied, written.blobs_pulled, written.blobs_present
    );
    println!(
        "tags: {} written, {} already there",
        written.tags_written, written.tags_present
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
        Command::Digest(args) => run_digest(&config, &genesis, &args, &read_auth),
        Command::Verify(args) => run_verify(&config, &genesis, &args, &read_auth),
        Command::Inspect(args) => run_inspect(&config, &args, &read_auth),
        Command::Rebuild(RebuildArgs {
            profile: Some(RebuildProfile::Blocks(args)),
            ..
        }) => run_rebuild_blocks(&config, &genesis, &args, &auth, &read_auth),
        Command::Rebuild(args) if args.write => run_rebuild_write(&genesis, &args, &auth),
        Command::Rebuild(args) => run_rebuild(&genesis, &args, &read_auth),
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
            "compatibility = \"v2-beta.1\"\n[chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n[storage]\npath = {:?}\n",
            dir.join("data").display().to_string(),
        );
        std::fs::write(&path, document).unwrap();
        path.display().to_string()
    }

    /// A password with no user: refused wherever credentials are resolved.
    fn orphan_password(name: &str) -> Option<String> {
        (name == PASSWORD_ENV).then(|| "secret".to_owned())
    }

    /// The batching flags as `backfill` reads them, on an otherwise valid
    /// command line.
    fn limits(flags: &[&str]) -> miette::Result<Limits> {
        let mut args = vec![
            "backfill",
            "--repo",
            "oci://registry.invalid/cardano/preview",
        ];
        args.extend_from_slice(flags);
        match parse(&args).unwrap().command {
            Command::Backfill(args) => args.limits(),
            _ => unreachable!("parsed a backfill"),
        }
    }

    #[test]
    fn rebuild_takes_the_dolos_lineage_by_its_flags_and_blocks_as_a_subcommand() {
        const OLD: &str = "oci://registry.invalid/cardano/preview";

        match parse(&["rebuild", "--source", OLD]).unwrap().command {
            Command::Rebuild(RebuildArgs {
                profile: None,
                source: Some(source),
                ..
            }) => assert_eq!(source.to_string(), OLD),
            other => panic!("{other:?}"),
        }

        let blocks = ["rebuild", "blocks", "--source", OLD, "--journal", "j"];
        match parse(&blocks).unwrap().command {
            Command::Rebuild(RebuildArgs {
                profile: Some(RebuildProfile::Blocks(args)),
                source: None,
                ..
            }) => {
                assert_eq!(args.journal, PathBuf::from("j"));
                assert!(args.upload.is_none());
            }
            other => panic!("{other:?}"),
        }

        for refused in [
            ["rebuild"].as_slice(),
            ["rebuild", "blocks", "--source", OLD].as_slice(),
            [
                "rebuild",
                "--source",
                OLD,
                "blocks",
                "--source",
                OLD,
                "--journal",
                "j",
            ]
            .as_slice(),
        ] {
            assert!(parse(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_batch_of_one_is_the_default_and_takes_no_budget() {
        assert_eq!(limits(&[]).unwrap(), Limits::ONE);
        assert_eq!(limits(&["--batch-epochs", "1"]).unwrap(), Limits::ONE);
        for flags in [
            ["--batch-scratch-mib", "10"].as_slice(),
            ["--batch-reserve-mib", "10"].as_slice(),
            ["--batch-epochs", "1", "--batch-scratch-mib", "10"].as_slice(),
        ] {
            let error = limits(flags).unwrap_err().to_string();
            assert!(error.contains("go together"), "{flags:?}: {error}");
        }
    }

    #[test]
    fn a_larger_batch_needs_its_budget_and_reads_it_in_mib() {
        for flags in [
            ["--batch-epochs", "4"].as_slice(),
            ["--batch-epochs", "4", "--batch-scratch-mib", "10"].as_slice(),
        ] {
            let error = limits(flags).unwrap_err().to_string();
            assert!(error.contains("go together"), "{flags:?}: {error}");
        }
        let read = limits(&[
            "--batch-epochs",
            "4",
            "--batch-scratch-mib",
            "10",
            "--batch-reserve-mib",
            "1",
        ])
        .unwrap();
        assert_eq!(read.max_epochs.get(), 4);
        assert_eq!(read.scratch, 10 * 1024 * 1024);
        assert_eq!(read.reserve_per_epoch, 1024 * 1024);
    }

    #[test]
    fn the_band_flag_is_gone() {
        for command in ["publish", "digest", "verify"] {
            let error = parse(&[command, "--index-band", "4"]).err().unwrap();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{command}"
            );
        }
    }

    #[test]
    fn digest_and_reproduction_take_a_scratch_directory() {
        let cli = parse(&["digest", "--scratch-dir", "/spool"]).unwrap();
        let Command::Digest(digest) = cli.command else {
            unreachable!("parsed a digest")
        };
        assert_eq!(digest.scratch_dir, Some(PathBuf::from("/spool")));

        // Only a reproduction spools.
        let plain = parse(&[
            "verify",
            "--repo",
            "file:///snapshot",
            "--scratch-dir",
            "/spool",
        ]);
        assert!(plain.is_err());
    }

    /// Each named what a snapshot inherited or retained, which the diff chain
    /// no longer has.
    #[test]
    fn the_flags_that_lost_their_meaning_are_gone() {
        let repo = ["--repo", "oci://registry.invalid/cardano/preview"];
        let removed: [(&str, &[&str]); 9] = [
            ("publish", &["--state-epochs-file", "/epochs"]),
            ("publish", &["--epochs", "1..2"]),
            ("publish", &["--rebuild"]),
            ("publish", &["--verify-carried"]),
            ("backfill", &["--state-epochs-file", "/epochs"]),
            ("backfill", &["--verify-carried"]),
            ("run", &["--state-epochs-file", "/epochs"]),
            ("verify", &["--reproduce", "--epochs", "1..2"]),
            ("verify", &["--reproduce", "--state-epochs-file", "/epochs"]),
        ];
        for (command, flags) in removed {
            let args: Vec<&str> = std::iter::once(command)
                .chain(repo)
                .chain(flags.iter().copied())
                .collect();
            let error = parse(&args).err().unwrap();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{command} {flags:?}"
            );
            parse(&args[..3]).unwrap();
        }
        for flags in [
            ["--chain-from", "/inscription.json"].as_slice(),
            ["--epochs", "1..2"].as_slice(),
            ["--state-epochs-file", "/epochs"].as_slice(),
        ] {
            let args: Vec<&str> = std::iter::once("digest")
                .chain(flags.iter().copied())
                .collect();
            let error = parse(&args).err().unwrap();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "digest {flags:?}"
            );
        }
        parse(&["initialize", "--source", "file:///snapshot"]).unwrap();
    }

    /// A digest's link and blocks come from a network root, a digest given in
    /// their place, or neither; a verification's link only with --reproduce.
    #[test]
    fn digest_and_reproduction_take_their_previous_link() {
        const DIGEST: &str =
            "sha256:0000000000000000000000000000000000000000000000000000000000000001";
        let Command::Digest(digest) = parse(&[
            "digest",
            "--repo",
            "file:///root",
            "--previous",
            DIGEST,
            "--blocks",
            DIGEST,
        ])
        .unwrap()
        .command
        else {
            unreachable!("parsed a digest")
        };
        assert!(digest.repo.is_some());
        assert_eq!(digest.previous, Some(DIGEST.parse().unwrap()));
        assert_eq!(digest.blocks, Some(DIGEST.parse().unwrap()));
        assert!(parse(&["digest", "--previous", "not-a-digest"]).is_err());
        assert!(parse(&["digest", "--insecure"]).is_err());

        let repo = ["verify", "--repo", "file:///root", "--previous", DIGEST];
        assert!(parse(&repo).is_err());
        let reproduce: Vec<&str> = repo.into_iter().chain(["--reproduce"]).collect();
        parse(&reproduce).unwrap();
    }

    /// Dropped on the way to the driver, these would silently publish one
    /// boundary at a time.
    #[test]
    fn backfill_options_carry_the_batch_limits() {
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();

        let cli = parse(&[
            "backfill",
            "--repo",
            "oci://registry.invalid/cardano/preview",
            "--batch-epochs",
            "4",
            "--batch-scratch-mib",
            "10",
            "--batch-reserve-mib",
            "1",
        ])
        .unwrap();
        let Command::Backfill(args) = cli.command else {
            panic!("parsed another command");
        };
        let options = args.options(&config, &|| Ok(Auth::Anonymous)).unwrap();
        assert_eq!(options.limits, args.limits().unwrap());
        assert_eq!(options.limits.max_epochs.get(), 4);
    }

    /// `--monolithic-max-mib` on each uploading command, read in MiB: absent
    /// is the transport's default, `0` is passed on to stream every layer, and
    /// a value above the upload memory is passed on for the transport to clamp.
    #[test]
    fn the_uploading_commands_take_the_single_request_size_in_mib() {
        const OLD: &str = "oci://registry.invalid/cardano/preview";
        const MIB: u64 = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();

        let given = |flag: &[&str], command: &[&str]| -> Option<u64> {
            let args: Vec<&str> = command.iter().chain(flag).copied().collect();
            match parse(&args).unwrap().command {
                Command::Run(args) => {
                    let (_, backfill) = args.options(&config, &|| Ok(Auth::Anonymous)).unwrap();
                    backfill.monolithic_max
                }
                Command::Rebuild(RebuildArgs {
                    profile: Some(RebuildProfile::Blocks(args)),
                    ..
                }) => args.options(Auth::Anonymous, None).monolithic_max,
                Command::Rebuild(args) => {
                    args.write_options(2, Auth::Anonymous, None).monolithic_max
                }
                other => panic!("{other:?}"),
            }
        };
        let above_budget = stelae::oci::DEFAULT_UPLOAD_MEMORY / MIB * 4;
        let above = above_budget.to_string();
        for command in [
            ["run", "--repo", OLD].as_slice(),
            ["rebuild", "blocks", "--source", OLD, "--journal", "j"].as_slice(),
            ["rebuild", "--write", "--plan", "p"].as_slice(),
        ] {
            assert_eq!(given(&[], command), None, "{command:?}");
            assert_eq!(
                given(&["--monolithic-max-mib", "0"], command),
                Some(0),
                "{command:?}"
            );
            assert_eq!(
                given(&["--monolithic-max-mib", "512"], command),
                Some(512 * MIB),
                "{command:?}"
            );
            assert_eq!(
                given(&["--monolithic-max-mib", &above], command),
                Some(above_budget * MIB),
                "{command:?}"
            );
        }

        // Planning writes nothing, so it has no upload to size.
        assert!(parse(&["rebuild", "--source", OLD, "--monolithic-max-mib", "512"]).is_err());
    }

    #[test]
    fn run_restores_with_its_inputs_and_the_registry_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = config::load(Some(Path::new(&node(dir.path())))).unwrap();
        let scratch = dir.path().join("scratch");
        let basic = Auth::Basic {
            user: "publisher".to_owned(),
            password: "secret".to_owned(),
        };
        let auth = || Ok(basic.clone());

        let repo = ["--repo", "oci://registry.invalid/cardano/preview"];
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
            assert_eq!(
                backfill.scratch_dir,
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

        let cli = parse(&[
            "--config",
            &config,
            "run",
            "--repo",
            "oci://registry.invalid/cardano/preview",
            "--window",
            "0",
            "--restore-source",
            "file:///missing-snapshot",
        ])
        .unwrap();
        let error = format!("{:?}", execute(cli, &|_| None).unwrap_err());
        assert!(error.contains("--window must be at least 1"), "{error}");
        assert!(!dir.path().join("data").exists());
    }

    #[test]
    fn a_command_off_the_registry_ignores_its_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let config = node(dir.path());
        let output = dir.path().join("snapshot");

        let from_directory = parse(&[
            "--config",
            &config,
            "initialize",
            "--source",
            "file:///missing-snapshot",
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
        let repo = "oci://registry.invalid/cardano/preview";

        let commands: [&[&str]; 7] = [
            &["initialize", "--source", repo],
            &["verify", "--repo", repo],
            &["inspect", "--repo", repo],
            &["digest", "--repo", repo],
            &["publish", "--repo", repo],
            &["backfill", "--repo", repo],
            &[
                "run",
                "--repo",
                repo,
                "--restore-source",
                "file:///missing-snapshot",
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

        // The catalog's user, which takes the official password with it.
        let anonymous = CredentialArgs::default();
        let catalog_user = stelae_cardano::registry_auth(&RegistryAuth::from_inputs(
            Some("stelae".to_owned()),
            None,
            None,
        ))
        .unwrap();
        assert!(matches!(&catalog_user, Auth::Basic { password, .. } if !password.is_empty()));
        assert_eq!(
            read(&anonymous, &official, &|_| None).unwrap(),
            catalog_user
        );
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

        // A scalar Dolos's parser takes as a string is taken here too.
        std::fs::write(&file, "password = 12345678\n").unwrap();
        assert_eq!(
            with_file.auth_from(&|_| None).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "12345678".to_owned(),
            }
        );

        // Refused without quoting the line, which is a secret.
        for document in [
            "secret = \"hunter2\"\n",
            "password = hunter2\n",
            "[password]\nhunter2 = 1\n",
        ] {
            std::fs::write(&file, document).unwrap();
            let error = format!("{:?}", with_file.auth_from(&|_| None).unwrap_err());
            assert!(error.contains("naming only"), "{error}");
            assert!(!error.contains("hunter2"), "{error}");
        }
    }

    #[test]
    fn the_credentials_file_is_taken_after_the_command() {
        for command in ["verify", "inspect"] {
            let cli = parse(&[
                command,
                "--repo",
                "file:///snapshot",
                "--registry-credentials-file",
                "/run/secrets/stelae.toml",
            ])
            .unwrap();
            assert_eq!(
                cli.credentials.registry_credentials_file.as_deref(),
                Some(Path::new("/run/secrets/stelae.toml")),
                "{command}"
            );
        }
    }
}

/// Do not render parser diagnostics: they can quote credential values.
fn read_direct_s3(
    path: Option<&Path>,
) -> miette::Result<Option<Box<stelae::oci::s3_upload::Config>>> {
    path.map(|path| {
        let bytes = std::fs::read(path).into_diagnostic()?;
        serde_json::from_slice(&bytes)
            .map_err(|_| miette::miette!("invalid direct S3 configuration"))
    })
    .transpose()
}

#[cfg(test)]
mod direct_s3_cli_tests {
    #[test]
    fn invalid_credentials_are_never_quoted_by_parse_errors() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            br#"{"secret_access_key":"do-not-print-me","unexpected":true}"#,
        )
        .unwrap();
        let error = super::read_direct_s3(Some(file.path())).unwrap_err();
        assert_eq!(error.to_string(), "invalid direct S3 configuration");
        assert!(!format!("{error:?}").contains("do-not-print-me"));
    }
}
