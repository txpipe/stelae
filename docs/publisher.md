# Stelae Cardano publisher

`stelae-publisher` owns the Cardano publisher process. Its
`stelae-cardano` integration crate contains the publish-before-advance loop and
repository policy; Dolos supplies the ledger engine, storage backends, Mithril
client and `io.txpipe.dolos.cardano` profile through supported library facades.
No command starts or shells out to a Dolos executable.

## Source and dependency identity

All imported Dolos crates are pinned to
`1ae4e91c18a9e1456a3612af402d7b9b97546d30`, the merge revision of
[Dolos PR 1334](https://github.com/txpipe/dolos/pull/1334). The root `dolos`
package is imported with default features disabled. The workspace patch for
`https://github.com/txpipe/stelae` resolves the profile's v0.2.0 dependencies
back to the workspace `stelae` and `stelae-driver` packages. This prevents two
same-version protocol type universes.

The lockfile is committed. These commands verify the intended graph:

```sh
cargo build --locked --workspace --all-targets --all-features
cargo tree --locked -p stelae -e normal --all-features
cargo tree --locked -p stelae-driver -e normal --all-features
cargo tree --locked -d
cargo deny check advisories bans
```

Whole-workspace builds use Rust 1.93 or newer, as required by the pinned
Mithril dependency graph.

The first two trees must contain no package matching `dolos` or `dolos-*`.
`deny.toml` permits Dolos packages only under `stelae-cardano`; a Dolos edge
from the protocol or generic driver is an error.

## Configuration precedence

The host deliberately keeps the current Dolos configuration names. Sources
are layered from lowest to highest precedence:

1. optional `/etc/dolos/daemon.toml`;
2. optional `./dolos.toml`;
3. the required file named by global `--config`;
4. `DOLOS_*` environment variables.

Registry credentials therefore remain
`DOLOS_STELAE_REGISTRY_USER`, `DOLOS_STELAE_REGISTRY_PASSWORD`, and
`DOLOS_STELAE_REGISTRY_TOKEN`. A token and user together, or a password with
no user, fail through the shared profile credential policy. CLI command names
belong to this executable; profile names, media types, tags and repository
identities remain the existing wire values.

## Commands

Publish the committed stores once to a directory:

```sh
stelae-publisher --config dolos.toml publish --output-dir ./stele
```

Publish once to a repository:

```sh
stelae-publisher --config dolos.toml publish \
  --repo oci://registry.example/cardano/preview \
  --concurrency 4
```

`publish` preserves epoch selection, index-band and producer tuning,
`--rebuild`, `--verify-carried`, `--dry-run`, and `--require-new`. An
up-to-date repository is a successful no-op unless `--require-new` is set.

Initialize empty stores and then backfill:

```sh
stelae-publisher --config dolos.toml run \
  --repo oci://registry.example/cardano/preview \
  --concurrency 4
```

`run` restores `latest` from the publication repository before replay. Use
`--restore-source` to restore elsewhere. Restore failure is fatal and partial
stores are cleared. `--allow-genesis` is the only genesis fallback and is for
a deliberately verified first publication. `initialize`, `publish`, and
`backfill` are also available independently.

OCI operations run synchronously on the process thread. Mithril calls have a
separate Tokio runtime. SIGINT and SIGTERM cancel acquisition and are observed
between import chunks and retry attempts; an in-flight OCI publication still
finishes before shutdown is observed. Replay sessions explicitly finalize on
success and failure.

## Persisted state and ordering

The following paths are part of restart compatibility:

| Path | Meaning |
| --- | --- |
| `<storage.path>/` | Dolos WAL, state, archive and mempool stores selected by the existing config |
| `<storage.path>/.snapshot-publish.json` | completed-layer publication journal used for in-place retry |
| `<storage.path>/.snapshot-restore.json` | immutable-layer restore checkpoint read by `--resume` |
| `<storage.path>/.stelae-publisher-initialized.json` | source, storage path, network magic and restore/genesis decision for cold start |
| `<storage.path>/scratch` | default OCI staging directory |
| `<storage.path>/mithril/immutable` | default bounded Mithril download window |

Before any cleanup, initialization requires an absolute storage path below a
filesystem root. A marker is accepted only when its storage path, network
magic and source match the current configuration. A restore marker without a
committed cursor fails closed. A genesis marker can describe an empty store
only while the current run explicitly opts into genesis.

The backfill state machine preserves these rules from the accepted Dolos host:

- publish a committed epoch boundary before replaying past it;
- prune history only after that publication succeeds;
- retain two immutable files of cleanup/download margin using the configured
  network's genesis chunk geometry;
- retry transient repository work in place using the publication journal;
- retain `snapshot.state_epochs` and the profile's existing plan, schema,
  media-type, compression and journal identities.

## Finite duplication window

Dolos still carries its old `snapshot publish` / `snapshot backfill` command
and backfill module at the pinned revision. This host does not call that
module: `stelae-cardano/src/backfill.rs` owns the running loop. The duplicate is
temporary evidence for parity. Publisher-pipeline step 6 removes the old Dolos
implementation after parity, and step 7 removes the stale dependency pin.

This increment does not ship an image, change a Helm chart, deploy, publish to
a production registry, or claim full production parity. Packaging, operational
cutover, differential live-registry evidence and later pin cleanup remain the
following publisher-pipeline steps.
