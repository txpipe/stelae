# Stelae Cardano publisher

`stelae-publisher` owns the Cardano publisher process. Its
`stelae-cardano` integration crate contains the publish-before-advance loop and
repository policy; Dolos supplies the ledger engine, storage backends, Mithril
client and `io.txpipe.dolos.cardano` profile through supported library facades.
No command starts or shells out to a Dolos executable.

## Source and dependency identity

All Dolos crates in the normal publisher dependency graph are pinned to
`e184a965644bcd7a4317097eda94206bd50a35d7`, a Dolos `main` revision carrying
the v2 configuration contract: bootstrap and publisher inputs on the
invocation ([PR 1466](https://github.com/txpipe/dolos/pull/1466)), the v2
schema and loader ([PR 1468](https://github.com/txpipe/dolos/pull/1468)),
store lifecycle ([PR 1479](https://github.com/txpipe/dolos/pull/1479)) and
the Fjall `standard` preset ([PR 1478](https://github.com/txpipe/dolos/pull/1478)).
Like every revision since the publisher retirement of
[PR 1335](https://github.com/txpipe/dolos/pull/1335), it retains the headless
ledger, storage, profile, restore and repository facades but contains no
publisher command, backfill loop or publisher policy module. The root `dolos`
package is imported with default features disabled, so its service features
are absent from the publisher graph.

The workspace patch for `https://github.com/txpipe/stelae` resolves the
profile's v0.2.0 dependencies back to the workspace `stelae` and
`stelae-driver` packages. This prevents two same-version protocol type
universes.

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

## Configuration

The node's lasting description is a Dolos v2 `dolos.toml`, read by Dolos's own
loader: the file named by global `--config`, else `./dolos.toml`, with
`DOLOS_` environment variables using `__` for nesting (for example
`DOLOS_STORAGE__PATH`) overriding it. Unknown fields are rejected, and so is a
network the Dolos catalog does not know. A known network (`mainnet`,
`preprod`, `preview`) takes its embedded genesis; a `custom` one names its
genesis files under `[chain.cardano.genesis]`. The rolling `history.window`
bounds how much chain history a restore reads.

What Dolos takes from an invocation rather than from that file, the publisher
takes the same way:

| Input | Source | Default |
| --- | --- | --- |
| Retained state-dump epochs | `--state-epochs-file PATH`, one epoch per line, ascending | none: `publish`, `backfill`, `run`, `digest` and `verify --reproduce` require it; an empty file retains the tip alone |
| Mithril aggregator | `--mithril-aggregator` or `DOLOS_MITHRIL_AGGREGATOR` | the known network's; required for `custom` |
| Mithril genesis key | `--mithril-genesis-vkey` or `DOLOS_MITHRIL_GENESIS_VKEY` | the known network's; required for `custom` |
| Registry user | global `--registry-user` or `DOLOS_STELAE_USER` | anonymous |
| Registry secret | `DOLOS_STELAE_PASSWORD` or `DOLOS_STELAE_TOKEN`, never an argument | none; a user with no secret takes the official registry's read-only password |

The retained epochs are echoed into every inscription's parameters as signed
input (decision 0026), so they are validated (ascending, no repeats, no epoch
0) before any store opens. A command that touches no registry (`publish
--output-dir`, `initialize --source file://…`, `digest`, `verify` or
`inspect` of a `file://` stele) reads no registry credentials. A token and user together, or a password with no
user, fail through the shared profile credential policy. CLI command names
belong to this executable; profile names, media types, tags and repository
identities remain the existing wire values.

## Commands

Publish the committed stores once to a directory:

```sh
stelae-publisher --config dolos.toml publish --output-dir ./stele \
  --state-epochs-file state-epochs
```

Publish once to a repository:

```sh
stelae-publisher --config dolos.toml publish \
  --repo oci://registry.example/cardano/preview \
  --state-epochs-file state-epochs \
  --concurrency 4
```

`publish` preserves epoch selection, index-band and producer tuning,
`--rebuild`, `--verify-carried`, `--dry-run`, and `--require-new`. An
up-to-date repository is a successful no-op unless `--require-new` is set.

Initialize empty stores and then backfill:

```sh
stelae-publisher --config dolos.toml run \
  --repo oci://registry.example/cardano/preview \
  --state-epochs-file state-epochs \
  --concurrency 4
```

`run` restores `latest` from the publication repository before replay. Use
`--restore-source` to restore elsewhere. Restore failure is fatal and partial
stores are cleared. `--allow-genesis` is the only genesis fallback and is for
a deliberately verified first publication. `initialize`, `publish`, and
`backfill` are also available independently.

### Reading and reproducing steles

`digest`, `verify` and `inspect` replace Dolos's former `snapshot` commands
over the same profile entry points. `--repo` takes `oci://HOST/PATH` or
`file://DIR`; registry credentials are resolved only for an `oci://` source.

Compute what the committed stores would publish, writing nothing. The stores
must stand on the first block of an epoch. The plan is reported on stderr and
the canonical inscription goes to stdout (or `--output FILE`), so it hashes to
the identity reported; `--chain-from FILE` extends a predecessor's history:

```sh
stelae-publisher --config dolos.toml digest \
  --state-epochs-file state-epochs > stele.json
```

Check a published stele's digests, every layer streamed against both of its
digests; `--reproduce` also rebuilds every layer from the committed stores and
requires a byte-identical inscription, taking `--epochs`, `--index-band`,
`--producers` and the `--state-epochs-file` the stele was published with:

```sh
stelae-publisher --config dolos.toml verify \
  --repo oci://registry.example/cardano/preview --point epoch-1200 \
  --reproduce --state-epochs-file state-epochs
```

Print a stele's table of contents without pulling a layer:

```sh
stelae-publisher --config dolos.toml inspect \
  --repo oci://registry.example/cardano/preview
```

All three take `--format text|json` (default `text`): data on stdout,
progress and logs on stderr. `inspect --format json` is the canonical
inscription verbatim, what `digest --chain-from` takes; `digest` writes the
inscription in either format; `verify --format json` is one report object.
They exit 0 on success, 1 on an error, 2 on a usage error and 3 when a
verification ran and the stele failed it.

OCI operations run synchronously on the process thread. Mithril calls have a
separate Tokio runtime. SIGINT and SIGTERM cancel acquisition and are observed
between import chunks and retry attempts; an in-flight OCI publication still
finishes before shutdown is observed. Replay sessions explicitly finalize on
success and failure.

## Persisted state and ordering

The following paths are part of restart compatibility:

| Path | Meaning |
| --- | --- |
| `<storage.path>/` | Dolos WAL, state, archive and mempool stores selected by the configuration |
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
- retain the `--state-epochs-file` epochs and the profile's existing plan,
  schema, media-type, compression and journal identities.

## Final ownership boundary

`stelae-cardano/src/backfill.rs` is the only running publisher loop, and
`stelae-cardano/src/publisher.rs` owns the host's repository-standing and
journal policy. It composes Dolos's supported `SnapshotRepository`,
`Publishing` and `SnapshotSource` facades; it does not import a Dolos publisher
or backfill API.

The image build, release identities, chart cutover and compatible rollback
are documented in [publisher-packaging.md](publisher-packaging.md). Building a
candidate does not publish, deploy, modify a network source, or claim production
parity; operational cutover remains a separate step.

The local OCI and host-policy evidence is documented in
[publisher-verification.md](publisher-verification.md).
