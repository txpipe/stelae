# Stelae Cardano publisher

`stelae-publisher` owns the Cardano publisher process. Its
`stelae-cardano` integration crate contains the publish-before-advance loop and
repository policy; Dolos supplies the ledger engine, storage backends, Mithril
client and `io.txpipe.dolos.cardano` profile through supported library facades.
No command starts or shells out to a Dolos executable.

## Source and dependency identity

Every Dolos crate in the publisher's dependency graph, normal and dev, is
pinned to `1db3e41fd04bca64bf245f50b54ca656719764ca`, the merge of
[PR 1553](https://github.com/txpipe/dolos/pull/1553), which adopts the Stelae
snapshot envelope `application/vnd.stelae.snapshot.v1`. `DOLOS_REVISION` in
`stelae-cardano/src/lib.rs` names the same commit.

The `io.txpipe.dolos.cardano` profile was frozen for Dolos v2 earlier, at
`e39a38a31665acb1b5e5701bbb2f8c7cb995cef1`, the merge of
[PR 1548](https://github.com/txpipe/dolos/pull/1548): profile and payload
media-type version 1, payloads named
`application/vnd.dolos.stelae.{kind}.v1+zstd`, every namespace at schema
revision 1, and only the epoch-boundary cut publishable. The pin keeps all
of that, but it is not the baseline plus the envelope alone: the range also
carries [PR 1546](https://github.com/txpipe/dolos/pull/1546), which changes
how `state-datums` counts hash-datum outputs and moves the WAL to version 10.
Initialization does not refuse stores synced at an earlier pin (opening them
only wipes their WAL), so delete a kept claim before the upgrade, as for any
storage pin change (see
[publisher-packaging.md](publisher-packaging.md)). The normative profile is
[`crates/snapshot/PROFILE.md`](https://github.com/txpipe/dolos/blob/1db3e41fd04bca64bf245f50b54ca656719764ca/crates/snapshot/PROFILE.md)
at the pinned commit.

Like every revision since the publisher retirement of
[PR 1335](https://github.com/txpipe/dolos/pull/1335), the pin retains the
headless ledger, storage, profile, restore and repository facades but
contains no publisher command, backfill loop or publisher policy module. The
root `dolos` package is imported with default features disabled, so its
service features are absent from the publisher graph.

The pinned profile depends on Stelae at `55d5270`, the snapshot envelope
rename ([PR 17](https://github.com/txpipe/stelae/pull/17)), versioned `0.5.0`.
The workspace patch for `https://github.com/txpipe/stelae` resolves those
dependencies back to the workspace `stelae` and `stelae-driver` packages, so
the profile and the publisher share one set of protocol types instead of two
of the same version. A Dolos pin whose Stelae reference carries a different
protocol from this workspace needs that skew resolved before it is pinned; the
patch would otherwise hide it.

Steles cut against an earlier pin are not part of this lineage: the frozen
profile renamed its payload media types and reset every schema revision to
1, and restoring or
chaining onto an older stele is unsupported. A repository that holds one
starts over (see [First publication](#first-publication)).

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
genesis files under `[chain.cardano.genesis]`.

A publisher keeps the full archive: `[history] mode` stays `"full"`, its
default. Dolos cuts a boundary only from an archive whose first block is the
chain's origin, so a `rolling` window would publish until its first prune and
then refuse every later boundary. `initialize`, `backfill` and `run` refuse a
`rolling` or `none` history before they open a store.

What Dolos takes from an invocation rather than from that file, the publisher
takes the same way:

| Input | Source | Default |
| --- | --- | --- |
| Retained state-dump epochs | `--state-epochs-file PATH`, one epoch per line, ascending | none: `publish`, `backfill`, `run`, `digest` and `verify --reproduce` require it; an empty file retains the tip alone |
| Mithril aggregator | `--mithril-aggregator` or `DOLOS_MITHRIL_AGGREGATOR` | the known network's; required for `custom` |
| Mithril genesis key | `--mithril-genesis-vkey` or `DOLOS_MITHRIL_GENESIS_VKEY` | the known network's; required for `custom` |
| Registry user | global `--registry-user` or `DOLOS_STELAE_USER` | anonymous; `verify` and `inspect` with no user or token read the configured network's official repository as its published read-only user, as Dolos does |
| Registry secret | `DOLOS_STELAE_PASSWORD` or `DOLOS_STELAE_TOKEN`, or global `--registry-credentials-file PATH` (a TOML file naming only `password` or `token`, in their place); never an argument | none; a user with no secret takes the official registry's read-only password |

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

Print a stele's table of contents without pulling a layer. A directory stele
published before `blobs.json` existed reports its compressed sizes as unknown
rather than reading every blob to find them:

```sh
stelae-publisher --config dolos.toml inspect \
  --repo oci://registry.example/cardano/preview
```

All three take `--format text|json` (default `text`): data on stdout,
progress and logs on stderr. `inspect --format json` is the canonical
inscription verbatim, what `digest --chain-from` takes; `digest` writes the
inscription in either format; `verify --format json` is one report object.
They exit 0 on success, 1 on an error, 2 on a usage error and 3 when `verify`
refused the stele: a document or layer whose bytes, digests, names or
encoding disagree, including a blob a registry serves under another digest
or past its descriptor's size and a blob that does not decompress,
an inscription of another profile or schema, a broken history, or, under
`--reproduce`, a stele of another network or sequence than the stores or a
reproduction that differs. A check that could not run, because the registry,
the disk, the credentials, the configuration or the local stores failed,
exits 1, including a record the local stores hold out of order or malformed
under `--reproduce`, as does a `--point` a directory stele does not hold and
any refusal by `inspect` or `digest`.

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
- retain the `--state-epochs-file` epochs, and the plan, schema, media-type,
  compression and journal identities of the pinned profile.

## First publication

A fresh lineage starts from an empty repository and replays from genesis.
Every inscription after the first names its predecessor and carries its
history forward, so the first stele is the only one published without a
predecessor.

1. Stop every other writer to the repository and wait for it to exit.
   Configure the node with full history (see
   [Configuration](#configuration)); its stores grow with the chain, to
   about 250 GiB on mainnet.
2. Delete every tag in the repository, `latest` first, so no earlier pin's
   stele is restored or chained onto.
3. Start the first run with `--allow-genesis`. Initialization finds no
   `latest`, opts into genesis and replays from block 0, publishing each
   epoch boundary before it replays past it.
4. Run every later invocation without `--allow-genesis`. Initialization then
   restores the published `latest` (or reuses the committed stores) and
   fails closed if it cannot, instead of silently replaying from genesis.

On the chart these are `allowGenesisFallback: true` for the first run and
`false` from the next one on; see
[publisher-packaging.md](publisher-packaging.md#first-publication-on-the-chart).

## Final ownership boundary

`stelae-cardano/src/backfill.rs` is the only running publisher loop, and
`stelae-cardano/src/publisher.rs` owns the host's repository-standing and
journal policy. It composes Dolos's supported `SnapshotRepository`,
`Publishing` and `SnapshotSource` facades; it does not import a Dolos publisher
or backfill API.

The image build, release identities and a fresh lineage's first run on the
chart are documented in [publisher-packaging.md](publisher-packaging.md).
Building a candidate does not publish, deploy or modify a network source;
deployment remains a separate step.

The local OCI and host-policy evidence is documented in
[publisher-verification.md](publisher-verification.md).

## Direct S3 blob uploads to Zot (opt-in)

`publish --repo`, `backfill` and `run` accept `--direct-s3-config PATH`.
The file is JSON containing `endpoint`, `bucket`, `prefix`, `region`,
`access_key_id` and `secret_access_key`. Keep it outside source control,
readable only by the publisher identity; mount it from a Secret in Kubernetes.
It is read by the binary, not discovered from environment by the protocol.
Set Helm `directS3SecretName` to that Secret name (key `direct-s3.json`);
changing the publisher pod template requires the usual run-counter bump.
Malformed configuration diagnostics and Debug output do not print credentials.
The endpoint must use HTTPS. R2's region is `auto`.

This mode targets **Zot with deduplication and garbage collection disabled**.
`prefix` is the actual physical object prefix before the repository name,
without the repository itself. For the operated bucket-root layout it is `""`.
If the registry's S3 driver duplicates a configured prefix, include both copies
here. Prove the mapping against an isolated repository before operational use.
A wrong mapping makes manifest validation fail; there is no automatic fallback.

```text
stelae-publisher --config dolos.toml run \
  --repo oci://registry.example/cardano/preprod \
  --state-epochs-file state-epochs \
  --direct-s3-config /run/secrets/direct-s3.json --concurrency 4
```

The existing concurrency bound covers both transports. Layers and the canonical
inscription are uploaded directly to `<prefix>/<repository>/blobs/sha256/<hex>`.
Files are SHA256/size checked before upload; large files use multipart uploads
with bounded buffers and abort on failure. Existing objects are checked for
size before reuse. Multipart ETags are not treated as SHA256 digests. S3 request
retries are handled by the storage client; publication failures still prevent
sealing and remain eligible for the host's existing retry/resume behavior.

The registry still handles predecessor reads, carried-layer verification,
manifest validation, immutable epoch tags and `latest`. A failed blob upload
never reaches manifest sealing. This preserves the snapshot/manifest format,
profile identity, lineage and ordinary OCI readers. Because Zot commits the
metadata through its API, this hybrid mode needs no out-of-band index writes
or metadata rebuild. It requires S3 credentials in addition to the registry
credentials and is not compatible with arbitrary registry storage layouts.

The live opt-in transport test is:

```text
STELAE_TEST_REGISTRY_URL=oci.example.com \
STELAE_TEST_DIRECT_S3_CONFIG=/run/secrets/direct-s3.json \
cargo test --locked -p stelae --all-features --test oci \
  direct_s3_blobs_are_readable_through_zot -- --ignored --nocapture
```

Provide registry credentials through the fixture's documented environment
variables. The test creates a fresh staging repository, publishes through S3,
and reads the inscription and layer records back through ordinary Zot/OCI.
It measures correctness, not mainnet-scale throughput; do not switch a running
publisher on the strength of this test alone.
