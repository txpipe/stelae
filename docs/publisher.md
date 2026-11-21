# Stelae Cardano publisher

`stelae-publisher` owns the Cardano publisher process. Its
`stelae-cardano` integration crate contains the backfill loop and repository
policy; Dolos supplies the ledger engine, storage backends, Mithril
client and `io.txpipe.dolos.cardano` profile through supported library facades.
No command starts or shells out to a Dolos executable.

## Source and dependency identity

Every Dolos crate in the publisher's dependency graph, normal and dev, is
pinned to `c31f215cfcc48e5a1f57176914eefd932af273ee`, the merge of
[PR 1650](https://github.com/txpipe/dolos/pull/1650). It records and admits
the `v2-beta.1` store contract
([PR 1646](https://github.com/txpipe/dolos/pull/1646)), so it opens only
stores and configurations that name `v2-beta.1` (see
[Crossing a store contract](#crossing-a-store-contract)). Its profile is the
one of [PR 1625](https://github.com/txpipe/dolos/pull/1625): it exports the
diff chain ([PR 1610](https://github.com/txpipe/dolos/pull/1610)), leaves
closed epochs' blocks to the blocks profile
([PR 1619](https://github.com/txpipe/dolos/pull/1619)) and names the blocks
snapshot of its sequence in `position`. It builds fjall from the
[txpipe fork](https://github.com/txpipe/fjall) at
`ae794ad448d90214c3491df4c50f59f0fe15cf90` (fjall 3.1.13 on lsm-tree 3.1.10),
as [PR 1599](https://github.com/txpipe/dolos/pull/1599) set it.
The publisher's own `fjall` dependency names the same fork revision, so the
graph holds one fjall. The fork lets a write at the L0 halt, or one waiting on
a worker that has died, return instead of waiting forever (see
[A volume that fills](#a-volume-that-fills)). `DOLOS_REVISION` in
`stelae-cardano/src/lib.rs` names the same commit. Boundary preparation and
staged export came with the merge of
[PR 1564](https://github.com/txpipe/dolos/pull/1564), `bf66dcc6`: an ordinary
export is a batch of one over that machinery, with the same canonical output.
Since that pin, no WAL version or profile version has changed; the store
traits gained a `persist` call, which the publisher does not implement. The
Stelae snapshot envelope `application/vnd.stelae.snapshot.v1` came earlier, with
[PR 1553](https://github.com/txpipe/dolos/pull/1553).

The `io.txpipe.dolos.cardano` profile was frozen for Dolos v2 earlier, at
`e39a38a31665acb1b5e5701bbb2f8c7cb995cef1`, the merge of
[PR 1548](https://github.com/txpipe/dolos/pull/1548): profile and payload
media-type version 1, payloads named
`application/vnd.dolos.stelae.{kind}.v1+zstd`, every namespace at schema
revision 1, and only the epoch-boundary cut publishable. The pin keeps all
of that, but it is not the baseline plus the envelope alone: the range also
carries [PR 1546](https://github.com/txpipe/dolos/pull/1546), which changes
how `state-datums` counts hash-datum outputs and moves the WAL to version 10.
Stores synced before that range lack its `state-datums` counts, and
relabeling them would not add them, so delete a claim kept from before it
rather than relabel it (see
[publisher-packaging.md](publisher-packaging.md)). The normative profile is
[`crates/snapshot/PROFILE.md`](https://github.com/txpipe/dolos/blob/c31f215cfcc48e5a1f57176914eefd932af273ee/crates/snapshot/PROFILE.md)
at the pinned commit, and the preparation contract this host consumes is
[`docs/contracts/export-preparation.md`](https://github.com/txpipe/dolos/blob/c31f215cfcc48e5a1f57176914eefd932af273ee/docs/contracts/export-preparation.md).

Like every revision since the publisher retirement of
[PR 1335](https://github.com/txpipe/dolos/pull/1335), the pin retains the
headless ledger, storage, profile, restore and repository facades but
contains no publisher command, backfill loop or publisher policy module. The
root `dolos` package is imported with default features disabled, so its
service features are absent from the publisher graph.

The pinned profile depends on Stelae at `b2735ed`, which holds a repository
in a directory as an OCI image layout
([PR 36](https://github.com/txpipe/stelae/pull/36)), over the diff chain and
the blocks profile. The workspace patch for `https://github.com/txpipe/stelae`
resolves `stelae`, `stelae-driver` and `stelae-blocks` back to the workspace
packages, so the profile and the publisher share one set of protocol types
instead of two of the same version. A Dolos pin whose Stelae reference carries
a different protocol from this workspace needs that skew resolved before it is
pinned; the patch would otherwise hide it.

Snapshots cut against an earlier pin are not part of this lineage: the frozen
profile renamed its payload media types and reset every schema revision to
1, and the diff chain changed every document in place, at the same versions.
Restoring or chaining onto an older snapshot is unsupported. A root's `dolos`
repository that holds one starts over (see
[First publication](#first-publication)).

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
`rolling` or `none` history before they open a store. An `ephemeral` archive
passes that check and can't publish either: it opens empty every time, so a run
can replay up to the next boundary before its publish refuses, saying the
archive holds no block with the cursor's hash. Keep the archive on `fjall`.

What Dolos takes from an invocation rather than from that file, the publisher
takes the same way:

| Input | Source | Default |
| --- | --- | --- |
| Mithril aggregator | `--mithril-aggregator` or `DOLOS_MITHRIL_AGGREGATOR` | the known network's; required for `custom` |
| Mithril genesis key | `--mithril-genesis-vkey` or `DOLOS_MITHRIL_GENESIS_VKEY` | the known network's; required for `custom` |
| Registry user | global `--registry-user` or `DOLOS_STELAE_USER` | anonymous; `verify`, `inspect` and `freshness` with no user or token read the configured network's official repository as its published read-only user, as Dolos does |
| Registry secret | `DOLOS_STELAE_PASSWORD` or `DOLOS_STELAE_TOKEN`, or global `--registry-credentials-file PATH` (a TOML file naming only `password` or `token`, in their place); never an argument | none; a user with no secret takes the official registry's read-only password |

Every source and destination a command names is a network root, such as
`oci://registry.example/cardano/preview`: the snapshots live in its `dolos`
repository, and the blocks snapshots they name in its `blocks` repository. A
`file://` root, and `publish --output-dir`, hold `dolos/` and `blocks/` OCI
image layouts. A command that touches no registry (`publish --output-dir`,
`initialize --source file://…`, `digest` without an `oci://` `--repo`,
`verify` or `inspect` of a `file://` root) reads no registry credentials. A token and user together, or a password with no
user, fail through the shared profile credential policy. CLI command names
belong to this executable; profile names, media types, tags and repository
identities remain the existing wire values.

## Commands

Publish the committed stores once to a directory:

```sh
stelae-publisher --config dolos.toml publish --output-dir ./preview
```

Publish once to a repository:

```sh
stelae-publisher --config dolos.toml publish \
  --repo oci://registry.example/cardano/preview \
  --concurrency 4
```

`publish` takes producer tuning, `--dry-run` and `--require-new`. An up-to-date
repository is a successful no-op unless `--require-new` is set. A repository
publish names the blocks snapshot the root's `blocks` repository holds at the
same sequence, and refuses, writing nothing, when there is none; a directory
publish writes that blocks snapshot itself, from the archive. `--index-band` is
gone: staging fills every window from one traversal. So are `--epochs`,
`--rebuild`, `--verify-carried` and `--state-epochs-file`: a snapshot lists
only its own layers, so nothing is selected, carried or retained.

Initialize empty stores and then backfill:

```sh
stelae-publisher --config dolos.toml run \
  --repo oci://registry.example/cardano/preview \
  --concurrency 4
```

`run` restores `latest` from the root's `dolos` repository before replay, with
the closed epochs' blocks from its `blocks` repository. `backfill` and `run`
publish each boundary's blocks snapshot themselves, before its Dolos snapshot (see
[Blocks snapshots](#blocks-snapshots)). Use
`--restore-source` to restore elsewhere. Restore failure is fatal and partial
stores are cleared. `--allow-genesis` is the only genesis fallback and is for
a deliberately verified first publication. `initialize`, `publish`, and
`backfill` are also available independently.

`run` and `backfill` take `--monolithic-max-mib M`, the largest layer uploaded
as one request; see [Single-request uploads](#single-request-uploads).

### Single-request uploads

A layer of up to 100 MB goes to the registry as one `POST` and one `PUT`
carrying the whole body. A larger one is streamed as a chain of 4 MiB `PATCH`
requests, and a registry that spends a fixed time on each `PATCH` makes that
chain far slower than one request. The 100 MB default fits the registry it was
measured against, which refuses a larger single request. zot accepts a whole
layer in one request, so against zot the limit can be raised.

`run`, `backfill`, `rebuild blocks` and `rebuild --write` take
`--monolithic-max-mib M` to set it, in MiB:

- absent, the 100 MB default holds;
- `0` streams every layer, for a registry that handles a single large `PUT`
  badly;
- a layer under the limit is held in memory in full while it goes up, inside
  the transport's 1 GiB upload budget. A value above that budget is clamped to
  it, not refused;
- with `--direct-s3-config`, blobs go to the registry's storage directly and
  the option has no effect.

```sh
stelae-publisher --config dolos.toml rebuild blocks \
  --source oci://registry.example/cardano/preview \
  --journal blocks.journal \
  --upload oci://registry.example/cardano/preview \
  --monolithic-max-mib 1024
```

### Reading and reproducing snapshots

`digest`, `verify` and `inspect` replace Dolos's former `snapshot` commands
over the same profile entry points. `--repo` takes a network root,
`oci://HOST/PATH` or `file://DIR`; registry credentials are resolved only for
an `oci://` source.

Compute what the committed stores would publish, writing nothing. The stores
must stand on the first block of an epoch, sequence N. The plan is reported on
stderr and the canonical inscription goes to stdout (or `--output FILE`), so it
hashes to the identity reported. Two inputs are part of the document and so of
its digest: the snapshot at N−1 it follows, and the blocks snapshot at N it
names. `--repo` reads both from a root, and `--previous DIGEST` and
`--blocks DIGEST` give either one instead, leaving that part of the root
unread. The root's N−1 is read without its tip, which a repository may no
longer keep, and a root with none there starts a lineage. A root's blocks
snapshot is held to what a publish names: one absent at N, or of another
sequence or network, is refused. With neither `--repo` nor the flags, the
reproduction starts a lineage and names no blocks snapshot. Stderr says which
it used:

```sh
stelae-publisher --config dolos.toml digest \
  --repo oci://registry.example/cardano/preview > snapshot.json
```

Check a published snapshot's digests: its chain back to the first snapshot of
its lineage, then every layer of its layer set streamed against both of its
digests. `--reproduce` also rebuilds every layer the snapshot lists from the
committed stores and requires a byte-identical inscription, taking
`--producers`. The chain walk has already held the published `previous` to the
snapshot the repository holds at N−1, and `--previous DIGEST` holds it to that
digest as well:

```sh
stelae-publisher --config dolos.toml verify \
  --repo oci://registry.example/cardano/preview --point epoch-1200 --reproduce
```

Print a snapshot's table of contents without pulling a layer: its own `diff`
and `tip`, its chain, and every layer of the set they make:

```sh
stelae-publisher --config dolos.toml inspect \
  --repo oci://registry.example/cardano/preview
```

A batch of one spools its index runs, reproductions included, under `--scratch-dir`
(default `<storage.path>/scratch`). `digest` and `verify --reproduce` take it as
`publish` does, to a repository or to `--output-dir`. A mainnet first publish
spools tens of gigabytes, and the directory must be writable by no one but the
user running the command. A larger batch's runs stay with the batch, under
`<storage.path>/.stelae-batch/staging`, whatever `--scratch-dir` says.

All three take `--format text|json` (default `text`): data on stdout,
progress and logs on stderr. `inspect --format json` is the canonical
inscription verbatim; `digest` writes the inscription in either format; `verify --format json` is one report object.
They exit 0 on success, 1 on an error, 2 on a usage error and 3 when `verify`
refused the snapshot: a document or layer whose bytes, digests, names or
encoding disagree, including a blob a registry serves under another digest
or past its descriptor's size and a blob that does not decompress,
an inscription of another profile or schema, a broken chain, or, under
`--reproduce`, a snapshot of another network or sequence than the stores or a
reproduction that differs. A check that could not run, because the registry,
the disk, the credentials, the configuration or the local stores failed,
exits 1, including a record the local stores hold out of order or malformed
under `--reproduce`, as does a `--point` the source does not hold, a tip the
registry no longer keeps, and any refusal by `inspect` or `digest`.

### Checking freshness

A publisher run that exits 0 says nothing about whether a network's snapshots
are current: when Mithril stalls, runs succeed with nothing to publish, and
when the schedule stops, none run, while `latest` ages either way. `freshness`
asks the question from the clock and the registry alone:

```sh
stelae-publisher --config dolos.toml freshness \
  --repo oci://registry.example/cardano/preview --deadline-hours 30
```

The network is the configuration's. Its epochs are placed in time by the
well-known genesis values pallas carries for the networks of Dolos's catalog
(`mainnet`, `preprod`, `preview`); any other network is refused, naming it,
and so is a custom network with the magic of a known one. The expected
sequence `E` is the newest epoch whose start plus `--deadline-hours` hours is
at or before now: sequence `E` is the cut at the boundary that opens epoch
`E`. A deadline that reaches back before the network's first epoch is
refused, naming it. The command reads the sequence and network `latest` names
in the root's `dolos` repository, its own documents only, as the identity
`inspect` reads with, refuses a `latest` of another network, naming both
magics, and prints one line on stdout: the network, `latest`'s sequence, `E` and the start
of epoch `E`. It opens no store and writes nothing. The `dolos` repository is
the one checked because a Dolos snapshot is published only after the blocks
snapshot it names, so its `latest` is the later of the two to move.

It exits 0 when `latest` is at or past `E`, and 4 when it is behind, saying by
how many sequences, or when the repository holds no `latest`. Any other
failure, a registry that cannot be reached and a `latest` of another network
included, exits 1, and a usage error 2. A failed check is therefore either
stale or unable to read; its log says which.

Choosing `H`: Mithril certifies a boundary's immutable files 18 to 23 hours
after it, and a scheduled publisher picks them up at its first run after
that. `H` is the certification delay, plus the publisher's interval, plus a
run's own duration and a margin, so that one slow run does not fail the check
and a stall shows within a deadline. On preview, whose epochs last a day, an
`H` past 24 hours puts the deadline of one epoch inside the next.

### Rebuilding a lineage from before the diff chain

`rebuild` plans how a Dolos lineage published before the diff chain becomes
one, from the documents its repository holds and with no chain replay.
`--source` names that repository, the network root the old lineage was
published at. The command reads each `epoch-N` tag's manifest and
inscription, never a layer, and writes nothing to a registry.

Sequence by sequence in ascending order, the Dolos profile's rule lists the
old snapshot's layers in the new `diff` and `tip`, as a native publish at that
boundary lists them. Closed epochs' `blocks` layers, which belong to the
blocks profile now, and retained state dumps are dropped, and so is
`parameters.stateEpochs`. Then the self-check: the diffs up to the sequence
plus its tip must be its old layer set less those, by `diffId`. A difference
refuses the rebuild and names the layers. So does a lineage that skips a
sequence, starts after one the repository no longer holds, or whose history
does not end at the inscription read before it. The epoch geometry comes from
the newest inscription's `indexes` scopes, and the stability window from the
configured genesis, whose network the lineage must be on.

```sh
stelae-publisher --config dolos.toml rebuild \
  --source oci://registry.example/cardano/preview \
  --blocks-journal blocks.journal --kept-tips-file kept-tips \
  --plan rebuild.plan --print-digests > digests
```

A new inscription names the blocks snapshot at its sequence, so it needs the
blocks lineage's journal, `--blocks-journal`: a line per sequence, its number
and the blocks inscription digest, apart by a space. Without it the command
runs the assignment and the self-check only. With it, `--plan FILE` writes the
plan. Its first line is a header (`rebuildPlan`, the format, then `source`,
`first` and `last`). Each line after it is one sequence as JSON: its
`inscription` and `inscriptionDigest`, its `diffManifest` and `tipManifest`,
`keptTip`, and the `blobs` the published manifests name, by their digest and
size in the old repository. The plan is written beside its name and moved
there once whole, so a refused rebuild leaves none.

`--kept-tips-file` names the epochs whose tips the registry keeps, one per
line as the retained-epochs file was. The newest tip is always kept, and a
tip's blobs are listed only where it is kept. `--print-digests` prints each
sequence and its new inscription digest, `N sha256:…`, on stdout, and moves
the summary to stderr. `rebuild` exits 0 on success, 1 on an error or a
refused lineage, 2 on a usage error and 3 when the self-check refused a
sequence.

#### The blocks lineage

`rebuild blocks` builds that journal, and the blocks lineage it names, from
the same old repository. The newest `epoch-N` lists a `blocks` layer for
every closed epoch, 0 to N−1, under a header naming the Dolos profile. Each is
streamed in turn and written again with the blocks profile's header and
every record byte kept, so the `diffId` and the blob change and the blocks do
not. Epoch E is sealed at sequence E+1, linked to the one before, as
`stelae-blocks` seals a blocks snapshot exported from immutable files.

```sh
stelae-publisher --config dolos.toml rebuild blocks \
  --source oci://registry.example/cardano/preview \
  --journal blocks.journal \
  --upload oci://registry.example/cardano/preview
```

On the way past, the bodies of each immutable file the epoch covers are
hashed: a block belongs to the file its slot divided by the network's file
width (ten times the security parameter) gives, so a Byron boundary block
falls in its epoch's first file, and a file with no block hashes as an empty
one. Each digest must be the `.chunk` digest of Mithril's published list,
fetched from the aggregator's `artifact/cardano-database/digests` route
(`--mithril-aggregator`, defaulting to the configured network's), or read
from a saved copy with `--mithril-digests FILE`. A file whose digest differs,
or that the list does not certify yet, refuses its epoch: the error names
each such file, nothing of that epoch is written, and the command exits 3.

The journal gets a line per sequence, `N sha256:…`, once its snapshot is
sealed. A run resumes after the journal's last line and appends to it, so a
refused or interrupted run is continued by running it again. Without
`--upload` nothing is written to a registry: the layers are hashed and
discarded, and the journal is what `rebuild --blocks-journal` reads for a dry
run. With `--upload ROOT`, the blobs, manifests and tags go to ROOT's
`blocks` repository one layer at a time. A blob the repository already holds
under the same digest, as after an interruption, is not sent again. Before
anything is written, the repository's newest snapshot must be where the
journal ends, or the one after it, whose journal line was lost and which is
sealed again with the same bytes. The summary gives the epochs re-encoded and
resumed, the files compared, and the layers' sizes. `--monolithic-max-mib M`
sets the largest layer uploaded as one request; see
[Single-request uploads](#single-request-uploads).

#### Writing the rebuilt lineage

`rebuild --write --plan FILE` executes a plan. It takes the old repository
from the plan's header and writes the new lineage to the `dolos` repository
under the network root, which is that repository unless `--root` names
another. It refuses a plan whose lines disagree (a gap, a broken `previous`
link, a manifest that does not list its inscription's array, blobs other than
the manifests name, an unkept newest tip), a plan whose positions are on
another network than the configuration's, and a target that is the plan's own
source.

```sh
stelae-publisher --config dolos.toml rebuild --write --plan rebuild.plan \
  --direct-s3-config /run/secrets/direct-s3.json
```

Every tag the plan writes is read first. A tag that already holds the planned
manifest is skipped, so a run that stopped part way is resumed by running it
again; a tag that holds another manifest refuses the run before anything is
written. Then:

1. every blob the published manifests name is put in the target: copied inside
   the registry's storage with `--direct-s3-config` when both repositories are
   on the same host (up to one copy's limit, 5 GiB), otherwise pulled from the
   old repository, held to its digest, and pushed;
2. per sequence, in ascending order: the inscription, the tip manifest under
   `epoch-N.tip` where the tip is kept, the diff manifest under `epoch-N`;
3. the closing pass resolves the last sequence from the target and holds its
   chain, its layer set and each layer's blob to the plan;
4. only then does `latest` move to the last sequence.

Nothing is written to the old repository, and nothing is deleted. A closing
pass that finds the target other than the plan exits 3; one the registry
could not answer exits 1, and a rerun resumes it. Either way `latest` is left
unwritten. Other failures exit 1 too.
`--scratch-dir` is where a pulled blob is staged on its way up.
`--monolithic-max-mib M` sets the largest pushed blob that goes up as one
request; see [Single-request uploads](#single-request-uploads).

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
| `<storage.path>/mithril/blocks-refetch/immutable` | one epoch's immutable files downloaded again for its blocks snapshot, beside the default window; used only once a `downloaded` marker beside them records that the download finished, and removed once the snapshot is named or when its download fails or stops (see [Blocks snapshots](#blocks-snapshots)) |
| `<storage.path>/.stelae-batch/` | a prepared batch's journal, bundles and staged runs; see [Prepared batches](#prepared-batches) |
| `<storage.path>/.stelae-batch-began.json` | the sequence a batch over an empty repository began at, kept outside the batch; see [What a restart does](#what-a-restart-does) |

Before any cleanup, initialization requires an absolute storage path below a
filesystem root. A marker is accepted only when its storage path, network
magic and source match the current configuration. A restore marker without a
committed cursor fails closed. A genesis marker can describe an empty store
only while the current run explicitly opts into genesis.

The backfill state machine preserves these rules from the accepted Dolos host:

- publish a committed epoch boundary before replaying past it, or on a larger
  batch, hold it as journaled prepared work until it is published;
- prune history only after that publication succeeds, and on a larger batch only
  after its last sequence commits;
- retain two immutable files of cleanup/download margin using the configured
  network's genesis chunk geometry, and on a batch of one, every file of the
  epoch the stores stand in;
- retry transient repository work in place using the publication journal;
- publish the blocks snapshot of each sequence into the blocks repository,
  from the immutable files of the epoch its boundary closes, before the
  boundary is captured or published and before those files are removed (see
  [Blocks snapshots](#blocks-snapshots));
- keep the plan, schema, media-type,
  compression and journal identities of the pinned profile.

### Crossing a store contract

Each store records the Dolos store contract it was created under, and the
configuration's `compatibility` names one too. An image whose Dolos records a
newer contract refuses the older stores and configuration, so it opens the
claim only after both stores are relabeled to that contract (Dolos's
[`store relabel`](https://github.com/txpipe/dolos/blob/c31f215cfcc48e5a1f57176914eefd932af273ee/xtask/README.md#store-relabel))
and `compatibility` is updated. A prepared bundle records the contract it was
captured under, and the new image refuses one captured under the old, so stop
the publisher at a boundary with no prepared batch under `.stelae-batch/`.
The procedure is: wait for such a boundary, stop the publisher, confirm that
`.stelae-batch/` still holds no prepared batch, relabel the state and archive
stores, set `compatibility` and the new image together, and start it again. If
a batch was prepared before the stop, start the old image again and drain it
first, as under [Upgrade and rollback](#upgrade-and-rollback). The protocol,
profile and media-type versions do not change with the store contract, so the
published lineage continues unchanged.

### Stores whose state passed the archive

Dolos commits a replayed block's state before its archive entry, and an import
keeps no WAL, so a kill or a full volume between the two leaves the state cursor
past blocks the archive never got. Replaying on closes the difference with those
blocks missing: the snapshot cut from there restores without complaint, and every
later snapshot inherits the hole. So `run` and `backfill` compare the cursor with the
archive's tip before replaying or publishing anything, on a batch of one and a
larger batch alike, and refuse while the tip is behind. An archive ahead of the
cursor passes. Replay writes the same blocks again, which heals it only up to the
next boundary: past that a publish refuses, saying the archive runs on past the
cursor, until the stores are replaced.

The refusal repeats until the stores are replaced, and nothing repairs imported
stores: clear the storage path and any store configured outside it, then restore
the stores from the repository's latest snapshot. A repository with no snapshot has
nothing to restore, so clear them the same way and run with `--allow-genesis`.
Stores a node synced differ: they keep a WAL, so a Dolos restart catches the archive
up, and their documented route, `publish` and `digest`, doesn't meet this check.

A stop between an epoch's opening and its first block leaves a cursor with no block
hash, past the archive. The check has nothing to compare and passes it, and nothing
anchors that cursor again, so every run then refuses as it reads the cursor, naming
the same recovery.

This can't vouch for stores that crossed a gap on an earlier build. Once later
imports bring the tip level with the cursor, the hole sits inside the archive and
the two compare equal. It also compares slots, so a Byron main block lost beside
its boundary block at one slot passes. The chart reruns a failed publisher by itself
(`restartPolicy: OnFailure`, `backoffLimit`) in the same pod, so `/data` survives
that rerun on an emptyDir as well as on a claim, and nobody read the stop first:
snapshots from a publisher rerun in place before this check are unchecked by it.

### A volume that fills

A replay or a store's close that fills the volume stops the run: make room and
rerun. If the state passed the archive on the way, the rerun refuses, as above. A
capture that ran out on the same volume is the exception: it closes the batch.

Three cases read differently:

- **A write or a background worker failing.** A failed write returns its own error
  once, and a full volume's reads as above. After that, or after a worker fails, the
  store answers only `FjallError: Poisoned`, and the log holds the error, such as
  `Worker #N crashed` or `persist failed`. A failing disk reads the same, so that stop
  asks you to check both and doesn't call the volume full. Each store's close also
  waits up to a minute for flushes a dead worker will never finish, so on a volume
  still full a rerun can take minutes to stop.
- **A write waiting on a worker.** The pinned fjall ends the waits that hung on
  earlier pins: a write held by backpressure or at the L0 write halt returns
  `FjallError: Poisoned` once the worker it waits on has died, and a store's close
  returns after it. The run then stops as in the first case. Rerun with more room
  than one compaction writes, because several can run at once across the stores: a
  rerun with too little stops again.
- **A download filling it.** The Mithril client reports only the locations it
  tried, so the failed fetch adds the free bytes of the download directory: read a
  low figure as a full volume. A stopped download can also leave partial or absent
  files below the highest one, which a rerun doesn't fetch again, so make room, empty
  the immutable directory and rerun. A batched run tries to close the batch and
  publish what it prepared first, so this failure can arrive beside a room error. The
  exit names both, and emptying the immutable directory still applies. That can't
  help once a rerun has imported across
  an absent file next to an epoch's start, which the chart's own restart can do: every
  run then refuses before any fetch, saying the archive has a gap there, unless the
  repository already holds that boundary. A held boundary isn't checked, so a run over
  one gets no such refusal, and a clean run doesn't show that no gap was crossed. Clear
  the storage path and any store configured outside it, then restore from the
  repository's latest snapshot.

## Blocks snapshots

Each boundary has two snapshots, written in this order: the blocks snapshot,
in the root's `blocks` repository, then the Dolos snapshot, in its `dolos` repository,
whose `position` names the blocks snapshot by its inscription digest. Both are
tagged `epoch-N` at the boundary that opens epoch N, and both lineages start at
`epoch-1`.

At the boundary that opens epoch N, the host exports epoch N−1 from its
immutable files with the blocks profile, one `blocks` layer for the whole
epoch, and seals it at sequence N after the blocks repository's `latest`,
which has to be `epoch-(N−1)`:

- A batch of one does this before it publishes the Dolos snapshot, and removes the
  immutable files the boundary consumed only once the Dolos snapshot is published. It
  keeps every file of the epoch the stores stand in, which the next
  boundary's blocks snapshot reads, so a run that ends part way through an
  epoch leaves its files to the next run.
- A larger batch does it when it captures the boundary, before the consumed
  files go. The bundle, which the journal names, carries the reference, so the
  flush publishes the Dolos snapshot later without reading the blocks repository.

A blocks snapshot already at N, which a run killed between the two snapshots
leaves, is reproduced from the same files, following the repository's
`epoch-(N−1)`, and compared by inscription digest, as a Dolos snapshot already at its
sequence is. A seal writes `epoch-N` before it moves `latest`, so one held
under a `latest` still at N−1 is a seal that stopped between the two: it is
reproduced following that `latest` and, once it compares, sealed again, which
writes the same tag and moves `latest`. A `latest` past N never moves. These
refuse, with nothing of the Dolos snapshot at N written and no retry:

- a blocks snapshot at N that the files do not reproduce;
- one that is another sequence's or another network's;
- a blocks repository whose latest is not N−1. The blocks lineage skips no
  sequence, and only `epoch-1` starts one.

A restart can need a blocks snapshot after its epoch's files were removed, as
on a volume that came back empty. The host then downloads that epoch's files
again from Mithril, with the window's validation setting, plus the file after
them, which shows the epoch's last file is closed. They go to
`blocks-refetch/` under the download directory, apart from the replay's
window, and are removed once the blocks snapshot is named. Only a download
that finished, with its files all there, is used: a `downloaded` marker
beside them records it once the files are synced to disk, and a retry or a
rerun seals from it without downloading again. Files without that marker,
as a crash part way through a download leaves them, are downloaded again.
One that Mithril rejected, or that was stopped, is
removed, and the run ends on that failure, which the download has already
retried. A boundary whose Dolos snapshot the `dolos` repository already holds reads no
blocks snapshot.

The blocks upload goes through the same transport settings as the Dolos snapshot's:
registry credentials, `--concurrency` and `--direct-s3-config`.

## Prepared batches

A batch prepares several boundaries before it publishes any, so one index
traversal serves them all. Replay may stand past the repository's latest snapshot
only while every sequence in between is a complete bundle the journal names.
`run` and `backfill` take the controls below. `publish` stays a batch of one. The
chart passes them only when its `batch` values are set, so a deployment doesn't batch
until its values say to.

| Control | Meaning |
| --- | --- |
| `--batch-epochs N` | Boundaries in a batch. 1, the default, captures nothing and publishes before it advances. |
| `--batch-scratch-mib M` | What a batch may hold on disk: bundles and staged runs. Required above one epoch. |
| `--batch-reserve-mib M` | Kept free per prepared boundary: what staging it takes, and what an epoch's replay and download add to this volume. Required above one epoch. |

`--batch-epochs` stays 1 unless a command says otherwise, because no window has yet
shown the reduction batching is for: see below. The two sizes have no defaults, since
they follow from the network.

### Choosing a window

Over 24 real preprod epochs, from a store holding 276 of them, publishing to a local
registry with the same bytes on every arm ([report](reports/batched-export-2026-11/README.md)):

| `--batch-epochs` | Whole run | Longest wait for the next sequence | Batch's disk high-water |
| --- | --- | --- | --- |
| last release before batches | 1345 s | 65 s | none |
| 1 | 2004 s | 93 s | none |
| 4 | 1838 s | 167 s | 1.8 GiB |
| 16 | 2036 s | 448 s | 6.7 GiB |
| 24 | 2151 s | 618 s | 10.3 GiB |

So 4 is the window to start from, on the strength of one unpaired run, and on a network
like preprod it isn't free: it is slower than the release before batches, and a batch of
one is slower still. A batch
saves index traversals, about 20 s a boundary here, and pays about 9 s a boundary to
capture. It should gain more where a traversal costs more: on mainnet one took about
410 s in an index-only measurement. No complete run has measured that, so treat a
window on mainnet as an experiment.

### How much room a batch takes

A bundle is one boundary's state, its own windows and its unsettled logs, so its size
follows the state and not the indexes. On preprod at this height:

| What | Each boundary | Where |
| --- | --- | --- |
| Bundle | 0.42 GiB | `.stelae-batch/prepared` |
| Staged runs | 12 MiB a window | `.stelae-batch/staging` |
| Growth of the stores from replay, chain files aside | 95 to 112 MiB an epoch | the stores |
| Download window, until consumed | 51 MiB an epoch | `--download-dir` |

The stores keep that growth for good, so a run of `N` epochs needs it on top of everything
else here: 9.3 to 10.9 GiB for 100 preprod epochs. The runs measured here don't show it,
because they had all their chain files on the volume from the start and replay deleted them
as it went. A real run downloads a window at a time, and nothing offsets it. Net of those
files, the stores at their highest stood 0.86 GiB up over 4 epochs, 1.06 over 16 and 0.96 to
1.08 over 24. About 0.7 GiB of the first is above what replay keeps, so allow a transient of
up to about 1 GiB, whatever the window.

Each publication also checks first that its volume can stage what it stages at once, and
refuses a shortfall. That is every state layer of the latest snapshot and its largest other
layers, as many as `--concurrency`. At this height it took 0.65 GiB at `--concurrency 4`,
which these runs used, and 0.79 at the default of 8, measured before the diff chain on a
lineage holding no retained dump, which the state layers alone now are.

Layers staged for upload are unlinked files, so they show only as the volume's free
space falling. On these runs it fell 4 to 5 GiB past the batch's own high-water, with
everything else on that volume counted in, so read that as an upper bound.

Before replay leaves a boundary, the batch must have room for what it holds, for the
reserve of every boundary it would then hold, and for one more boundary as large as
its largest. Both limits are held to that sum. The free-space line adds the costs above,
which the batch doesn't hold itself to. So a window of `W` over a run of `N` epochs needs about

```text
--batch-scratch-mib   >=  W x (bundle + reserve)
free space on volume  >=  W x (bundle + reserve) + one publication's check + transient + N x growth
```

For a window of 4 here, on a lineage with no dump at `--concurrency 4`, that's about 2.1 GiB
of budget and 3.8 GiB free with the download elsewhere, and 2.3 and 4.0 on the default layout,
where it shares the volume. Those free figures leave out `N x growth`. The default concurrency
adds 0.13 GiB. That is the floor a run should
get through, not what it was seen to take: the one window 4 run that held its window with the
registry off this volume saw it fall 6.2 GiB over 16 epochs, other work on it included. A batch
of one holds none of the batch's terms, and needs its index spool, the check, the transient and
`N x growth`. No run has tried that floor. One that runs short stops on the room it lacks, which
is recoverable: see [A volume that fills](#a-volume-that-fills).

`--batch-reserve-mib` should cover a boundary's staged runs and replay growth, about 105 MiB
here and up to about 125 MiB at the fastest growth above, plus the download where it shares the
volume until consumed, as it does by default: about 158 to 175 MiB. A budget that holds one
boundary doesn't fail when it's too small for the window: at `--batch-reserve-mib 512`, a
`--batch-scratch-mib` of 2048 closed each batch of 4 at two boundaries, and 1024 with a
reserve of 256 closed each at one, and 4096 with a reserve of 512 held all 4. Each
smaller batch costs its own traversals, so a budget that keeps closing early is worse
than a smaller `--batch-epochs`. Why a batch closed is in `journal.json` until it drains.

To size a network you haven't measured, read one boundary's bundle: run with a window
above 1, then check `.stelae-batch/prepared` once the first capture is journaled. A
batch is held to the largest boundary so far, so the state's growth moves that figure
up as replay goes on.

### What a batch delays

A window delays every sequence in it. The first waits for `W` replays and captures, then
staging, then its own publication. The rest follow one at a time. Nothing else changes:
`latest` still moves through every sequence in order.

**Sequences appear later.** A sequence reaches the repository only when its batch
closes, stages and gets to it, so `latest` trails the stores by up to a batch while
replay runs. Every sequence still appears, each with the same content a batch of one
gives it, and `latest` moves through them in order. A consumer restoring meanwhile
gets the last one published. At the tip nothing waits for a full batch: a run closes
on what the source offered and publishes it, so a window only delays a backfill.

### State transitions

1. **Admit.** Before replay leaves a boundary, the batch must have room for what
   it holds, and for one more boundary estimated at the largest so far. Both the
   budget and the volume's free space are held to that. Nothing but the reserve
   stands for what replay and its download add, so size it to include them.
2. **Replay** to the next boundary. Nothing prunes while a batch holds anything,
   and nothing is published. For a replay that fills the volume anyway, see
   [A volume that fills](#a-volume-that-fills).
3. **Capture** while the session that imported the boundary is open, because it
   holds the block's index delta.
4. **Close the session**, so the stores are durable.
5. **Journal** the bundle, and only then remove consumed immutable files. Pending
   work reads none of them, and a replay after a lost bundle downloads them again,
   once the window the lost replay left is gone: see the recovery below.
6. **Close the batch** with the reason it stopped growing, or go back to 1.
7. **Stage** the closed windows once, from the stores as they stand. Replay doesn't
   move again until the last commit, so that view stays frozen.
8. **Commit each sequence in order.** Read what the repository holds, and hold it
   to the journal. Publish through the existing transport, chained to the real
   predecessor. Journal the snapshot, then release its bundle and any payload no later
   bundle shares.
9. **Clean up** after the last commit: the staged runs, then the bundles, then the
   journal. A crash at any point only repeats this. Pruning resumes here, and replay
   goes back to 1.

| A batch closes on | At |
| --- | --- |
| `epoch-limit` | the last boundary `--batch-epochs` allows |
| `until-epoch` | the boundary `--until-epoch` names |
| `scratch-budget` | the last boundary before the next would cross the budget |
| `disk-pressure` | the last boundary before the next would not fit the volume, or a capture that filled it, whose payloads go so the batch is charged only for what its journal names |
| `end-of-source` | the last captured boundary, when the source runs out or stops answering. The epoch in progress has none and stays unpublished. |

A closed batch stays closed across restarts, whatever the controls say then. A
periodic run reaches `end-of-source` and publishes what it prepared, however few
boundaries that is: nothing waits at the tip for a full batch. A source that stays
down past its retries, or a fetch that brings nothing new, closes the batch the same
way, and the run publishes what it prepared before it exits on that failure. If
anything on that way fails too, the exit names both: a failure before the journal
records the close leaves the batch open, and one after leaves it closed. So an outage
during a backfill costs one smaller batch, and one more index traversal for the next.

### Progress

Each journal write logs and reports the frontiers: `published`, the snapshot the next
publication follows, which moves with each commit; `prepared`, the last journaled
sequence;
`staged`, where the runs were staged from; and `replay`, the slot the stores reached.

### What a restart does

| The stores stand | The run |
| --- | --- |
| in the last prepared epoch, at or past its boundary | continues replay; nothing is captured again |
| on the next boundary, uncaptured | captures it with an index traversal, because the delta died with the process: at most one per restart |
| past that, or further on | refuses: nothing holds the sequences in between |
| behind the journal's replay frontier | refuses: the stores were rebuilt or rolled back |

These refuse too, and none is repaired automatically: a repository whose latest
snapshot is not the one the batch follows, a journal or bundle that is missing or
damaged, and another repository, network, store contract or Dolos revision than the
batch was prepared from. A batch of one and a repository
`publish` refuse to run past a pending batch, and the refusal names the flags that
publish it. A bundle the machine can't read right now, through permissions or a
failing disk, may be intact: that refusal says so, and a rerun goes on once it is
readable.

A run that stopped during publication reads the repository first, so it goes on
from the sequence the journal names:

| The repository holds | The run |
| --- | --- |
| the snapshot the journal says comes before this sequence, and nothing under this sequence | publishes it |
| this sequence's tip, under its `.tip` tag only: a seal that stopped after its first tag | seals again, which writes the same tip |
| this sequence under its immutable tag, and `latest` behind it: a seal that stopped before its last tag | reproduces it, compares identities and seals again |
| this sequence as `latest`: an upload whose answer was lost | reproduces it, compares identities, journals it and doesn't upload it again |
| this sequence as `latest` with another identity | refuses, and overwrites nothing: another writer published it, or these stores aren't what the batch was prepared from. The recovery below applies |
| this sequence with another identity under its immutable tag only | refuses, and overwrites nothing: another writer stopped before moving `latest`, or this publisher did and these stores aren't what the batch was prepared from. If you know that tag is another writer's, stop that writer, delete the tag and rerun. Otherwise leave the tag and treat the stores as wrong, because deleting it publishes from them unchecked: the recovery below applies |
| anything else: past this sequence, behind the journal, or a predecessor under another identity | refuses: the repository moved |

A batch of one has no such check on the immutable tag and overwrites it, as it
always has.

The read a publication chains from is held to the journal too, so `latest` moving
after these checks refuses before anything is uploaded. The exception is `latest`
moving to this very sequence, which can be this batch's own seal applied after its
answer was lost: the next try compares it as the table says.

The registry has no compare-and-set, so nothing guards the upload that follows that
read. Another writer's seal landing during the upload is overwritten by this batch's
at its tags and never found, on a batch's last sequence as on any other. While a
later sequence is left to publish, a `latest` landing after this batch's own refuses
at that sequence's read, and a tag that writer left at that sequence refuses as the
table says. So keep to one publisher per repository.

Reproducing a sequence costs what assembling its layers costs, without the upload.
Staged runs are disposable. Missing, damaged or staged for something else, they are
staged again from the stores, once in a run, under the checks a first staging makes
(the store contract, the network and how far back the archive reaches). A bundle
isn't: damage to it, or an archive pruned past the first block it needs, stops at that sequence
with the recovery below, and the earlier sequences stay published.

A publication that runs out of room stops without moving the journal: make room and
rerun. Staging is held to what `--batch-scratch-mib` has left, or the reserve for what
the batch holds where a capture overshot the budget. Runs a stopped staging left count
against it, so a restarted staging that runs out starts over from nothing first, which
repeats its traversals. Past the budget from nothing it stops and drops its runs, so
raise the budget and rerun.

Stopping a publisher mid-batch and starting a batch of one, a smaller `--batch-epochs`
or a newer image are different things. A smaller limit continues the batch. A batch of
one refuses until it drains. A new image on another Dolos revision refuses as above,
so drain a pending batch with the image that prepared it before upgrading. See
[Upgrade and rollback](#upgrade-and-rollback) for images from before batches.

An interrupted single publish leaves `<storage.path>/.snapshot-publish.json`. A batch
reads it as a batch of one would: whatever it names under this repository and these
parameters, of the layers this sequence adds, is resumed if the repository still holds
the blob, and rebuilt if not. A record that doesn't parse stops the publication: delete the file and rerun,
which costs only re-uploading what it named.

The recovery starts the stores over from what the repository holds, which after a
partial publication includes the sequences this batch already committed. A restore
changes nothing over stores that are still there, and deleting the batch alone
leaves stores past the repository, which refuse, so clear them first:

- **It holds a snapshot.** Clear the storage path and any store configured outside it,
  which deletes the batch with them. Then restore the stores from its latest snapshot
  with `initialize`, or `run`, which starts with it, to replay those sequences.
- **It is empty.** There is nothing to restore: clear them the same way and run with
  `--allow-genesis`.

A store configured outside the storage path survives clearing that path and reads as
existing stores afterwards. `--allow-genesis` then keeps it, and a first snapshot starts
past what the batch held with nothing to refuse it.

A download window left from the lost replay can't continue a restored one. With
`--download-dir` outside the storage path it survives the clearing, so the run
refuses and names the immutable directory: empty it and rerun, and what the replay
needs downloads again.

A batch over an empty repository records where it began in
`<storage.path>/.stelae-batch-began.json`, outside the batch. A history can't start
past that sequence while the repository is empty, on a batch of one or a repository
`publish` as well, because a snapshot there would leave out sequences only the batch
held. The record means nothing once the repository holds a snapshot. Delete it only
to start a history past those sequences on purpose.

Cancelling keeps all of it: a shutdown signal neither publishes nor manufactures
a boundary, and a rerun continues. During publication it is honoured between
sequences, and an in-flight staging or upload finishes first or dies with the
process, which a rerun resumes. Not even one boundary fitting is a recoverable stop
too: make room or raise the budget and rerun.

### Upgrade and rollback

**The diff chain.** A build from before the diff chain wrote its batch journal in
format 1, which held bundles this build can't publish. This one refuses it, saying to
reset: clear the storage path and any store configured outside it, then restore from
the network root's latest snapshot, or run with `--allow-genesis` where it holds none.
The snapshots that build published can't be continued either, so a network moves to
its new repositories with [First publication](#first-publication).

**Upgrade.** The last release before batches, on the `1db3e41f` pin, can be stopped by a
signal at any point, or killed during a publication, and this one takes over its stores:
they carry over, and a publication it left unfinished is published first. Over real
preprod, a run of that release killed part way through a publication and continued at
`--batch-epochs 6` published the same bytes as one that was never interrupted. A kill or
a full volume during replay is different: it can leave
[stores whose state passed the archive](#stores-whose-state-passed-the-archive), which
this one refuses until they're replaced. A release on an older pin still needs what
[Source and dependency identity](#source-and-dependency-identity) says.

Batching stays off until a command names a batch, because `--batch-epochs` stays 1 by
default and the chart passes none of these unless its `batch` values are set. The upgrade
itself still costs a deployment something, on a batch of one: on preprod a run takes about
49% longer than the last release before batches ([Choosing a window](#choosing-a-window);
mainnet isn't measured), it spools index runs under `--scratch-dir`, tens of gigabytes on a
mainnet first publish where the older pin spooled nothing, and it gains the refusal above.
Size the claim and any deadline for that before upgrading.

**Rollback.** The stores can stand ahead of `latest` while a batch is open, and an image
from before batches can't read the journal that accounts for that. So drain first:
run the image that prepared the batch with `--until-epoch` set to the last prepared
sequence, which exits there with the journal gone, then roll back. After that nothing is
different from any other rollback. The exception is rolling back from an image you suspect
of a bad export: don't drain it, because that seals more snapshots from it, and clear the
stores and restore instead, and the batch goes with them
([the chart's data volume](../k8s/dolos-publisher/README.md#data-volume)).

If a rollback reaches a pending batch anyway, the repository and the stores decide what
happens:

- **The stores are two or more sequences past the repository's latest snapshot.** The older
  image refuses, after its retries, saying the node is sequences ahead of the latest
  snapshot and a publish would leave a gap. It publishes nothing, and the batch is untouched.
  Go back to the image that prepared it and the batch finishes, to the same bytes. This
  ran over real preprod, at three sequences ahead.
- **The stores are one sequence past it, and past that boundary.** A batch stands here while
  it holds its first boundary and replays toward its second, and so does a batch that closed on
  `end-of-source` past that boundary and stopped during its last sequence. The older image
  fails on every retry, saying to restore that epoch's snapshot. It publishes nothing and the
  batch is untouched, so clear nothing: go back to the image that prepared it, and the batch
  goes on. Like the next case, this comes from reading both releases and hasn't been run.
- **The stores are one sequence past it, on that boundary.** A batch stopped during its last
  sequence can leave this, when it closed holding that boundary. The older image finds nothing
  to refuse: it publishes that sequence, with the right bytes, and replays on, leaving
  `.stelae-batch` on the volume. The image that prepared the batch then refuses those stores,
  so going back means the recovery above. This comes from reading both releases and hasn't
  been run.
- **The repository is empty.** On a boundary nothing refuses. The older image doesn't read
  `.stelae-batch-began.json`, so it starts the history where the stores stand, and
  every sequence the batch held is gone from that lineage for good: a batch stopped at
  its third boundary and continued by the last release published `epoch-3` onward,
  with no `epoch-1` or `epoch-2`. Past a boundary it fails on every retry instead, asking
  for a snapshot of that epoch to restore, and there isn't one. It publishes nothing and the
  batch is untouched, so go back to the image that prepared it, and the batch finishes: this
  comes from reading both releases. Don't roll back over a first batch. If the older image has
  published, stop the writer, delete the tags, clear the storage path and any store configured
  outside it (on a claim, delete the claim), then run with `--allow-genesis`. Without the
  clearing, the same stores publish a wider gap.

Losing the batch directory is a different thing, and so is losing `/data`: see the
recovery above. On an emptyDir, or a claim you deleted, the batch goes with the stores,
and the run restores `latest` and replays what wasn't published. That costs time and
loses nothing. A repository with no snapshot yet has no `latest` to restore, so a run
without `--allow-genesis` fails: that is the recovery's empty case, which clears any
store configured outside the storage path and runs with `--allow-genesis`
(`allowGenesisFallback: true` on the chart).

## First publication

A fresh lineage starts from an empty `dolos` repository and replays from
genesis, publishing the blocks lineage beside it as it goes. Every inscription
after the first names its predecessor, so the first snapshot is the only one
published without a predecessor.

1. Stop every other writer to the network root and wait for it to exit.
   Configure the node with full history (see
   [Configuration](#configuration)); its stores grow with the chain, to
   about 250 GiB on mainnet.
2. Delete every tag in the root's `dolos` repository, `latest` first, so no
   earlier pin's snapshot is restored or chained onto. A lineage published
   before the diff chain sits at the root itself, not under `dolos`: leave it
   alone. The `blocks` repository may keep what it holds: each blocks snapshot
   there is reproduced and kept, and one that differs refuses (see
   [Blocks snapshots](#blocks-snapshots)).
3. Clear the storage path and any store configured outside it, since an
   earlier attempt can leave stores and a batch there, then start the first
   run with `--allow-genesis`. Initialization finds no `latest`, opts into
   genesis and replays from block 0. A batch of one publishes each epoch
   boundary before it replays past it. A larger batch publishes its
   boundaries once it closes, so the repository trails replay by up to a
   batch ([What a batch delays](#what-a-batch-delays)).
4. Run every later invocation without `--allow-genesis`. Initialization then
   restores the published `latest` (or reuses the committed stores) and
   fails closed if it cannot, instead of silently replaying from genesis.

A published lineage starts at `epoch-1`. Stores at a later sequence publish
only onto a `dolos` repository that holds the sequence before theirs: over an
empty one, `backfill` and `run` refuse before anything is downloaded or
written for either repository, so stores restored from another lineage never
start one part way along it.

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

`publish --repo`, `backfill`, `run` and `rebuild --write` accept
`--direct-s3-config PATH`; `rebuild --write` uses it to copy blobs between
repositories inside the bucket.
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
  --direct-s3-config /run/secrets/direct-s3.json --concurrency 4
```

The existing concurrency bound covers both transports. Layers and the canonical
inscription are uploaded directly to `<prefix>/<repository>/blobs/sha256/<hex>`.
Files are SHA256/size checked before upload; large files use multipart uploads
with bounded buffers and abort on failure. Existing objects are checked for
size before reuse. Multipart ETags are not treated as SHA256 digests. S3 request
retries are handled by the storage client; publication failures still prevent
sealing and remain eligible for the host's existing retry/resume behavior.

The registry still handles predecessor reads, manifest validation, immutable
epoch tags and `latest`. A failed blob upload
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
