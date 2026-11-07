# Measuring batched publication

How to compare complete publisher runs at different `--batch-epochs`, from replay
through the last successful publication. Results are in
[reports/batched-export-2026-11/](reports/batched-export-2026-11/README.md).

Index-only timings aren't an answer here. A batch saves index traversals and pays
for captures, so what counts is the whole run: replay, capture, staging, assembly and
upload, with the same stores, chain, predecessor and retained list on every arm.

## What runs

- `stelae-publisher backfill`, unmodified, on each arm. The old exporter is the same
  command from the last release before batches, built from its tag.
- A loopback registry and Mithril listing, `stelae-cardano/examples/loopback.rs`. It
  writes blobs to disk, checks each against its digest and counts what it received.
  The listing offers nothing past the files already on disk, so a run fetches nothing.
  No network, no cloud and no production repository is involved.
- `tools/batched-export/measure.py`, which clones the inputs, starts both processes,
  samples disk use and load once a second, and appends one line to
  `observations.jsonl` for every run, failed ones included.

## Procedure

Build the arms, and keep each binary's hash with the results:

```sh
cargo +1.93 build --release --locked -p stelae-publisher
cargo +1.93 build --release --locked -p stelae-cardano --features testing --example loopback
git archive <last-release-tag> | tar -x -C <scratch>/old-src   # and build it there
```

Choose a root on the volume being measured, with room for a clone of the stores and
of the repository per run. Copy an immutable directory there, because a run deletes
what it consumes and so reads clones of these files. Then replay to a base epoch and
seed its stele, once:

```sh
measure.py prepare --root ROOT --corpus IMMUTABLE --state-epochs EPOCHS \
  --loopback LOOPBACK --publisher PUBLISHER --base-epoch 256
```

Run the arms, with each arm once in each position across rounds. `--first-file` is two
immutable files below the base's cursor and `--last-file` is a few past the boundary
`--until` names, on the network's own numbering:

```sh
measure.py run --root ROOT --corpus IMMUTABLE --state-epochs EPOCHS --loopback LOOPBACK \
  --workload NAME --base base/preprod --until 300 --first-file N --last-file M \
  --round 1 --position 1 \
  --arm '{"name":"w16","binary":"PUBLISHER",
          "flags":["--batch-epochs","16","--batch-scratch-mib","400000","--batch-reserve-mib","1024"]}'
```

`--from-genesis` starts a run with nothing, for a short parity run across eras. A
fault's cost is measured with `--kill-on REGEX --kill-nth N`, which sends SIGKILL when
the log matches and reruns, or reruns each `--rerun-arm` in turn to try an upgrade or a
rollback. `--fail-every N` has the registry refuse every Nth upload. `--discard-above
BYTES` has it keep only the size of larger blobs, which takes its writes off the volume
being measured, and those blobs can't be restored. `--keep` leaves a run's stores and
repository in place for `restore`.

Then read them:

```sh
measure.py compare --root ROOT      # every publication, against the workload's first run
measure.py summarize --root ROOT --pair old:w16 --pair w1:w16
measure.py restore --root ROOT --run RUN --sequence N --consumer OLD_DOLOS \
  --publisher PUBLISHER --loopback LOOPBACK --state-epochs EPOCHS
```

`restore` has a Dolos binary from before batches restore each named publication, then
has the publisher digest the restored stores and compares the document byte for byte.

## What the numbers mean

- **Wall time** is the process's, from spawn to exit. Inputs are cloned before the
  clock starts. Clones share blocks but not page cache, so each run reads its stores
  cold, whatever ran before. The OS cache isn't otherwise controlled.
- **Replay, capture, stage and publish** are spans between log lines, so they're as
  good as the log's timestamps. On a batch of one, staging falls inside publishing.
  The old exporter logs no traversal and walks its indexes beside its layers, so it
  has no stage span and its traversal count reads "not logged".
- **Traversal counts** are every `index staging traversal started` line in the
  process's log, which `Staged::report()` also counts.
- **High-water marks** are sampled once a second, so a shorter peak can be missed:
  `prepared` and `staged` under the batch directory, `spool` under the scratch
  directory, and growth of everything under the storage path.
- **Upload bytes** are what the registry received, not what the publisher reports.
- **Peak RSS** and **CPU** come from the kernel's accounting for the process. The
  registry isn't counted, and it runs on the same host and volume, so its hashing and
  writes are competing for the same disk. A real registry over a network takes longer,
  and takes the same time on every arm, because every arm uploads the same bytes. That
  dilutes any reduction measured here.
- **Host noise** is recorded and not removed: load at the start and through each run.
  A shared host's load makes a small difference unreadable, so compare pairs and read
  their spread before a median.
