# Batched publication: preprod measurements, November 2026

Complete publisher runs over real preprod, from replay through the last publication,
on one shared host and local disposable transports. The procedure is
[batched-export-measurement.md](../../batched-export-measurement.md). Every run the tool
got to record, faults included, is in [observations.jsonl](observations.jsonl), and each
of those ended with its last process exiting 0. Runs stopped by hand left nothing, and
are listed under [Runs stopped by hand](#runs-stopped-by-hand): the tool has since been
changed so that they would leave a row.

## Verdict

- **Parity holds.** Every publication of every arm is byte-identical: 69 tags across all
  workloads, 0 disagreeing, from the last release before batches through windows 1, 4,
  6, 12, 16 and 24, each with the same bytes uploaded.
- **The working target is not met here.** Window 16 is 51.9% slower than the last release
  (median of 3 pairs) and 1.6% slower than a batch of one (median of 3 pairs), against a
  target of 25% faster. Window 4 is the best measured, 8.3% faster than a batch of one
  and 36.7% slower than the last release, from one run.
- **A batch of one, which every deployment gets by default, is 49% slower than the last
  release** on this workload, with about the same CPU.
- **Mainnet is not measured.** A traversal there costs far more against everything else,
  so it could come out differently, and nothing here shows it either way.

## Setup

| | |
| --- | --- |
| Candidate | `main` at `5618dff`, Dolos `bf66dcc`, Rust 1.93, release profile |
| Last release before batches ("old") | `v0.5.2`, Dolos `1db3e41`, built from its tag |
| Consumer for restores | A Dolos binary at `4ac2f90a`, from before batches |
| Chain | Preprod's certified immutable files, through file 6062 |
| Inputs | Copy-on-write clones of one store and repository per run, the retained list the preprod publisher uses, `--concurrency 4` |
| Registry | The loopback registry, on the measured volume except where noted |
| Host | Apple M4, 10 cores, 16 GiB, external PCIe SSD on APFS, shared with other work: median load 4.6 to 5.2 across the paired runs |

Binary hashes and the retained list are in [provenance.json](provenance.json), the
schedules in [schedules.sh](schedules.sh), and all runs in [tables.md](tables.md).

**What the rows leave out.** The runs were recorded with the names of the host's busiest
processes. Those names are out of the committed rows and the tool no longer collects them,
so `busiest_at_start` holds CPU figures only. Load, free memory and volume space at the
start are in `host_at_start`. The logs, the raw samples and the runs' manifests stayed on
the host. `provenance.json` is the tool's output less its line naming the host, with the
commits added by hand. The first 26 rows have no `volume_drop_bytes`, which the tool gained
after them.

**Cache.** The OS cache was neither controlled nor measured: `inblock` and `oublock` read
0 on every leg. Clones are separate files, so a run shouldn't find another's pages cached,
but nothing here shows it. Free memory at the start ran 0.09 to 4.72 GiB across all runs,
and 2.69 to 4.72 across the paired ones. Within the pairs an arm's later positions ran from
1% slower to 4% faster than its first (old 1357, 1345, 1340 s; window 1 2004, 2020, 2003 s;
window 16 2087, 2036, 2003 s). Three rounds can't separate that from load, and it is far
smaller than the gap between the old exporter and window 1.

## Paired runs

Epochs 276 to 300, each arm once in each position across three rounds. Epoch 300 is a
retained dump.

| Arm | Runs | Median wall | Spread | Median CPU | Peak RSS | Traversals tag/exact/entity |
| --- | --: | --: | --: | --: | --: | --- |
| old | 3 | 1345 s | 1340 to 1357 s | 1564 s | 2.68 GiB | not logged |
| window 1 | 3 | 2004 s | 2003 to 2020 s | 1720 s | 2.79 GiB | 24/24/24 |
| window 4 | 1 | 1838 s | | 1571 s | 2.17 GiB | 6/6/6 |
| window 16 | 3 | 2036 s | 2003 to 2087 s | 1544 s | 2.16 GiB | 2/2/2 |
| window 24 | 1 | 2151 s | | 1567 s | 2.00 GiB | 1/1/1 |

Paired reductions, by round:

| | Round 1 | Round 2 | Round 3 | Median |
| --- | --: | --: | --: | --: |
| Window 16 against old | -47.6% | -51.9% | -55.2% | **-51.9%** |
| Window 16 against window 1 | 0.8% | -1.6% | -4.2% | **-1.6%** |

Where each boundary's time goes, from log markers:

| Seconds a boundary | Replay | Capture | Stage | Publish, less staging | Whole run |
| --- | --: | --: | --: | --: | --: |
| old | 14.6 | | beside publish | 41.4 | 56.0 |
| window 1 | 14.9 | | 19.8 | 48.8 | 83.5 |
| window 4 | 16.2 | 8.9 | 4.3 | 47.2 | 76.6 |
| window 16 | 15.0 | 8.9 | 1.5 | 59.4 | 84.8 |
| window 24 | 14.7 | 8.9 | 0.7 | 65.3 | 89.6 |

## Why it comes out this way

These are findings for a decision, not fixes: nothing here changes the exporter.

1. **The index scan no longer overlaps the layers.** The old exporter ran its index bands
   on a reserved worker beside the layer pool. Dolos `bf66dcc` finishes staging first,
   because the layer count needs the entity presence it finds, then starts the pool
   (`crates/snapshot/src/export.rs:754,950`). That's about 20 s of the 28 s a batch of one
   adds. Only the entity traversal feeds that count, at about 0.4 s here, so the tag and
   exact scans might go back beside the pool. Nothing has tried that.
2. **A capture costs about as much as staging saves on this chain.** A window of `W`
   saves `(W - 1) / W` of about 20 s and pays about 9 s to capture, then compresses each
   payload again at the profile's level when it publishes.
3. **Publishing slows part way through a long batch, from about 47 s to about 69 s a
   sequence.** It starts 4 to 8 sequences in, at no fixed epoch, and a window of 4 never
   reaches it. It holds with the registry off the disk, so the registry isn't why. Samples
   early and late show the same hot path, so the profile doesn't explain it either. A
   host effect from sustained fsyncs isn't ruled out.
4. **Saving the publish record dominates a publication on this host, in every arm.**
   `Chained::landed` rewrites and syncs the whole record, under a lock, for each inheritable
   layer that lands, and a publication here carries about 4,500 of them
   (`stelae-driver/src/publish.rs:670`). In 20 s samples taken in mid-publish, a save was
   under way for 97% of the old exporter's, 93% of a batch's early and 70% of its late,
   with the file sync 44 to 77% of the time ([profiles.txt](profiles.txt)). This predates
   batches, and it sits on top of everything above: a publication's layers are mostly
   waiting for it. The samples cover the first part of a publication, so they don't give
   its whole share. The early and late samples aren't like for like, window 12 at median
   load 5.2 and window 8 at 17.1, so the fall from 93% to 70% doesn't read as a change
   within a batch. That run's chain files ran out at epoch 286, so the chain closed its
   batch at 10 boundaries, and its row reads `succeeded` from before the tool checked `--until`.

A batch's gain therefore depends on what a traversal costs against everything else, and
preprod's is small: about 20 s against about 41 s of layers. In one exploratory index-only
measurement a mainnet traversal took about 410 s. As a rough model, the old exporter takes
`replay + max(traversal, layers)` and a window takes `replay + capture + traversal / W +
layers`, so a window only beats the old exporter where a traversal clearly exceeds the
capture. That model is not a measurement.

## Parity

| Workload | What it covers | Runs | Tags | Result |
| --- | --- | --: | --: | --- |
| Genesis to 24 | Byron boundary blocks beside main blocks, every era crossing to 24, retained 4, 5, 6, 7 and 12 | old, 1, 4, 16 | 25 | identical |
| 276 to 300 | Full history, settling account logs, delayed seals, inherited layers, retained 300, a small final batch at 16 | 11 | 46 | identical |
| 276 to 292, registry off the disk | The same bytes, without a registry on the volume | 4 | 38 | identical |
| 276 to 282, with faults | Every kill, retry and upgrade below | 11 | 28 | identical |

`compare` also holds each `epoch-N` across all workloads on one lineage. The rollback over
an empty repository is left out of that, because it's meant to differ.

**Restores.** The Dolos binary from before batches restored each of the 24 publications
from genesis, and the publisher's digest of what it restored matched each byte for byte.
It did the same for two publications of a real batch over full history, epochs 277 and
284, all 26 reproduced. `stelae-cardano/tests/parity.rs` does the same for every publication of a
synthetic batch, and replays on to the next.

## What a stop costs

Epochs 276 to 282, window 6, against 457 s uninterrupted and window 1 against 496 s. Each
total is both processes.

| Fault | Arm | Killed at | Rerun | Total | Extra traversals |
| --- | --- | --: | --: | --: | --- |
| SIGKILL at a capture | 6 | 58 s | 404 s | 462 s | one the log doesn't count, per the docs |
| SIGKILL during staging | 6 | 165 s | 305 s | 470 s | tag and exact again |
| SIGKILL with a publication starting | 6 | 247 s | 192 s | 440 s | none |
| SIGKILL with a publication starting | 1 | 170 s | 316 s | 486 s | none |
| Registry refusing every 40th upload | 6 | | | 478 s | none |

Differences this small are within what this host varies by, so read these as "a stop costs
at most about what it interrupted". No upload completed twice in any of them, but a refused
one still crossed the wire. The log of the refusing row holds 13 refusals on 12 blobs,
47,659,965 bytes sent again against 2,274,877,727 uploaded, about 2%. The registry in these
runs counted only bodies it kept, so `received_bytes` equals `uploaded_bytes` on all of them.
A later run of the same fault, on a registry that counts refused bodies, received 47,802,989
bytes more than it kept from 13 refusals, and uploaded the same bytes. That run and the second
upgrade below shared the host with builds, at load 10 to 17 as they started, so their wall
times aren't comparable with these.

The tool that recorded these rows let a span run on across a kill. So the staging kill's
row in [tables.md](tables.md) gives 42 s of staging, from the killed process's first staging
traversal to the rerun's `index staging finished`, which takes in the rerun's opening of its
stores, where the rerun staged for 18 s. The rows stay as recorded, and the wall times
above don't depend on spans.

## Upgrade and rollback

| Case | Result |
| --- | --- |
| Old killed as a publication started, before it uploaded anything, then window 6 on the same stores | Finishes, identical bytes |
| Old killed part way through a publication, 2,996 layers in its publish record, then window 6 on the same stores | Window 6 carries those 2,996 layers forward, finishes with identical bytes and uploads no blob twice |
| Window 6 stopped with the stores ahead of `latest`, then old | Old refuses after its retries, 41 s, exit 1, publishes nothing. Window 6 then drains the batch, identical bytes |
| Window 4 stopped at its third boundary over an empty repository, then old | **Old publishes `epoch-3` onward, and `epoch-1` and `epoch-2` never exist** |

## Room, and the limits

Measured high-water, in GiB. "Volume" is how far the volume's free space fell, which also
catches unlinked upload staging, and anything else on that volume. It comes from the runs
with the registry off the disk, so its writes aren't in it.

| Arm | Bundles | Staged runs | Stores' growth, including both | Volume |
| --- | --: | --: | --: | --: |
| old | | | 1.0 | |
| window 1 | | | 1.0 | 4.9 |
| window 4 | 1.73 | 0.06 | 2.8 | 6.2 |
| window 16 | 6.54 | 0.19 | 7.1 | 10.6 |
| window 24 | 9.98 | 0.27 | 10.9 | |

A bundle is about 0.42 GiB and a staged window about 12 MiB, so it is the state and not
the index spool that sizes a batch: 35 to one here. The 6.97 GB spool of the earlier
index-only benchmark would have understated a batch by a long way.

"Stores' growth" is net of the chain files replay deleted, about 51 MiB an epoch. The tool
seeds a run's whole slice of chain files under the storage path, so `stores_at_start` follows
the slice's length, 14.86 to 15.75 GiB, and those deletions offset what the stores keep: the
old exporter's stood 0.86 GiB up over 4 epochs, 1.06 over 16 and 0.96 to 1.08 over 24. A real
run downloads bounded windows, and nothing offsets it. The stores themselves grew 94 to 99 MiB
an epoch on these runs, about 2.2 GiB over 24 epochs, and 112 from 256 to 276, without their
chain files. The 0.86 GiB is about 0.68 above growth less the slice, so about 0.7 GiB is
transient.

A publication's own check of its staging sums every state layer of the latest stele, retained
dumps included, and its largest other layers, as many as `--concurrency`. Read off the
manifests, `epoch-286` takes 0.65 GiB at these runs' `--concurrency 4` and 0.79 at the default
of 8, and `epoch-300`, with one dump, takes 1.04 and 1.17. This base carries no dump before
300, and a lineage from genesis holds 11 at this height, which wasn't measured.

| Window, scratch, reserve (MiB) | Batches over 8 epochs | Traversals tag/exact/entity |
| --- | --: | --- |
| 4, 4096, 512 | 2 | 2/2/2, as asked |
| 4, 2048, 512 | 4 | 4/4/4, closed early at two boundaries |
| 4, 1024, 256 | 8 | 8/8/8, closed early at one |

No window is chosen as a default, so `--batch-epochs` stays 1. Where someone batches
anyway, 4 is the window these runs support, and
[publisher.md](../../publisher.md#how-much-room-a-batch-takes) has the sizing rule.

## What this doesn't show

- **Mainnet.** No complete run. The plan is to measure it by replacing a running mainnet
  publisher, which is a separate operator action.
- **A remote registry.** The registry is local and takes about the same on every arm, and
  every arm uploads the same bytes, so a slow one dilutes any difference here.
- **Linux and a cloud volume.** A file sync on this host is `F_FULLFSYNC`, which may cost
  far more than a sync does on the deployed storage. That inflates finding 4 by an unknown
  amount, and may bear on finding 3.
- **A real full volume during publication.** The volume filled during these runs from
  other work, on another disk. Nothing here exercised it, and neither did anything try the
  free-space floor in [publisher.md](../../publisher.md#how-much-room-a-batch-takes).
- **A rollback with the stores one sequence ahead.** Read from both releases, not run.
- **Windows 4 and 24 have one run each,** the 4, 2048 and 4, 1024 limits runs and the
  registry-off runs have one run each, and the 4, 4096 run shared the host with other runs
  of this work, so its wall time isn't comparable.
- **Sampled disk use** is once a second, so a shorter peak can be missed.
- **The old exporter's traversals** aren't logged, so its count is by construction only.

## Runs stopped by hand

The host's system disk filled while the last checks ran. They were stopped to see if they
were the cause, and they weren't: the space didn't come back, and their data was on the
other disk. Under the tool as it then was, a first profile of the old exporter and the
restores of epochs 280 and 281 recorded nothing. The profile and the restore of epoch 284 ran
again, and 280 and 281 did not. A first attempt at the window 8 profile also sampled another run's process, and
that sample was discarded and taken again.

The first three-arm trial, and the run that took the base from epoch 256 to 276, ran under
an earlier version of the tool, and are in [observations-early.jsonl](observations-early.jsonl).
