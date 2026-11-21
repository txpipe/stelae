# Publisher — deployment unit

The chart in [chart/](chart/) runs one `stelae-publisher` per network, as a
one-shot Job or, with `schedule` set, as a CronJob (see
[Scheduled runs](#scheduled-runs)).
The publisher restores its latest snapshot, replays to epoch boundaries,
publishes, prunes, and exits at the aggregator tip. The operator maintains
network values and owns deployment, first publication and monitoring.

The chart runs a publisher built on the Dolos v2 configuration: `dolosToml`
is a v2 file keeping full history. There is no host selector. The directory and chart name stay
`dolos-publisher` to preserve release and object naming.

## Getting the chart

A release publishes the chart as an OCI package at its own `version:`. Pull
or install a pinned version without a checkout:

```sh
helm pull oci://ghcr.io/txpipe/charts/dolos-publisher --version <v>
```

## Rendered resources

- ConfigMap `<release>-config`, mounted at `/etc/publisher`, carries
  `dolos.toml` verbatim from `dolosToml`. No shell entrypoint is generated.
- PersistentVolumeClaim `<release>-data`, only with `persistence.enabled`
  and no `persistence.existingClaim`. It carries
  `helm.sh/resource-policy: keep`, so `helm uninstall` leaves it.
- Job `<network>-backfill-<run>` directly invokes
  `/usr/local/bin/stelae-publisher --config /etc/publisher/dolos.toml run`.
  It mounts `/data` (see
  [Data volume](#data-volume)) and reads registry credentials from the existing Secret's
  `DOLOS_STELAE_REGISTRY_USER` and `DOLOS_STELAE_REGISTRY_PASSWORD` keys into
  the publisher's `DOLOS_STELAE_USER` and `DOLOS_STELAE_PASSWORD`.
  Rendered only when `schedule` is empty.
- CronJob `<network>-publisher`, only when `schedule` is set, in place of the
  Job. Its runs have the Job's pod, labelled `app: <network>-publisher`.
- CronJob `<network>-freshness`, only when `freshness.schedule` is set, with
  or without `schedule`. See [Freshness check](#freshness-check).

A known network (`mainnet`, `preprod`, `preview`) takes its genesis and
Mithril aggregator from the Dolos catalog, so `dolosToml` names neither.
Storage, markers, journals, scratch and Mithril downloads stay beneath the
configured `storage.path`, `/data/db` in the deployed values. Direct PID-1 execution delivers SIGTERM to
the publisher.
The pod requires a non-root image user and sets `fsGroup: 65532`, giving the
image's UID/GID `65532:65532` write access to `/data`.

## Data volume

`/data` holds the publisher's stores, so where it lives sets the restart
cost.

- **emptyDir** (the default). Every pod restart, eviction included, restores
  the latest snapshot, all of history, and `resources` bounds `/data` through
  its `ephemeral-storage` request and limit, which must hold the whole
  archive. A failed container reruns in the same pod and keeps `/data`;
  deleting the pod or bumping `run` starts over. It suits only a network whose
  history restores quickly.
- **Persistent claim** (`persistence.enabled: true`). `/data` is a
  ReadWriteOnce claim that outlives the pod: a restarted pod resumes the
  stores it left, with no restore. Initialization checks those stores
  against the marker the first restore wrote, which records the network and
  `repo` but not the image or the store layout. Without
  a marker nothing is checked, and a pod killed during its first restore
  leaves none. Killed after the restore committed its cursor, the stores are
  accepted unchecked by every later run. Killed before, they hold partial
  rows with no cursor, and the next run restores into them without clearing
  them. Restarts of the same Job restore the same `repo` again, which is
  harmless while `latest` is the same snapshot. Nothing refuses a changed
  `repo` or network, though: a different snapshot lands over the stale rows,
  which then become authoritative. Delete the claim when a first restore
  did not complete, before the next run.
  The chart creates `<release>-data` of `persistence.size` in
  `persistence.storageClassName` (empty uses the cluster default), or mounts
  `persistence.existingClaim` and creates nothing. The pod sets
  `fsGroupChangePolicy: OnRootMismatch`, so a remount does not re-own the
  whole store. `ephemeral-storage` no longer covers `/data`; size the claim
  instead.

Every publisher keeps full history: `dolosToml` leaves `[history]` at its
default `mode = "full"`. Dolos cuts a boundary only from an archive that
starts at the chain's origin, so the publisher refuses a `rolling` or `none`
history before it opens a store; a rolling window would publish until its
first prune and then fail every later run. Mainnet at full history needs
about 250 GiB and the persistent claim: from an emptyDir every eviction
would restore all of it.

On EKS an EBS volume lives in one availability zone, and every later pod is
scheduled only in that zone. Use a `WaitForFirstConsumer` class, so the
volume is created where the first pod lands, and make sure the
`nodeSelector` pool has capacity in that zone; otherwise the pod stays
Pending. An `existingClaim` must already be in a zone the pool reaches.

The claim survives `helm uninstall`. Installing the same release again
adopts it. To start the stores over, stop the writer, then delete the claim
with `kubectl` before the next run. With `existingClaim`, also create that
claim again, empty, before the next run; until it exists the pod stays
Pending. Start over whenever the kept stores no longer fit the release:

- a changed `repo` or network, including a fresh lineage cut into the same
  repository. Where the stores carry a marker, a changed `repo` fails every
  restart until `backoffLimit` on the mismatch; without one nothing refuses
  it (see above). A re-cut would resume the old lineage's stores;
- an image upgrade or rollback whose stores are incompatible. Initialization
  refuses stores the image's Dolos will not open, and fails every restart
  until `backoffLimit`, but does not check the image itself;
- a rollback away from a suspect image. The rolled-back image continues
  from the state the suspect image left, and every boundary it publishes
  afterwards is built on that state;
- stores whose state passed the archive, which a kill or a full volume can
  leave, including a cursor with no block hash. Every restart refuses them
  until `backoffLimit`. The check can't vouch for stores that crossed a gap
  on an image without it (see
  [publisher.md](../../docs/publisher.md#stores-whose-state-passed-the-archive));
- stores a rerun imported across an absent file next to an epoch's start.
  Unless the repository already holds that boundary, every restart refuses
  them before any fetch, saying the archive has a gap there (see
  [publisher.md](../../docs/publisher.md#a-volume-that-fills)).

Once the claim exists its class is fixed. Naming a class other than the
one the claim has fails, and so does clearing a named class back to empty,
even when that class is the cluster default: once a class is named, keep
naming it. Left empty at the first install, the claim got the cluster
default, and naming that same class later is no change; on a cluster with no
default class it got none and stays Pending, and naming any class then
succeeds. The size can only
grow, and only on a Bound claim of a class that sets `allowVolumeExpansion`.
The one exception, on Kubernetes 1.32 and later: while an expansion has not
finished, `size` can be lowered again to a value above the claim's current
capacity, which is how a failed expansion is recovered. Changing the class,
lowering `size` otherwise, or raising it on a Pending claim or a class
without expansion makes `helm upgrade` report failure. That failure
is not atomic: with a bumped `run` the new Job
is still created and runs on the claim, and the old Job is not removed and
keeps running, so two writers share the claim. Each further failed upgrade
adds another Job. To recover, delete the release's backfill Jobs with
`kubectl` and wait for their pods to terminate, then fix the values, bump
`run` and upgrade again. Name the class at the first install.

## Values

| Value | Purpose |
| --- | --- |
| `network` | Job name prefix |
| `run` | Deliberate re-run counter; bump when changing the Job. Required without `schedule`, ignored with it |
| `schedule` | Cron schedule; set, renders the CronJob instead of the Job. Needs `persistence.enabled` |
| `suspend` | Scheduled runs: start no new run; default `false` |
| `startingDeadlineSeconds` | Scheduled runs: how late a missed run may still start; default 3600 |
| `successfulJobsHistoryLimit`, `failedJobsHistoryLimit` | Scheduled runs: finished Jobs kept; default 3 each |
| `activeDeadlineSeconds` | Scheduled runs: seconds a run may last; unset by default |
| `repo` | Network root, `oci://`: each boundary's blocks snapshot goes to its `blocks` repository, then the Dolos snapshot naming it to its `dolos` repository |
| `concurrency` | Measured upload concurrency, with no chart default |
| `dolosToml` | Network's Dolos v2 configuration, verbatim |
| `image.repository` | Defaults to `ghcr.io/txpipe/stelae-publisher` |
| `image.tag` | Tested version or `sha-<7>` tag, with no default |
| `resources` | Network working set, with no default |
| `persistence.enabled` | Mount `/data` from a claim instead of an emptyDir; default `false` |
| `persistence.size` | Size of the claim the chart creates, with no default |
| `persistence.storageClassName` | Class of the created claim; empty uses the cluster default |
| `persistence.existingClaim` | Claim to mount instead of creating one |
| `batch.epochs` | Boundaries in a prepared batch, at least 2; unset publishes one at a time |
| `batch.scratchMib` | MiB a batch may hold on disk, with no default |
| `batch.reserveMib` | MiB kept free per prepared boundary, with no default |
| `freshness.schedule` | Cron schedule of the freshness check; empty renders none |
| `freshness.deadlineHours` | Hours after an epoch starts by which its snapshot is due; required with `freshness.schedule` |
| `freshness.repo` | Network root checked; defaults to `repo` |
| `freshness.insecure` | Plain HTTP to `freshness.repo`; defaults to `insecure` |
| `freshness.secretName` | Secret with the reader's `DOLOS_STELAE_REGISTRY_USER` and `DOLOS_STELAE_REGISTRY_PASSWORD`; empty passes no credentials |
| `freshness.resources` | The check's resources; required with `freshness.schedule` |
| `freshness.backoffLimit` | Retries before a check is marked failed; default 2 |

[values.yaml](chart/values.yaml) also exposes transport, secret name, retry
budget, termination grace period and placement. Initialization fails closed
by default. `allowGenesisFallback: true` adds `--allow-genesis`; use it only
for a deliberately verified first publication.

## Batching

By default the Job publishes one boundary at a time. Setting `batch.epochs`,
`batch.scratchMib` and `batch.reserveMib` together passes `--batch-epochs`,
`--batch-scratch-mib` and `--batch-reserve-mib`, so the run prepares several
boundaries and publishes them in order. Set all three or none: a partial set
fails the render, as the publisher would refuse it. Changing them changes the
pod template, so bump `run`.

**Sequences appear later.** A sequence reaches the repository only when its
batch closes, so during a backfill `latest` trails the stores by up to a
batch. Consumers restoring meanwhile get the last one published. At the tip a
run publishes what the source offered without waiting for a full batch.

**Sizing.** A batch lives on `/data`, under
`<storage.path>/.stelae-batch/`, on top of the stores. For a window of `W`
boundaries over a run of `N` epochs:

```text
batch.scratchMib        >=  W x (bundle + reserve)
batch.reserveMib        >=  staged runs + replay growth + download, per boundary
free space on /data     >=  W x (bundle + reserve) + one publication's check + transient + N x growth
```

On preprod at the measured height a bundle is about 0.42 GiB and the reserve
about 158 to 175 MiB with the download on the same volume, so a window of 4
needs a `scratchMib` of about 2.3 GiB and at least 4 GiB free beyond the
stores and their growth over the run, with the stores growing about 0.1 GiB an
epoch. That 4 GiB is a floor, measured on a lineage with no dump at a
`concurrency` of 4: the default of 8 adds about 0.13 GiB, and a run that held
its window was seen to use 6.2 GiB. Size the persistent claim, or the
emptyDir's `ephemeral-storage`, for that free space on top of the stores, with
margin. A bundle follows the network's state,
so measure a network not listed by reading `.stelae-batch/prepared` after the
first capture. A budget that holds one boundary but is too small for the
window does not fail: batches close
early and pay their own traversals, which is worse than a smaller window. The
measurements, the window to start from and the cost of each term are in
[publisher.md](../../docs/publisher.md#prepared-batches).

On a persistent claim, a run that fills `/data` stops on the room it lacks:
make room and rerun, see
[A volume that fills](../../docs/publisher.md#a-volume-that-fills). On an
emptyDir it doesn't: the publisher reads the node's free space, not the pod's
`ephemeral-storage` limit, so overrunning that limit evicts the pod and loses
`/data`. The next pod restores the same `latest` and prepares the same batch,
so an undersized limit repeats without progress until `backoffLimit`. There
`scratchMib` and the `ephemeral-storage` size are the only bounds. A restart
on a persistent claim goes on from the journaled batch, or refuses where the
stores and journal disagree, as
[What a restart does](../../docs/publisher.md#what-a-restart-does) lists. With
an emptyDir, a pod restart loses the batch with the stores and restores from
`latest`.

While a persistent claim holds a pending batch, a run without the flags
refuses and names the flags that publish it. The chart passes no
`--until-epoch`, so a batch drains only when a Job runs to the tip and
completes, after which its journal under `.stelae-batch/` is gone. Until then keep `image.tag` and the
`batch` values the batch was prepared with: unset the values, upgrade the
image or roll back to a chart without them only once the batch has drained, as
[Upgrade and rollback](../../docs/publisher.md#upgrade-and-rollback)
explains.

## Scheduled runs

Setting `schedule` renders the CronJob `<network>-publisher` instead of the
Job. Each run is `stelae-publisher run` on the kept claim, so `schedule`
needs `persistence.enabled`: on an emptyDir every run would restore all of
history, and the render refuses it.

- **Nothing to publish.** A run resumes the stores the last one left,
  publishes any boundary Mithril has added since, and exits 0 when there is
  nothing further, publishing nothing. Running more often than epochs close
  is safe, and most runs are of that kind.
- **One writer.** `concurrencyPolicy: Forbid` is the single-writer guarantee
  between scheduled runs: a run due while one is still in flight is held
  back, and starts late within `startingDeadlineSeconds` or is skipped until
  the next time. It covers only the CronJob's own runs. A Job started by
  hand beside it, `kubectl create job --from=cronjob/...` included, is a
  second writer on the claim; preventing that is the operator's. Set
  `suspend: true`, wait for the run in flight to finish, and only then start
  one by hand. Within a run, `podReplacementPolicy: Failed` keeps a
  replacement pod from starting until the pod it replaces has terminated, so
  a run's pod that is deleted or evicted does not overlap its successor.
  The schedule requires Kubernetes 1.31 or later. Before 1.31 a Job is
  marked failed while its pod may still be terminating, for up to its grace
  period, and the next run can start beside that pod on the claim; no choice
  of values prevents it. Do not run the schedule on an older cluster: use the
  one-shot Job there.
- **Failure.** A Job is marked failed only after `backoffLimit` restarts,
  and until then it holds the schedule back under `Forbid`. The default of
  200 suits a one-shot backfill on spot capacity; a scheduled release wants a
  small `backoffLimit`, so a run that keeps failing shows as failed within
  one interval. `activeDeadlineSeconds` bounds a run that hangs.
- **From the one-shot Job.** Moving a release to the schedule is one
  upgrade that sets `schedule` (and `persistence.enabled`, which a release
  on the claim already has). It removes the finished Job and keeps the
  claim, so the first scheduled run resumes the Job's stores. Wait for the
  Job to finish first: removing a Job in flight stops its pod, and the first
  scheduled run may start before that pod has terminated.
- **Back to the one-shot Job.** Clearing `schedule`, or rolling back to a
  chart before 0.5, deletes the CronJob and creates the Job in the same
  upgrade. Deleting the CronJob stops its run in flight, whose pod keeps
  writing to the claim until it terminates, beside the new Job. Set
  `suspend: true` first, wait for the run in flight to finish and its pod to
  terminate, and only then upgrade.
- **Changes.** An image or values change takes effect at the next run, with
  no `run` bump: each run is a new Job from the CronJob's current template,
  and a run in flight keeps the template it started with. The claim rules of
  [Data volume](#data-volume) still apply: when the kept stores no longer fit
  the release, suspend the schedule, wait for the run in flight, delete the
  claim, and only then upgrade and resume.

## Freshness check

A publisher run that succeeds does not say the network is current: when
Mithril stalls, runs succeed with nothing to publish, and when the schedule
stops, none run. Setting `freshness.schedule` renders a second CronJob,
`<network>-freshness`, whose runs are `stelae-publisher freshness` (see
[the publisher's commands](../../docs/publisher.md#checking-freshness)): each
fails once the root's `latest` is behind the newest epoch that started
`freshness.deadlineHours` or more hours ago, or when it holds no `latest`.
Nothing is delivered: the failed Job, after `freshness.backoffLimit` retries,
is the signal, for whatever reads Jobs from kube-state-metrics.

- **What it reads.** It mounts the release's ConfigMap for the network and no
  `/data`, holds no claim and writes nothing, so it never competes with the
  publisher. It runs with or without `schedule`, and suspending the publisher
  leaves it running, which is when it is wanted.
- **Where it reads.** The check is most useful against the public hostname
  consumers read, `oci://<public host>/cardano/<network>` in
  `freshness.repo` with `freshness.insecure: false`, rather than the
  in-cluster write path: it then also fails when the publisher writes but
  readers cannot see it.
- **As whom.** With no `freshness.secretName` it passes no credentials, so it
  reads as the network's published read-only user on the official registry,
  and anonymously elsewhere. Name a Secret only for a registry that needs a
  reader's account; the publisher's own Secret is a writer.
- **Choosing the deadline.** Mithril certifies a boundary's files 18 to 23
  hours after it; add the publisher's interval, a run's duration and a margin.
  The [publisher docs](../../docs/publisher.md#checking-freshness) have the
  reasoning.

## Validation

```sh
k8s/dolos-publisher/test.sh
helm template stelae-publisher-preprod k8s/dolos-publisher/chart \
  --namespace stelae-publisher --values "$PUBLISHER_VALUES"
```

The checks cover direct invocation, the removed `stateEpochs` value refused,
both `/data` modes, the scheduled form and its refusals, the freshness
check and its refusals, batching all or none, genesis opt-in, TLS selection, missing or invalid configuration and
accidental use of the legacy Dolos image.

## Upgrade and rollback

This section is about the one-shot Job; a scheduled release takes a change
at its next run, and moving it back to the Job, by values or by rollback,
needs the schedule suspended first (see [Scheduled runs](#scheduled-runs)). A Job's pod
template is immutable. Bump `run` to create a new Job for an
upgrade or re-run. Helm also removes the old Job, but that replacement order
does not guarantee a single writer: stop the previous writer and wait for its
pod to terminate before starting the next run. With an emptyDir the new Job
restores from `latest`. With a persistent claim it resumes the kept stores,
so delete the claim first when they no longer fit, and always for a
rollback away from a suspect image (see [Data volume](#data-volume)). While a
batch is pending, change neither the image nor the `batch` values until a Job
has drained it (see [Batching](#batching)).

Rolling back to chart 0.3 drops the claim from the Job: 0.3 accepts the
`persistence` block but mounts an emptyDir, so a full-history publisher
restores all of history into ephemeral storage. The claim stays, and a
later 0.4 install with persistence adopts it again.

An image on a new Dolos profile pin cannot continue a lineage cut by an
earlier one: restoring or chaining onto an older snapshot is unsupported, so the
move starts a fresh lineage in the root's emptied `dolos` repository, as
[First publication on the chart](../../docs/publisher-packaging.md#first-publication-on-the-chart)
describes. A lineage from before the diff chain sits at the root itself and
is left alone. Rollback to the earlier image works only while its lineage is
still held: at the root for an image from before the diff chain, in `dolos`
before its tags are deleted for a later one. Never reuse incompatible
persistent scratch or an unpublished checkpoint across images.

Full build and release details, and the first publication of a fresh
lineage, are in
[publisher-packaging.md](../../docs/publisher-packaging.md).
