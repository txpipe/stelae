# Publisher — deployment unit

The chart in [chart/](chart/) runs one `stelae-publisher` Job per network.
The publisher restores its latest stele, replays to epoch boundaries,
publishes, prunes, and exits at the aggregator tip. The operator maintains
network values and owns deployment, first publication and monitoring.

The chart runs a publisher built on the Dolos v2 configuration: `dolosToml`
is a v2 file keeping full history and the retained state epochs are their
own value. There is no host selector. The directory and chart name stay
`dolos-publisher` to preserve release and object naming.

## Rendered resources

- ConfigMap `<release>-config`, mounted at `/etc/publisher`, carries
  `dolos.toml` verbatim from `dolosToml` and `state-epochs`, one epoch per
  line, from `stateEpochs`. No shell entrypoint is generated. The retained
  epochs are signed input and must not change incidentally.
- PersistentVolumeClaim `<release>-data`, only with `persistence.enabled`
  and no `persistence.existingClaim`. It carries
  `helm.sh/resource-policy: keep`, so `helm uninstall` leaves it.
- Job `<network>-backfill-<run>` directly invokes
  `/usr/local/bin/stelae-publisher --config /etc/publisher/dolos.toml run
  --state-epochs-file /etc/publisher/state-epochs`. It mounts `/data` (see
  [Data volume](#data-volume)) and reads registry credentials from the existing Secret's
  `DOLOS_STELAE_REGISTRY_USER` and `DOLOS_STELAE_REGISTRY_PASSWORD` keys into
  the publisher's `DOLOS_STELAE_USER` and `DOLOS_STELAE_PASSWORD`.

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
  the latest stele, all of history, and `resources` bounds `/data` through
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
  harmless while `latest` is the same stele. Nothing refuses a changed
  `repo` or network, though: a different stele lands over the stale rows,
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
  Every restart refuses them before any fetch, saying the archive has a gap
  there (see [publisher.md](../../docs/publisher.md#a-volume-that-fills)).

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
| `run` | Deliberate re-run counter; bump when changing the Job |
| `repo` | Network's `oci://` publication repository |
| `concurrency` | Measured upload concurrency, with no chart default |
| `dolosToml` | Network's Dolos v2 configuration, verbatim |
| `stateEpochs` | Retained state-dump epochs, ascending, with no chart default; `[]` retains the tip alone |
| `image.repository` | Defaults to `ghcr.io/txpipe/stelae-publisher` |
| `image.tag` | Tested version or `sha-<7>` tag, with no default |
| `resources` | Network working set, with no default |
| `persistence.enabled` | Mount `/data` from a claim instead of an emptyDir; default `false` |
| `persistence.size` | Size of the claim the chart creates, with no default |
| `persistence.storageClassName` | Class of the created claim; empty uses the cluster default |
| `persistence.existingClaim` | Claim to mount instead of creating one |

[values.yaml](chart/values.yaml) also exposes transport, secret name, retry
budget, termination grace period and placement. Initialization fails closed
by default. `allowGenesisFallback: true` adds `--allow-genesis`; use it only
for a deliberately verified first publication.

## Validation

```sh
k8s/dolos-publisher/test.sh
helm template stelae-publisher-preprod k8s/dolos-publisher/chart \
  --namespace stelae-publisher --values "$PUBLISHER_VALUES"
```

The checks cover direct invocation, the state epochs file and argument,
both `/data` modes, genesis opt-in, TLS selection, missing or invalid configuration and
accidental use of the legacy Dolos image.

## Upgrade and rollback

A Job's pod template is immutable. Bump `run` to create a new Job for an
upgrade or re-run. Helm also removes the old Job, but that replacement order
does not guarantee a single writer: stop the previous writer and wait for its
pod to terminate before starting the next run. With an emptyDir the new Job
restores from `latest`. With a persistent claim it resumes the kept stores,
so delete the claim first when they no longer fit, and always for a
rollback away from a suspect image (see [Data volume](#data-volume)).

Rolling back to chart 0.3 drops the claim from the Job: 0.3 accepts the
`persistence` block but mounts an emptyDir, so a full-history publisher
restores all of history into ephemeral storage. The claim stays, and a
later 0.4 install with persistence adopts it again.

An image on a new Dolos profile pin cannot continue a repository cut by an
earlier one: restoring or chaining onto an older stele is unsupported, so the
move starts a fresh lineage in an emptied repository, as
[First publication on the chart](../../docs/publisher-packaging.md#first-publication-on-the-chart)
describes. Rollback to the earlier image works only while the repository
still holds its lineage, that is, before its tags are deleted. Never reuse
incompatible persistent scratch or an unpublished checkpoint across images.

Full build and release details, and the first publication of a fresh
lineage, are in
[publisher-packaging.md](../../docs/publisher-packaging.md).
