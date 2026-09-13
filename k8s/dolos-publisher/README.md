# Publisher — deployment unit

The publisher is one Kubernetes Job per network, rendered from the Helm
chart in [`chart/`](chart/) with a values file per network maintained by the
operator. The operator's runbook owns deployment, re-run, first-run, and
monitoring procedures.
The chart retains the official `ghcr.io/txpipe/dolos` host by default. An
operator explicitly selecting `host: stelae` and the Stelae image runs the
new host without a Dolos executable. Both replay to epoch boundaries, publish,
prune, repeat, and exit 0 at the aggregator tip. Each publication remains its
own checkpoint.

## What the chart renders

Release `stelae-publisher-<network>` in namespace `stelae-publisher` — the
registry's namespace, which the chart does not create; the deploy passes
`--namespace`. Two objects:

- **ConfigMap** `<release>-config`, mounted at `/etc/publisher`:
  the legacy `entrypoint.sh` and `dolos.toml`, the
  network's config carried **verbatim** from the values string `dolosToml`.
  Its `[snapshot] state_epochs` list is signed input, frozen at the
  network's first publish (decisions 0028, 0030, 0038) and extended only
  above the then-current tip. Carrying the file verbatim means no template
  can change it silently, and its comments stay in the values file.
- **Job** `<network>-backfill-<run>`: the selected image pinned by
  `image.tag`, `/data` an `emptyDir`, the ConfigMap at `/etc/publisher`, the
  registry pair from the Secret named `publisherSecretName`
  (`stelae-registry-publisher` — referenced, never templated).

## The `run` mechanism

A Job's pod template is immutable: it cannot be upgraded in place. The
chart makes the re-run a deliberate act by putting a counter in the Job's
name. Bumping `run` in the values file and upgrading makes Helm create the
new Job and remove the old one; the new pod restores from `latest` and the
epoch cost is paid on purpose. Changing anything else in the pod template
without a bump fails the upgrade against the immutable field — the guard
the raw manifests lacked. Deleting a Job by hand instead of bumping leaves
the release history and the cluster disagreeing: the next upgrade recreates
it under the old name. Bump, never delete.

## Values a network must supply

| value | holds |
|---|---|
| `network` | prefixes the Job name, labels the pod |
| `host` | `dolos` (default compatibility mode) or explicit `stelae` |
| `run` | the re-run counter |
| `repo` | the `oci://` URL the publisher writes |
| `concurrency` | uploads in flight per publish — measured per network, never a default |
| `dolosToml` | the network's `dolos.toml`, verbatim |
| `image.repository` | defaults to Dolos; set to `ghcr.io/txpipe/stelae-publisher` with `host: stelae` |
| `image.tag` | the `sha-<short>` or version pin; there is no chart-wide application version |
| `resources` | the working set; no default |

Everything else defaults in [`chart/values.yaml`](chart/values.yaml)
with its reason beside it: `insecure` on (the write path is in-cluster plain
HTTP), `allowGenesisFallback: false`, `publisherSecretName`, `backoffLimit`,
`terminationGracePeriodSeconds`, and empty placement.

## Host compatibility

`host: dolos` preserves chart 0.1 behavior, including its shell entrypoint and
legacy restore-or-genesis fallback. The new `allowGenesisFallback` value is not
applied to that path, so upgrading the chart alone cannot change a running
publisher's behavior.

`host: stelae` runs `/usr/local/bin/stelae-publisher` directly with `run`.
Registry credentials keep their `DOLOS_STELAE_REGISTRY_*` names and the same
Secret keys. The configuration, repository, concurrency, insecure transport,
mounts, resources, placement, retry budget and termination grace period are
the same chart values. Initialization fails closed by default;
`allowGenesisFallback: true` adds `--allow-genesis` and is only valid for a
deliberately verified first publication.

The Stelae image carries genesis at `/etc/genesis/<network>/`, so the existing
`dolos.toml` paths remain valid. Its storage, initialization marker, publication
journal, scratch and Mithril downloads remain beneath `/data/db` for the
current configurations. Direct PID-1 execution delivers SIGTERM to the new
host's cancellation watcher.

## Constraints

- **Concurrency is a network fact.** Mainnet 16 because zot parallelises
  where the old Worker did not; the testnets 4 because in-cluster zot
  serialises blob commits, and a deeper queue only adds latency to any
  mainnet window beside it. Never lowered to make bursts smaller.
- **Single-publisher discipline.** Exactly one writer per
  `cardano/<network>`, ever.
- **Placement is a value.** The dedicated `stele-backfill` nodegroup was
  deleted on 2026-08-31; every network runs on the shared best-effort pool
  today, and a dedicated node again is a `nodeSelector` and `tolerations`
  change in that network's values file.

Local check, no cluster needed:

```bash
k8s/dolos-publisher/test.sh
helm lint k8s/dolos-publisher/chart --values "$PUBLISHER_VALUES"
helm template stelae-publisher-preprod k8s/dolos-publisher/chart \
  --namespace stelae-publisher --values "$PUBLISHER_VALUES"
```

The contract check renders legacy, fail-closed Stelae and explicit-genesis
Stelae modes and rejects invalid host or missing configuration values.

## Upgrade and rollback

A cutover sets `host: stelae`, changes `image.repository` and `image.tag`,
leaves `allowGenesisFallback: false` for an established lineage, and bumps
`run`. Never start it until the prior writer has stopped.

Rollback is another deliberate run: prove the Stelae Job has stopped, restore
`host: dolos` and the prior tested image, then bump `run`. The chart's emptyDir
starts clean and restores the compatible published head. A successor using
persistent scratch/storage must discard it and restore fresh whenever the two
images do not share the same Dolos storage pin, journal schema and profile
version; an unpublished or incompatible checkpoint is not rollback input.

Its predecessors are in git history: raw manifests applied by hand, and before
them a Cloudflare Worker with a container-backed Durable Object.
