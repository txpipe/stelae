# Dolos publisher — deployment unit

The publisher is one Kubernetes Job per network, rendered from the Helm
chart in [`chart/`](chart/) with a values file per network maintained by the
operator. The operator's runbook owns deployment, re-run, first-run, and
monitoring procedures.
Each Job runs the official `ghcr.io/txpipe/dolos` image on the Demeter m2
EKS cluster: it restores its network's latest stele (or starts from
genesis), replays to the next epoch boundary, publishes, prunes, repeats,
and exits 0 at the aggregator tip. Each publish is its own checkpoint, so a
pod restart costs one epoch and nothing more.

## What the chart renders

Release `stelae-publisher-<network>` in namespace `stelae-publisher` — the
registry's namespace, which the chart does not create; the deploy passes
`--namespace`. Two objects:

- **ConfigMap** `<release>-config`, mounted at `/etc/publisher`:
  `entrypoint.sh`, the restore-or-genesis script templated from the chart
  (`concurrency` and `--insecure` from values), and `dolos.toml`, the
  network's config carried **verbatim** from the values string `dolosToml`.
  Its `[snapshot] state_epochs` list is signed input, frozen at the
  network's first publish (decisions 0028, 0030, 0038) and extended only
  above the then-current tip. Carrying the file verbatim means no template
  can change it silently, and its comments stay in the values file.
- **Job** `<network>-backfill-<run>`: the official image pinned by
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
| `run` | the re-run counter |
| `repo` | the `oci://` URL the publisher writes |
| `concurrency` | uploads in flight per publish — measured per network, never a default |
| `dolosToml` | the network's `dolos.toml`, verbatim |
| `image.tag` | the `sha-<short>` pin; there is no chart-wide dolos version |
| `resources` | the working set; no default |

Everything else defaults in [`chart/values.yaml`](chart/values.yaml)
with its reason beside it: `insecure` on (the write path is in-cluster plain
HTTP), `publisherSecretName`, `backoffLimit`,
`terminationGracePeriodSeconds`, and empty placement.

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
helm lint k8s/dolos-publisher/chart --values "$PUBLISHER_VALUES"
helm template stelae-publisher-preprod k8s/dolos-publisher/chart \
    --namespace stelae-publisher --values "$PUBLISHER_VALUES"
```

Its predecessors are in git history: raw manifests applied by hand, and before
them a Cloudflare Worker with a container-backed Durable Object.
