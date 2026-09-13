# Publisher — deployment unit

The chart in [chart/](chart/) runs one `stelae-publisher` Job per network.
The publisher restores its latest stele, replays to epoch boundaries,
publishes, prunes, and exits at the aggregator tip. The operator maintains
network values and owns deployment, first publication and monitoring.

Chart 0.2 requires the Stelae image. Existing Dolos deployments remain pinned
to chart 0.1 until cutover; rollback uses that previous chart and image.
There is no host selector. The directory and chart name stay
`dolos-publisher` to preserve release and object naming.

## Rendered resources

- ConfigMap `<release>-config`, mounted at `/etc/publisher`, carries
  `dolos.toml` verbatim from `dolosToml`. No shell entrypoint is generated.
  The retained `[snapshot] state_epochs` list remains signed input and must
  not change incidentally.
- Job `<network>-backfill-<run>` directly invokes
  `/usr/local/bin/stelae-publisher --config /etc/publisher/dolos.toml run`.
  It mounts an emptyDir at `/data` and reads registry credentials from the
  existing Secret, using `DOLOS_STELAE_REGISTRY_USER` and
  `DOLOS_STELAE_REGISTRY_PASSWORD`.

The image bundles genesis at `/etc/genesis/<network>/`. Existing config
paths remain valid, including storage, markers, journals, scratch and Mithril
downloads beneath `/data/db`. Direct PID-1 execution delivers SIGTERM to
the publisher.

## Values

| Value | Purpose |
| --- | --- |
| `network` | Job name prefix |
| `run` | Deliberate re-run counter; bump when changing the Job |
| `repo` | Network's `oci://` publication repository |
| `concurrency` | Measured upload concurrency, with no chart default |
| `dolosToml` | Network configuration, verbatim |
| `image.repository` | Defaults to `ghcr.io/txpipe/stelae-publisher` |
| `image.tag` | Tested version or `sha-<7>` tag, with no default |
| `resources` | Network working set, with no default |

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

The checks cover direct invocation, genesis opt-in, TLS selection, missing
configuration and accidental use of the legacy Dolos image.

## Upgrade and rollback

A Job's pod template is immutable. Bump `run` to create a new Job for an
upgrade or re-run. Helm also removes the old Job, but that replacement order
does not guarantee a single writer: stop the previous writer and wait for its
pod to terminate before starting the next run.

For cutover, select chart 0.2, set the Stelae image repository and tested tag,
bump `run`, and retain the existing config, secret and tuning values.
Keep `allowGenesisFallback: false` for established repositories.

For rollback, stop the Stelae writer, select the pinned 0.1 chart and previous
tested Dolos image, and bump `run` again. The new emptyDir restores the
compatible published head. Never reuse incompatible persistent scratch or an
unpublished checkpoint across hosts.

Full build, release and rollback details are in
[publisher-packaging.md](../../docs/publisher-packaging.md).
