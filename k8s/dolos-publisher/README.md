# Publisher — deployment unit

The chart in [chart/](chart/) runs one `stelae-publisher` Job per network.
The publisher restores its latest stele, replays to epoch boundaries,
publishes, prunes, and exits at the aggregator tip. The operator maintains
network values and owns deployment, first publication and monitoring.

Chart 0.3 runs a publisher built on the Dolos v2 configuration: `dolosToml`
is a v2 file and the retained state epochs are their own value. Releases on
chart 0.2 keep their v1 configuration until rewritten for 0.3. There is no
host selector. The directory and chart name stay
`dolos-publisher` to preserve release and object naming.

## Rendered resources

- ConfigMap `<release>-config`, mounted at `/etc/publisher`, carries
  `dolos.toml` verbatim from `dolosToml` and `state-epochs`, one epoch per
  line, from `stateEpochs`. No shell entrypoint is generated. The retained
  epochs are signed input and must not change incidentally.
- Job `<network>-backfill-<run>` directly invokes
  `/usr/local/bin/stelae-publisher --config /etc/publisher/dolos.toml run
  --state-epochs-file /etc/publisher/state-epochs`. It mounts an emptyDir at
  `/data` and reads registry credentials from the existing Secret's
  `DOLOS_STELAE_REGISTRY_USER` and `DOLOS_STELAE_REGISTRY_PASSWORD` keys into
  the publisher's `DOLOS_STELAE_USER` and `DOLOS_STELAE_PASSWORD`.

A known network (`mainnet`, `preprod`, `preview`) takes its genesis and
Mithril aggregator from the Dolos catalog, so `dolosToml` names neither.
Storage, markers, journals, scratch and Mithril downloads stay beneath the
configured `storage.path`, `/data/db` in the deployed values. Direct PID-1 execution delivers SIGTERM to
the publisher.
The pod requires a non-root image user and sets `fsGroup: 65532`, giving the
image's UID/GID `65532:65532` write access to the `/data` emptyDir.

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
genesis opt-in, TLS selection, missing or invalid configuration and
accidental use of the legacy Dolos image.

## Upgrade and rollback

A Job's pod template is immutable. Bump `run` to create a new Job for an
upgrade or re-run. Helm also removes the old Job, but that replacement order
does not guarantee a single writer: stop the previous writer and wait for its
pod to terminate before starting the next run.

Chart 0.3 cannot continue a repository published by chart 0.2. Its
publisher refuses to restore a stele cut before the Dolos v2 profile, and
chart 0.2 refuses one cut by 0.3, so a release moves to 0.3 by starting a
fresh lineage. Stop the writer and delete every tag in the repository,
`latest` first. Then set a publisher image built on the v2 configuration,
rewrite `dolosToml` in the v2 schema, move its `[snapshot] state_epochs`
list unchanged into `stateEpochs`, set `allowGenesisFallback: true` and bump
`run`. The Job replays from genesis. Once `latest` is near the chain tip,
set `allowGenesisFallback: false` and bump `run` again. Never deploy 0.3
before the repository is empty. The Job refuses to restore the old `latest`
and, with genesis fallback on, starts from genesis instead. It then replays
silently: every boundary up to the old `latest` reports nothing to publish,
which lasts days on mainnet. Only at the next boundary does it refuse the v1
predecessor, and from there it restarts until `backoffLimit`. It uploads
nothing, so the old lineage stays intact. The Secret and tuning values stay
as they are.

Rollback to chart 0.2 works only while the repository still holds its v1
lineage, that is, before its tags are deleted. Stop the writer, select chart
0.2 with its previous image and v1 `dolosToml`, and bump `run` again. Once a
0.3 Job has published, going back to 0.2 needs a fresh v1 lineage in the same
way. Never reuse incompatible persistent scratch or an unpublished checkpoint
across hosts.

Full build, release and rollback details are in
[publisher-packaging.md](../../docs/publisher-packaging.md).
