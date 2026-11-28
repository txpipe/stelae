# Publisher image

The publisher ships as a Linux amd64/arm64 image at
`ghcr.io/txpipe/stelae-publisher`. It contains the publisher executable, a
digest-pinned distroless runtime with CA roots, and vendored Cardano genesis.
It runs the executable directly as PID 1 with UID/GID `65532:65532`. No Dolos
executable or shell is needed. The image owns `/data` as that user, so new
Docker volumes inherit writable storage. Host bind mounts and existing
volumes must grant that UID/GID write access.

Genesis files are copied into `/etc/genesis/<network>/`; their
[source](../.github/image/GENESIS.md) is recorded once. Under the Dolos v2
configuration a known network takes its embedded genesis from the Dolos
catalog and names no genesis path, so these files are read only by a
`custom` network configuration that points at them.

## Build and verification

On x86_64 Linux, build and stage the native amd64 executable:

```sh
cargo +1.93.0 build --locked --release --package stelae-publisher
mkdir -p .github/image/bin
cp target/release/stelae-publisher .github/image/bin/stelae-publisher-Linux-amd64
docker build --platform linux/amd64 --tag stelae-publisher:local .github/image
.github/image/smoke.sh stelae-publisher:local
k8s/dolos-publisher/test.sh
```

On arm64 Linux, replace `Linux-amd64` with `Linux-arm64` and
`--platform linux/amd64` with `--platform linux/arm64`. Both the staged name
and image platform must match the native binary.

The `Publisher image` workflow builds with Rust 1.93.0 and the committed
`Cargo.lock` on native amd64 and arm64 runners. Each runner builds its image
and checks executable startup, a v2 configuration, a mounted data volume and
SIGTERM delivery. The smoke check runs `run` with networking
disabled, opts into genesis against a missing local restore source, waits
for the acquisition loop and stops the process. It needs no chain fixture or
registry. Replay, restore policy and OCI correctness remain covered by the
existing host and registry suites.

Docker's setup, metadata, login and build actions handle image assembly and
publication. BuildKit provenance and a signed GitHub artifact attestation
identify the released image and source revision. Native binaries are only
intermediate workflow artifacts; there are no standalone binary releases or
custom provenance files.

## Candidate identity

A candidate is the tuple of the full Stelae commit, committed `Cargo.lock`,
Dolos revision `25548d6818953301ce42c79ab2a906277b008dbe` (fjall built from the
txpipe fork, see
[publisher.md](publisher.md#source-and-dependency-identity)),
chart source at that same Stelae
commit, and reviewed values/configuration. Record all of them at handoff. Before
publication the image digest is intentionally unknown; use the commit as the
candidate identity, then add the registry digest and attestation after the
explicit tag-driven publication. Never substitute a moving image tag for the
missing digest.

The publisher image and its Dolos profile/protocol dependencies do not need an
artificial release cadence. The immutable git pin and lockfile make the exact
combination reproducible, so a publisher candidate can be tested without a new
Dolos release, and a Dolos or Stelae protocol release need not publish a new
publisher image when its locked inputs did not change. This repository's one
workspace tag remains a source identity for the Stelae crates and application;
publication of protocol crates, image artifacts and deployment are still
distinct, explicit decisions.

## Release

1. Verify the candidate tuple above passed the workspace, registry,
   image and chart checks.
2. Prepare the version/changelog with the repository's `cargo release` flow
   and review the release commit and signed `v<version>` tag before pushing.
3. Pushing the version tag triggers image publication after both architecture
   checks and the chart check pass. The workflow verifies the tag matches the
   publisher's package version and publishes `v<version>` and `sha-<7>` tags,
   with no moving `latest` tag.
4. Record the published digest and verify its provenance with
   `gh attestation verify oci://ghcr.io/txpipe/stelae-publisher@sha256:<digest> --repo txpipe/stelae`.
   Operations selects the tested image for a separate deployment.

5. The same tag publishes the charts under `oci://ghcr.io/txpipe/charts/`:
   `dolos-publisher` and `stelae-registry`. Each is published at its own
   `Chart.yaml` `version:`, independent of the workspace version, and only
   once: a version the registry already holds is skipped, never replaced. A
   pull request that changes a chart since the newest release tag must bump
   that chart's version, or the chart check fails. The release repeats that
   check against the previous release tag before it pushes, so a change that
   reached the branch without a bump fails the release instead of going
   unpublished. The previous release is the highest `v*` tag below the one
   being released, even when it was tagged on a commit the branch does not
   reach, as a squash-merged release PR leaves it.
6. The first push of a chart creates a private package. The organization
   owner makes it public by hand in the package settings; later versions
   inherit that visibility.

Pull requests and branch-based manual runs only build and test. A manual run
on a version tag follows the same publication path as a tag push. The workflow
does not create GitHub Releases, tags, or deployments.

## First publication on the chart

An image built on the frozen profile publishes a fresh lineage, following
the order in [publisher.md](publisher.md#first-publication). The chart
directory and name remain `dolos-publisher` to preserve release and object
naming.

1. Stop every other writer to the network root and wait for its pod to
   terminate. Helm's resource replacement order does not guarantee a single
   writer.
2. Delete every tag in the root's `dolos` repository, `latest` first. The
   first run must find it empty. A lineage published before the diff chain
   sits at the root itself, not under `dolos`: leave it alone, since a
   rollback to its image restores from it. With `persistence.enabled: true`,
   also delete any claim an earlier attempt left, because initialization
   resumes kept stores. A deleted `persistence.existingClaim` must be created
   again, empty, before step 3: until it exists the pod stays Pending (see the
   chart README's
   [Data volume](../k8s/dolos-publisher/README.md#data-volume)). Delete only a
   claim that holds nothing but the publisher's stores: point
   `persistence.existingClaim` at a new, empty claim instead of deleting one
   shared with other data. Check its volume's reclaim policy first, too: under
   `Delete` the volume goes with the claim, and under `Retain` the volume and
   its data stay until someone removes them.
3. Install or upgrade with the candidate's image, the network's v2
   `dolosToml` with full history (no `[history]` window), `allowGenesisFallback: true`
   and a bumped `run`. From an emptyDir or a fresh
   claim, the Job replays from genesis and publishes both snapshots of every
   epoch boundary on the way: the blocks snapshot into the root's `blocks`
   repository, then the Dolos snapshot naming it into `dolos`. The `blocks` repository
   may keep what it holds, since each blocks snapshot found there is
   reproduced and kept, and one that differs refuses.
4. Set `allowGenesisFallback: false` for every later run. Initialization
   then restores the published `latest`, or resumes the kept stores on a
   persistent claim, and fails closed if it cannot.

The first publication runs as the one-shot Job. Once it has finished, a
release on a persistent claim can move to the scheduled form: setting
`schedule` replaces the Job with a CronJob whose runs resume the kept claim
and publish each new boundary as it appears, one run at a time (see the chart
README's [Scheduled runs](../k8s/dolos-publisher/README.md#scheduled-runs)).

The `/data` volume is an emptyDir by default, and every run restores the
published head, all of history. A mainnet publisher needs the persistent
claim. With a persistent claim the next run resumes the kept stores
instead, publishing first any boundary they committed but did not publish.
So moving away from a suspect image deletes the claim first, as does any
change of storage pins, journal schemas or profile versions (see the chart
README's [Data volume](../k8s/dolos-publisher/README.md#data-volume)).

See the [chart documentation](../k8s/dolos-publisher/README.md) for its values.
