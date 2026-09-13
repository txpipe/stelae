# Publisher image

The publisher ships as a Linux amd64/arm64 image at
`ghcr.io/txpipe/stelae-publisher`. It contains the publisher executable, a
digest-pinned distroless runtime with CA roots, and vendored Cardano genesis.
It runs the executable directly as PID 1 with UID/GID `65532:65532`. No Dolos
executable or shell is needed. The image owns `/data` as that user, so new
Docker volumes inherit writable storage. Host bind mounts and existing
volumes must grant that UID/GID write access.

Genesis files are copied into `/etc/genesis/<network>/`; their
[source](../.github/image/GENESIS.md) is recorded once. They are independent
of subsequent Dolos code revisions. Explicit genesis paths and
`force_protocol` in existing configuration keep their meaning.

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
and checks executable startup, bundled genesis/configuration, a mounted data
volume and SIGTERM delivery. The smoke check runs `run` with networking
disabled, opts into genesis against a missing local restore source, waits
for the acquisition loop and stops the process. It needs no chain fixture or
registry. Replay, restore policy and OCI correctness remain covered by the
existing host, publisher-parity and registry suites.

Docker's setup, metadata, login and build actions handle image assembly and
publication. BuildKit provenance and a signed GitHub artifact attestation
identify the released image and source revision. Native binaries are only
intermediate workflow artifacts; there are no standalone binary releases or
custom provenance files.

## Candidate identity

A candidate is the tuple of the full Stelae commit, committed `Cargo.lock`,
accepted Dolos revision
`2cadf62a2cd9b15ed1c17a9816f976a8646ac2e5`, chart source at that same Stelae
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

1. Verify the candidate tuple above passed the workspace, parity, registry,
   image and chart checks.
2. Prepare the version/changelog with the repository's `cargo release` flow
   and review the release commit and signed `v<version>` tag before pushing.
3. Pushing the version tag triggers image publication after both architecture
   checks and the chart check pass. The workflow verifies the tag matches the
   publisher's package version and publishes `v<version>` and `sha-<7>` tags,
   with no moving `latest` tag.
4. Record the published digest and verify its provenance with
   `gh attestation verify oci://ghcr.io/txpipe/stelae-publisher@sha256:<digest> --repo txpipe/stelae`.
   Operations selects the tested image for a separate cutover.

Pull requests and branch-based manual runs only build and test. A manual run
on a version tag follows the same publication path as a tag push. The workflow
does not create GitHub Releases, tags, or deployments.

## Chart cutover and rollback

Chart 0.2 runs Stelae only. Existing Dolos deployments stay pinned to chart
0.1 and their tested image until the operator performs an explicit cutover.
The chart directory/name remains `dolos-publisher` to preserve release and
object naming; the legacy shell entrypoint and host selector are removed.

Keep the previous chart source revision and tested Dolos image available
([0.1 chart at the predecessor commit](https://github.com/txpipe/stelae/tree/bd4b8c31398986068234ffae323aa5cf820f612c/k8s/dolos-publisher/chart)).
Stop the old writer and wait for its pod to terminate before upgrading to
chart 0.2 with the Stelae image and a bumped `run`. Retain the network config,
repository, credentials and tuning; leave `allowGenesisFallback: false`
for an established lineage.

Rollback uses the pinned 0.1 chart and previous Dolos image with another
bumped `run`, after the Stelae writer has stopped. Do not rely on Helm's
resource replacement order to prevent overlapping writers. The chart uses
an emptyDir and restores a compatible published head. If storage becomes
persistent, discard incompatible scratch/checkpoints and restore fresh when
storage pins, journal schemas or profile versions differ. An unpublished
checkpoint is not a rollback source.

See the [chart documentation](../k8s/dolos-publisher/README.md) for its values.
