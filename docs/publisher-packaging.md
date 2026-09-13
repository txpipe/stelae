# Publisher packaging and release

The publisher is delivered as two Linux binaries and one multi-architecture
OCI image. The image contains `stelae-publisher`, its runtime libraries, CA
roots and the public Cardano genesis files; it deliberately contains no Dolos
executable or shell. The Helm chart keeps `host: dolos` as its default, so
building or releasing this image does not switch an installation.

## Reproducible inputs and candidate identity

The source revision and committed `Cargo.lock` identify the Rust build. CI uses
Rust 1.93.0 and names candidate binaries `stelae-publisher-Linux-amd64` and
`stelae-publisher-Linux-arm64`. Each candidate has a SHA-256 file and a JSON
provenance record containing the source ref/revision, lockfile digest, target,
toolchain and runner-image version. Release-tag builds also receive signed
GitHub artifact attestations.

The image definition pins all external content:

- Dockerfile frontend `docker/dockerfile:1.7.0` at
  `sha256:dbbd5e059e8a07ff7ea6233b213b36aa516b4c53c645f1817a4dd18b83cbea56`;
- runtime `gcr.io/distroless/cc-debian12` at
  `sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa`;
- release builder `moby/buildkit:v0.30.0` at
  `sha256:0168606be2315b7c807a03b3d8aa79beefdb31c98740cebdffdfeebf31190c9f`;
- genesis files from Dolos
  `1ae4e91c18a9e1456a3612af402d7b9b97546d30`, the same dependency revision
  used by `stelae-cardano`, with an independent checksum on every file;
- smoke registry `registry:2` at
  `sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373`.

The OCI config records the source URL, full revision, package version and
source commit time. A release publishes only identity-bearing `v<version>` and
`sha-<7>` tags; it does not move `latest`, `stable`, or a production values
file. BuildKit emits maximal provenance for the multi-architecture manifest,
and the workflow attaches a signed GitHub attestation to its digest.

On Linux, build the locked binary and stage it under the architecture name the
image expects:

```sh
cargo build --locked --release --package stelae-publisher
mkdir -p .github/image/bin
# Use Linux-amd64 instead on x86_64 Linux.
cp target/release/stelae-publisher \
  .github/image/bin/stelae-publisher-Linux-arm64
docker build --platform linux/arm64 \
  --build-arg BUILD_CREATED="$(git show --no-patch --format=%cI HEAD)" \
  --build-arg BUILD_REVISION="$(git rev-parse HEAD)" \
  --build-arg BUILD_VERSION="0.2.0" \
  --tag stelae-publisher:local .github/image
```

`Publisher artifacts` repeats this on native amd64 and arm64 runners for pull
requests and manual dispatches. Its image smoke gate exports the pinned Preview
epoch-one stele, starts the Linux image, verifies the filesystem has no Dolos
binary, restores the fixture, runs the publish-before-advance backfill path,
and observes `latest` in an isolated local registry. Pull-request and manual
candidate runs do not target a remote registry.

## Founder release checklist

Only a founder/ops release action publishes. Before pushing a release tag:

1. Confirm the packaging PR and its dependency PRs are merged, the working tree
   is clean, and CI passed the workspace, parity, registry, chart and image
   gates at the exact candidate commit.
2. Run the repository's configured `cargo release` preparation, inspect the
   version changes and generated changelog, then push the release commit.
3. Create and push its signed `v<version>` tag only after checking that the tag
   points at that release commit. The tag is the publication trigger; the
   workflow never creates a tag.
4. Confirm both release binaries, `SHA256SUMS`, provenance JSON files, the
   `v<version>` and `sha-<7>` image tags, and their attestations agree on the
   same full source revision. Verify locally with `sha256sum -c SHA256SUMS` and
   `gh attestation verify ... --repo txpipe/stelae`.
5. Do not edit any network values or deploy as part of the release. Operations
   selects a digest/tag and performs the separate single-writer cutover.

## Chart upgrade and rollback

Chart 0.2 retains the release name, object names, ConfigMap keys, `/data` and
`/etc/publisher` mounts, `dolos.toml`, registry credential environment names,
resource/placement controls, retry budget, grace period, repository,
concurrency and insecure-registry setting from chart 0.1. With the default
`host: dolos`, the old shell entrypoint and its restore-or-genesis behavior are
unchanged. `allowGenesisFallback` is ignored in that compatibility mode.

A Stelae cutover is explicit and changes three values together:

```yaml
host: stelae
image:
  repository: ghcr.io/txpipe/stelae-publisher
  tag: sha-<tested-commit>
allowGenesisFallback: false
```

Bump `run` for that cutover. Stelae mode runs the binary directly as PID 1, so
SIGINT/SIGTERM reach its cancellation watcher. `run` performs fail-closed
latest restore and writes its initialization marker beneath the configured
storage path. Set `allowGenesisFallback: true` only for a first publication
whose empty repository was checked immediately before the Job starts.

Never overlap the Dolos and Stelae Jobs against the same stores or repository
head. To roll back this chart, first prove the Stelae Job has stopped, restore
`host: dolos` and the previously tested Dolos image, bump `run`, and let the new
emptyDir restore the last compatible published stele. Do not reuse a scratch
directory, initialization marker or unpublished checkpoint across hosts when
their Dolos storage pin, journal schema or profile version differs; start with
fresh scratch/storage and restore a compatible publication instead.
