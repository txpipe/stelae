# zot v2.1.20 + upstream fe0679da ("perf: drop the index walk on blob
# Content-Type", #4350), which is merged upstream but not yet released.
#
# Why this image exists at all: v2.1.17..v2.1.20 resolve a blob HEAD's
# Content-Type by reading the repo's manifests from S3 one by one until a
# descriptor matches. On cardano/mainnet (~300+ epoch tags, one manifest
# each) that is 20-150 s per HEAD, growing with every published epoch —
# kb/zot-instance-disconnects/. No released version both has
# redirectBlobURL (v2.1.17+) and lacks the walk, so this carries exactly
# one vendored upstream patch until v2.1.21 ships; delete the whole
# directory then and re-pin the official image.
#
# Build & push (founder, Apple Silicon builds arm64 natively):
#   cd k8s/registry/zot-build
#   docker build --platform linux/arm64 -t ghcr.io/txpipe/zot-linux-arm64:v2.1.20-pr4350 .
#   docker push ghcr.io/txpipe/zot-linux-arm64:v2.1.20-pr4350
#   docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/txpipe/zot-linux-arm64:v2.1.20-pr4350
# then pin the printed digest in the operator's registry values (`image.digest`).

FROM --platform=linux/arm64 golang:1.26-trixie AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 --branch v2.1.20 https://github.com/project-zot/zot.git .
COPY 4350-drop-index-walk.patch /tmp/4350.patch
# Only the handler change; the patch's test and swagger hunks are not needed
# in a binary build.
RUN git apply --include=pkg/api/routes.go /tmp/4350.patch \
    && grep -q "resolveBlobResponseMediaType" pkg/api/routes.go
# Mirrors the Makefile's `binary` target (no BUILD_LABELS), minus the
# build-metadata indirection; the ldflags only stamp version strings.
RUN env CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=linux GOARCH=arm64 \
    go build -o bin/zot -trimpath \
    -ldflags "-X zotregistry.dev/zot/v2/pkg/buildinfo.ReleaseTag=v2.1.20-pr4350 \
              -X zotregistry.dev/zot/v2/pkg/buildinfo.Commit=v2.1.20+fe0679da \
              -X zotregistry.dev/zot/v2/pkg/buildinfo.BinaryType=dist \
              -X zotregistry.dev/zot/v2/pkg/buildinfo.GoVersion=go1.26 \
              -s -w" ./cmd/zot

FROM --platform=linux/arm64 debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/bin/zot /usr/local/bin/zot
EXPOSE 5000
ENTRYPOINT ["/usr/local/bin/zot"]
CMD ["serve", "/etc/zot/config.json"]
