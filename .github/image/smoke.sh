#!/bin/sh
set -eu

IMAGE=${1:?usage: smoke.sh IMAGE}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CONTAINER="stelae-image-smoke-$$"
cleanup() {
    docker logs "$CONTAINER" 2>&1 || true
    docker rm -fv "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 1' INT TERM

docker run --rm --network none "$IMAGE" --version | grep -F 'stelae-publisher'
test "$(docker image inspect --format '{{.Config.User}}' "$IMAGE")" = 65532:65532

# Exercise the chart's run command with packaged genesis and a data volume.
# With no network or blocks, it initializes and waits for Mithril until stopped.
docker create --name "$CONTAINER" --network none \
    --volume /data \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml run \
    --restore-source file:///missing --allow-genesis \
    --repo oci://127.0.0.1:1/cardano/smoke --insecure --concurrency 1 >/dev/null
docker start "$CONTAINER" >/dev/null

# Wait for the acquisition loop, not a fixed startup delay.
ready=false
for attempt in $(seq 1 60); do
    if docker logs "$CONTAINER" 2>&1 | grep -Fq 'listing mithril snapshots'; then
        ready=true
        break
    fi
    test "$(docker inspect --format '{{.State.Running}}' "$CONTAINER")" = true
    sleep 1
done
test "$ready" = true
docker cp "$CONTAINER:/data/db/.stelae-publisher-initialized.json" - |
    tar -xO | grep -F '"mode": "genesis"'

docker stop --time 20 "$CONTAINER" >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$CONTAINER")" = 1
docker logs "$CONTAINER" 2>&1 | grep -F 'shutdown requested'
