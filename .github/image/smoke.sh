#!/bin/sh
set -eu

IMAGE=${1:?usage: smoke.sh IMAGE}
FIXTURE=${STELAE_PACKAGING_FIXTURE:?STELAE_PACKAGING_FIXTURE is required}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUN_ID="stelae-image-smoke-$$"
NETWORK="$RUN_ID-network"
REGISTRY="$RUN_ID-registry"
INSPECT="$RUN_ID-inspect"
STOP="$RUN_ID-stop"
DATA=$(mktemp -d)
FAIL_DATA=$(mktemp -d)
GENESIS_DATA=$(mktemp -d)

cleanup() {
    docker rm -f "$REGISTRY" "$INSPECT" "$STOP" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -rf "$DATA" "$FAIL_DATA" "$GENESIS_DATA"
}
trap cleanup EXIT INT TERM

docker network create "$NETWORK" >/dev/null
docker run --detach --rm \
    --name "$REGISTRY" \
    --network "$NETWORK" \
    --publish 127.0.0.1::5000 \
    "${STELAE_TEST_REGISTRY_IMAGE:?STELAE_TEST_REGISTRY_IMAGE is required}" >/dev/null

docker run --rm "$IMAGE" --version | grep -F "stelae-publisher"

# Inspect the filesystem, not PATH: the runtime is intentionally shell-free.
docker create --name "$INSPECT" "$IMAGE" --version >/dev/null
if docker export "$INSPECT" | tar -tf - | grep -Eq '(^|/)(bin/)?dolos$'; then
    echo "publisher image unexpectedly contains a Dolos executable" >&2
    exit 1
fi
docker rm "$INSPECT" >/dev/null

# The packaged executable retains the host's cold-start policy: missing latest
# fails closed, and genesis remains an explicit opt-in.
if docker run --rm \
    --volume "$FAIL_DATA:/data" \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml \
    initialize --source file:///missing; then
    echo "missing restore source unexpectedly selected genesis" >&2
    exit 1
fi
test ! -e "$FAIL_DATA/db/.stelae-publisher-initialized.json"

docker run --rm \
    --volume "$GENESIS_DATA:/data" \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml \
    initialize --source file:///missing --allow-genesis
grep -F '"mode": "genesis"' "$GENESIS_DATA/db/.stelae-publisher-initialized.json"

docker run --rm \
    --network "$NETWORK" \
    --volume "$FIXTURE:/fixture:ro" \
    --volume "$DATA:/data" \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml \
    initialize --source file:///fixture

test -f "$DATA/db/.stelae-publisher-initialized.json"

docker run --rm \
    --network "$NETWORK" \
    --volume "$DATA:/data" \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml \
    backfill --repo "oci://$REGISTRY:5000/cardano/image-smoke" \
    --insecure --until-epoch 1

PORT=$(docker port "$REGISTRY" 5000/tcp | sed -n 's/.*://p')
curl --fail --silent --show-error \
    "http://127.0.0.1:$PORT/v2/cardano/image-smoke/tags/list" |
    grep -F '"latest"'

test -d "$DATA/db/scratch"

# After the pending boundary is published, the host waits on the deliberately
# unreachable Mithril endpoint. SIGTERM must stop it before Docker's kill code.
docker run --detach \
    --name "$STOP" \
    --network "$NETWORK" \
    --volume "$DATA:/data" \
    --volume "$SCRIPT_DIR/preview-smoke.toml:/etc/publisher/dolos.toml:ro" \
    "$IMAGE" --config /etc/publisher/dolos.toml \
    backfill --repo "oci://$REGISTRY:5000/cardano/image-smoke" \
    --insecure >/dev/null
sleep 2
docker stop --time 20 "$STOP" >/dev/null
EXIT_CODE=$(docker inspect --format '{{.State.ExitCode}}' "$STOP")
test "$EXIT_CODE" = 1
docker logs "$STOP" 2>&1 | grep -F "shutdown requested"
docker rm "$STOP" >/dev/null
