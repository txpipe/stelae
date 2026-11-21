#!/bin/sh
# push-charts.sh against a local OCI registry: the first run publishes every
# chart, the second skips each one and exits 0.
set -eu

K8S=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# zot from GHCR: ECR Public throttles anonymous pulls from shared runner IPs.
IMAGE=${STELAE_TEST_REGISTRY_IMAGE:-ghcr.io/project-zot/zot:v2.1.20}
OUT=$(mktemp -d)
container=
cleanup() {
    if [ -n "$container" ]; then docker rm -f "$container" >/dev/null; fi
    rm -rf "$OUT"
}
trap cleanup EXIT

# A failed pull is infrastructure, not a finding: back off and try again.
for attempt in 1 2 3 4 5; do
    docker pull --quiet "$IMAGE" >/dev/null && break
    if [ "$attempt" = 5 ]; then
        echo "cannot pull $IMAGE" >&2
        exit 1
    fi
    sleep $((attempt * 15))
done
container=$(docker run -d -p 127.0.0.1::5000 "$IMAGE")
port=$(docker port "$container" 5000/tcp | sed -n '1s/.*://p')
for attempt in $(seq 30); do
    curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null 2>&1 && break
    if [ "$attempt" = 30 ]; then
        echo "the local registry did not start" >&2
        exit 1
    fi
    sleep 1
done

export STELAE_CHART_REGISTRY="oci://127.0.0.1:$port/charts"
export STELAE_CHART_PLAIN_HTTP=1

"$K8S/package-charts.sh" "$OUT/charts" >"$OUT/packaged"
count=$(grep -c '' "$OUT/packaged")

"$K8S/push-charts.sh" "$OUT/charts" >"$OUT/first"
cat "$OUT/first"
test "$(grep -c ': published$' "$OUT/first")" = "$count"

"$K8S/push-charts.sh" "$OUT/charts" >"$OUT/second"
cat "$OUT/second"
test "$(grep -c ': already published, skipped$' "$OUT/second")" = "$count"

while read -r name version; do
    helm show chart --plain-http "$STELAE_CHART_REGISTRY/$name" --version "$version" |
        grep -Fx "version: $version"
done <"$OUT/packaged"

echo "push-charts: ok"
