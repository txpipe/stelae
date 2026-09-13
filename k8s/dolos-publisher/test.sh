#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
CHART="$ROOT/k8s/dolos-publisher/chart"
VALUES="$ROOT/k8s/dolos-publisher/tests/values.yaml"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT INT TERM

helm lint "$CHART" --values "$VALUES"

helm template publisher "$CHART" --values "$VALUES" >"$OUT/dolos.yaml"
grep -F 'image: ghcr.io/txpipe/dolos:sha-deadbee' "$OUT/dolos.yaml"
grep -F 'command: ["/bin/sh", "/etc/publisher/entrypoint.sh"]' "$OUT/dolos.yaml"
grep -F 'exec dolos --config "$CONFIG" snapshot backfill' "$OUT/dolos.yaml"
if grep -Fq '/usr/local/bin/stelae-publisher' "$OUT/dolos.yaml"; then
    echo "legacy rendering unexpectedly selected the Stelae host" >&2
    exit 1
fi

helm template publisher "$CHART" --values "$VALUES" \
    --set host=stelae \
    --set image.repository=ghcr.io/txpipe/stelae-publisher >"$OUT/stelae.yaml"
grep -F 'image: ghcr.io/txpipe/stelae-publisher:sha-deadbee' "$OUT/stelae.yaml"
grep -F 'command: ["/usr/local/bin/stelae-publisher"]' "$OUT/stelae.yaml"
grep -F -- '- /etc/publisher/dolos.toml' "$OUT/stelae.yaml"
if grep -Fq -- '- --allow-genesis' "$OUT/stelae.yaml"; then
    echo "Stelae rendering enabled genesis fallback by default" >&2
    exit 1
fi

helm template publisher "$CHART" --values "$VALUES" \
    --set host=stelae \
    --set image.repository=ghcr.io/txpipe/stelae-publisher \
    --set allowGenesisFallback=true >"$OUT/stelae-genesis.yaml"
grep -F -- '- --allow-genesis' "$OUT/stelae-genesis.yaml"

if helm template publisher "$CHART" --values "$VALUES" \
    --set host=unknown >"$OUT/invalid-host.yaml" 2>&1; then
    echo "unknown host passed chart validation" >&2
    exit 1
fi
if helm template publisher "$CHART" --values "$VALUES" \
    --set-string repo= >"$OUT/missing-repo.yaml" 2>&1; then
    echo "missing publisher repository passed chart validation" >&2
    exit 1
fi
if helm template publisher "$CHART" --values "$VALUES" \
    --set-string dolosToml= >"$OUT/missing-config.yaml" 2>&1; then
    echo "empty publisher configuration passed chart validation" >&2
    exit 1
fi
