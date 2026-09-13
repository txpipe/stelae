#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
CHART="$ROOT/k8s/dolos-publisher/chart"
VALUES="$ROOT/k8s/dolos-publisher/tests/values.yaml"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

helm lint "$CHART" --values "$VALUES"
helm template publisher "$CHART" --values "$VALUES" >"$OUT/default.yaml"
grep -F 'image: ghcr.io/txpipe/stelae-publisher:sha-deadbee' "$OUT/default.yaml"
grep -F 'command: ["/usr/local/bin/stelae-publisher"]' "$OUT/default.yaml"
grep -F -- '- /etc/publisher/dolos.toml' "$OUT/default.yaml"
grep -Fx '            - run' "$OUT/default.yaml"
grep -Fx '        runAsNonRoot: true' "$OUT/default.yaml"
grep -Fx '        fsGroup: 65532' "$OUT/default.yaml"
if grep -Eq -- 'entrypoint.sh|--allow-genesis' "$OUT/default.yaml"; then
    echo "default chart enabled genesis fallback or a shell entrypoint" >&2
    exit 1
fi

helm template publisher "$CHART" --values "$VALUES" \
    --set allowGenesisFallback=true >"$OUT/genesis.yaml"
grep -F -- '- --allow-genesis' "$OUT/genesis.yaml"

helm template publisher "$CHART" --values "$VALUES" \
    --set insecure=false >"$OUT/tls.yaml"
if grep -Fq -- '- --insecure' "$OUT/tls.yaml"; then
    echo "TLS configuration enabled insecure transport" >&2
    exit 1
fi

helm template publisher "$CHART" --values "$VALUES" \
    --set-string run=8 >"$OUT/counter.yaml"
grep -Fx '  name: preview-backfill-8' "$OUT/counter.yaml"

for invalid in image.repository=ghcr.io/txpipe/dolos repo= dolosToml= run=retry-one run=0 run=-1; do
    if helm template publisher "$CHART" --values "$VALUES" \
        --set-string "$invalid" >"$OUT/invalid.yaml" 2>&1; then
        echo "invalid configuration accepted: $invalid" >&2
        exit 1
    fi
done
