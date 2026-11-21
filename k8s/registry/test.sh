#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
CHART="$ROOT/k8s/registry/chart"
VALUES="$ROOT/k8s/registry/tests/values.yaml"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# zot's config.json from a render, as one JSON document.
config() {
    helm template registry "$CHART" --values "$VALUES" "$@" \
        --show-only templates/configmap.yaml |
        awk '/^  config.json: \|$/ { inside = 1; next } inside { sub(/^    /, ""); print }'
}

# The admin policy is the same with or without a writer.
admin() {
    jq -e '.http.accessControl.adminPolicy ==
        { "users": ["publisher"], "actions": ["read", "create", "update", "delete"] }' "$1" >/dev/null || {
        echo "$1: the admin policy changed" >&2
        exit 1
    }
}

helm lint "$CHART" --values "$VALUES"

# No writer: the read-only policy alone on **.
config >"$OUT/default.json"
jq -e '.http.accessControl.repositories["**"].policies ==
    [{ "users": ["reader"], "actions": ["read"] }]' "$OUT/default.json" >/dev/null || {
    echo "without a writer, ** holds other than the read-only policy" >&2
    exit 1
}
admin "$OUT/default.json"

# A writer: read, create and update, never delete.
helm lint "$CHART" --values "$VALUES" --set auth.writerUsername=writer
config --set auth.writerUsername=writer >"$OUT/writer.json"
jq -e '.http.accessControl.repositories["**"].policies ==
    [{ "users": ["reader"], "actions": ["read"] },
     { "users": ["writer"], "actions": ["read", "create", "update"] }]' "$OUT/writer.json" >/dev/null || {
    echo "the writer's policy is not exactly read, create and update" >&2
    exit 1
}
admin "$OUT/writer.json"

# A writer named like another user is refused, naming both values.
for user in publisher reader; do
    if helm template registry "$CHART" --values "$VALUES" \
        --set auth.writerUsername="$user" >"$OUT/invalid.txt" 2>&1; then
        echo "a writer named like another user was accepted: $user" >&2
        exit 1
    fi
    grep -F "auth.writerUsername \"$user\" must differ" "$OUT/invalid.txt"
done
