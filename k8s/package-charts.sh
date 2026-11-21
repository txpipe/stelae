#!/bin/sh
# Package every Stelae chart into <out-dir> at its own Chart.yaml version and
# print "<name> <version>" for each.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ $# -ne 1 ]; then
    echo "usage: $0 <out-dir>" >&2
    exit 2
fi
OUT=$1
mkdir -p "$OUT"

for chart in "$ROOT"/k8s/dolos-publisher/chart "$ROOT"/k8s/registry/chart; do
    helm package "$chart" --destination "$OUT" >/dev/null
    name=$(sed -n 's/^name: *//p' "$chart/Chart.yaml")
    version=$(sed -n 's/^version: *//p' "$chart/Chart.yaml")
    test -f "$OUT/$name-$version.tgz"
    echo "$name $version"
done
