#!/bin/sh
# Fail for each chart whose files differ from <base-ref> while its Chart.yaml
# version does not. Run inside the repository; a chart absent at <base-ref>
# passes.
set -eu

if [ $# -ne 1 ]; then
    echo "usage: $0 <base-ref>" >&2
    exit 2
fi
BASE=$1
cd "$(git rev-parse --show-toplevel)"

failed=0
for chart in k8s/dolos-publisher/chart k8s/registry/chart; do
    if git diff --quiet "$BASE" -- "$chart"; then
        continue
    fi
    if ! base_yaml=$(git show "$BASE:$chart/Chart.yaml" 2>/dev/null); then
        continue
    fi
    base_version=$(printf '%s\n' "$base_yaml" | sed -n 's/^version: *//p')
    version=$(sed -n 's/^version: *//p' "$chart/Chart.yaml")
    name=$(sed -n 's/^name: *//p' "$chart/Chart.yaml")
    if [ "$version" = "$base_version" ]; then
        echo "$name: $chart changed since $BASE but its version is still $version" >&2
        failed=1
    fi
done
exit "$failed"
