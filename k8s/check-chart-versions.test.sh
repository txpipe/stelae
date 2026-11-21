#!/bin/sh
# check-chart-versions.sh in a throwaway repository: a changed chart with an
# unchanged version fails, a bumped version passes, an untouched chart passes.
set -eu

CHECK=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/check-chart-versions.sh
REPO=$(mktemp -d)
trap 'rm -rf "$REPO"' EXIT
cd "$REPO"

git init -q
git config user.email test@example.invalid
git config user.name test
for chart in dolos-publisher registry; do
    mkdir -p "k8s/$chart/chart/templates"
    printf 'apiVersion: v2\nname: %s\nversion: 0.1.0\n' "$chart" >"k8s/$chart/chart/Chart.yaml"
    echo 'kind: ConfigMap' >"k8s/$chart/chart/templates/configmap.yaml"
done
git add k8s
git commit -q -m base
git tag v0.1.0

# Untouched charts pass.
"$CHECK" v0.1.0

# A changed template with the same version fails and names its chart.
echo 'data: {}' >>k8s/dolos-publisher/chart/templates/configmap.yaml
git commit -q -am "change a template"
if "$CHECK" v0.1.0 2>"$REPO/err"; then
    echo "a changed chart with an unchanged version passed" >&2
    exit 1
fi
grep -F 'dolos-publisher:' "$REPO/err"
if grep -F 'stelae-registry' "$REPO/err" || grep -F 'registry:' "$REPO/err"; then
    echo "the untouched chart was reported" >&2
    exit 1
fi

# A bumped version passes; the other chart stays untouched.
sed -i.bak 's/^version: 0.1.0$/version: 0.1.1/' k8s/dolos-publisher/chart/Chart.yaml
rm k8s/dolos-publisher/chart/Chart.yaml.bak
git commit -q -am "bump the chart"
"$CHECK" v0.1.0

echo "check-chart-versions: ok"
