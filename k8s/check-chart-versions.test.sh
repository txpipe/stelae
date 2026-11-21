#!/bin/sh
# check-chart-versions.sh in a throwaway repository: a changed chart with an
# unchanged version fails, a bumped version passes, an untouched chart passes,
# and at release time previous-release-tag.sh finds the tag to check against,
# even when that release was tagged off the branch.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CHECK=$HERE/check-chart-versions.sh
PREVIOUS=$HERE/previous-release-tag.sh
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

# At release time, a change that landed without a bump after the previous
# release fails against the tag the release job resolves.
git tag v0.1.1
echo 'kind: Secret' >k8s/registry/chart/templates/secret.yaml
git add k8s/registry/chart/templates/secret.yaml
git commit -q -m "add a template without a bump"
git tag v0.1.2
previous=$("$PREVIOUS" v0.1.2)
test "$previous" = v0.1.1
if "$PREVIOUS" v0.1.0 2>/dev/null; then
    echo "the first release resolved a previous release" >&2
    exit 1
fi
if "$CHECK" "$previous" 2>"$REPO/err"; then
    echo "an unbumped change since the previous release passed" >&2
    exit 1
fi
grep -F 'registry:' "$REPO/err"

# A release tagged on a side commit, then squashed onto the branch as an
# identical commit, is still the previous release of the next tag.
main=$(git symbolic-ref --short HEAD)
git checkout -q -b release
sed -i.bak 's/^version: 0.1.1$/version: 0.1.2/' k8s/dolos-publisher/chart/Chart.yaml
rm k8s/dolos-publisher/chart/Chart.yaml.bak
echo 'data: {}' >>k8s/dolos-publisher/chart/templates/configmap.yaml
git commit -q -am "release: v0.1.3"
git tag v0.1.3
git checkout -q "$main"
git checkout v0.1.3 -- k8s
git commit -q -m "release: v0.1.3 (squash)"
test -z "$(git diff v0.1.3 HEAD)"
if git merge-base --is-ancestor v0.1.3 HEAD; then
    echo "the side release is reachable from the branch" >&2
    exit 1
fi
echo 'data: {}' >>k8s/dolos-publisher/chart/templates/configmap.yaml
git commit -q -am "change the chart without a bump"
git tag v0.1.4
# Ancestry alone would resolve v0.1.2, where the version differs, and pass.
test "$(git describe --tags --abbrev=0 --match 'v*' HEAD^)" = v0.1.2
previous=$("$PREVIOUS" v0.1.4)
test "$previous" = v0.1.3
if "$CHECK" "$previous" 2>"$REPO/err"; then
    echo "an unbumped change since a release tagged off the branch passed" >&2
    exit 1
fi
grep -F 'dolos-publisher:' "$REPO/err"

echo "check-chart-versions: ok"
