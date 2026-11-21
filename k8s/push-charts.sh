#!/bin/sh
# Push every chart package in <out-dir> to the chart registry, skipping any
# version the registry already holds: a published version is never replaced.
# STELAE_CHART_REGISTRY overrides the registry root and STELAE_CHART_PLAIN_HTTP=1
# talks to it over plain HTTP; both exist for tests.
set -eu

REGISTRY=${STELAE_CHART_REGISTRY:-oci://ghcr.io/txpipe/charts}

if [ $# -ne 1 ]; then
    echo "usage: $0 <out-dir>" >&2
    exit 2
fi
OUT=$1

set --
if [ "${STELAE_CHART_PLAIN_HTTP:-}" = 1 ]; then
    set -- --plain-http
fi

found=0
for package in "$OUT"/*.tgz; do
    test -f "$package" || continue
    found=1
    name=$(helm show chart "$package" | sed -n 's/^name: *//p')
    version=$(helm show chart "$package" | sed -n 's/^version: *//p')
    if error=$(helm show chart "$@" "$REGISTRY/$name" --version "$version" 2>&1 >/dev/null); then
        echo "$name $version: already published, skipped"
        continue
    fi
    # Only a missing version is pushed; any other lookup failure stops here,
    # since pushing then could replace a version the lookup failed to see.
    case $error in
    *"not found"* | *"NOT_FOUND"* | *"MANIFEST_UNKNOWN"* | *"NAME_UNKNOWN"*) ;;
    *)
        echo "$name $version: cannot tell whether it is published: $error" >&2
        exit 1
        ;;
    esac
    helm push "$@" "$package" "$REGISTRY"
    echo "$name $version: published"
done

if [ "$found" = 0 ]; then
    echo "no chart packages in $OUT" >&2
    exit 1
fi
