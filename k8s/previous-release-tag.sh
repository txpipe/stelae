#!/bin/sh
# Print the highest v* tag ordered below <tag> by version, whether or not the
# branch reaches its commit: a release tagged off the branch still counts.
# Fail when <tag> is not a tag or no earlier release exists.
set -eu

if [ $# -ne 1 ]; then
    echo "usage: $0 <tag>" >&2
    exit 2
fi
TAG=$1

if ! git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    echo "$TAG is not a tag" >&2
    exit 1
fi
previous=$(git tag --list 'v*' --sort=-v:refname | awk -v tag="$TAG" 'seen { print; exit } $0 == tag { seen = 1 }')
if [ -z "$previous" ]; then
    echo "no release tag precedes $TAG" >&2
    exit 1
fi
echo "$previous"
