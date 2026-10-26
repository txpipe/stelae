#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
CHART="$ROOT/k8s/dolos-publisher/chart"
VALUES="$ROOT/k8s/dolos-publisher/tests/values.yaml"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# The rendered document of one kind.
doc() {
    awk -v kind="$1" '/^---/ { found = 0 } $0 == "kind: " kind { found = 1 } found' "$2"
}

# The lines nested under the first key of that name.
under() {
    awk -v key="$1" '
        /^ *$/ { next }
        inside { match($0, /^ */); if (RLENGTH <= depth) exit; print }
        !inside && $0 ~ "^ *" key ":$" { inside = 1; match($0, /^ */); depth = RLENGTH }
    '
}

# The list item that starts with "- name: <name>".
item() {
    awk -v name="$1" '
        /^ *$/ { next }
        inside { match($0, /^ */); if (RLENGTH <= depth) exit; print }
        !inside && $0 ~ "^ *- name: " name "$" { inside = 1; print; match($0, /^ */); depth = RLENGTH }
    '
}

# The Job mounts its volume named data read-write at /data, and that volume
# has the expected source line.
data_volume() {
    mount=$(doc Job "$1" | under volumeMounts | item data)
    if ! printf '%s\n' "$mount" | grep -Eq '^ +mountPath: /data$' ||
        printf '%s\n' "$mount" | grep -Fq readOnly; then
        echo "$1: /data is not the read-write mount of volume data" >&2
        exit 1
    fi
    doc Job "$1" | under volumes | item data | grep -Fx "$2"
    if doc Job "$1" | under volumes | item data | grep -Fq readOnly; then
        echo "$1: volume data is read-only at its source" >&2
        exit 1
    fi
}

helm lint "$CHART" --values "$VALUES"
helm template publisher "$CHART" --values "$VALUES" >"$OUT/default.yaml"
grep -F 'image: ghcr.io/txpipe/stelae-publisher:sha-deadbee' "$OUT/default.yaml"
grep -F 'command: ["/usr/local/bin/stelae-publisher"]' "$OUT/default.yaml"
grep -F -- '- /etc/publisher/dolos.toml' "$OUT/default.yaml"
grep -Fx '            - run' "$OUT/default.yaml"
grep -Fx '        runAsNonRoot: true' "$OUT/default.yaml"
grep -Fx '        fsGroup: 65532' "$OUT/default.yaml"
grep -F -- '- --state-epochs-file' "$OUT/default.yaml"
grep -F -- '- /etc/publisher/state-epochs' "$OUT/default.yaml"
grep -Fx '  state-epochs: |' "$OUT/default.yaml"
grep -Fx '    646' "$OUT/default.yaml"
grep -F 'name: DOLOS_STELAE_USER' "$OUT/default.yaml"
grep -F 'name: DOLOS_STELAE_PASSWORD' "$OUT/default.yaml"
if grep -Eq -- 'entrypoint.sh|--allow-genesis' "$OUT/default.yaml"; then
    echo "default chart enabled genesis fallback or a shell entrypoint" >&2
    exit 1
fi

# /data: an emptyDir by default, a kept claim when persistence is on.
grep -Fx '          emptyDir: {}' "$OUT/default.yaml"
if grep -Eq 'PersistentVolumeClaim|persistentVolumeClaim|fsGroupChangePolicy' "$OUT/default.yaml"; then
    echo "default chart rendered a persistent /data" >&2
    exit 1
fi
data_volume "$OUT/default.yaml" '          emptyDir: {}'

helm lint "$CHART" --values "$VALUES" --set persistence.enabled=true
helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.enabled=true >"$OUT/claim.yaml"
grep -Fx 'kind: PersistentVolumeClaim' "$OUT/claim.yaml"
grep -Fx '    helm.sh/resource-policy: keep' "$OUT/claim.yaml"
grep -Fx '    - ReadWriteOnce' "$OUT/claim.yaml"
grep -Fx '  storageClassName: gp3' "$OUT/claim.yaml"
grep -Fx '      storage: 250Gi' "$OUT/claim.yaml"
grep -Fx '          persistentVolumeClaim:' "$OUT/claim.yaml"
grep -Fx '            claimName: publisher-data' "$OUT/claim.yaml"
grep -Fx '        fsGroupChangePolicy: OnRootMismatch' "$OUT/claim.yaml"
if grep -Fq 'emptyDir' "$OUT/claim.yaml"; then
    echo "persistent chart still mounted an emptyDir" >&2
    exit 1
fi
pvc_name=$(awk '/^kind: PersistentVolumeClaim$/ { pvc = 1 } pvc && /^  name: / { print $2; exit }' "$OUT/claim.yaml")
claim_name=$(sed -n 's/^ *claimName: //p' "$OUT/claim.yaml")
if [ -z "$pvc_name" ] || [ "$pvc_name" != "$claim_name" ]; then
    echo "the Job mounts '$claim_name' but the chart created '$pvc_name'" >&2
    exit 1
fi
doc PersistentVolumeClaim "$OUT/claim.yaml" | under metadata | under annotations |
    grep -Fx '    helm.sh/resource-policy: keep'
doc PersistentVolumeClaim "$OUT/claim.yaml" | under spec | under resources | under requests |
    grep -Fx '      storage: 250Gi'
data_volume "$OUT/claim.yaml" '            claimName: publisher-data'

helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.enabled=true \
    --set persistence.storageClassName= >"$OUT/default-class.yaml"
grep -Fx 'kind: PersistentVolumeClaim' "$OUT/default-class.yaml"
if grep -Fq 'storageClassName' "$OUT/default-class.yaml"; then
    echo "an empty storage class was rendered instead of omitted" >&2
    exit 1
fi

helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.enabled=true \
    --set persistence.existingClaim=mainnet-history \
    --set persistence.size= >"$OUT/existing.yaml"
grep -Fx '            claimName: mainnet-history' "$OUT/existing.yaml"
grep -Fx '        fsGroupChangePolicy: OnRootMismatch' "$OUT/existing.yaml"
if grep -Fq 'kind: PersistentVolumeClaim' "$OUT/existing.yaml"; then
    echo "an existing claim was also created" >&2
    exit 1
fi
data_volume "$OUT/existing.yaml" '            claimName: mainnet-history'

helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.existingClaim=mainnet-history >"$OUT/existing-off.yaml"
data_volume "$OUT/existing-off.yaml" '          emptyDir: {}'
if grep -Eq 'PersistentVolumeClaim|persistentVolumeClaim|mainnet-history' "$OUT/existing-off.yaml"; then
    echo "an existing claim was mounted with persistence off" >&2
    exit 1
fi

# The install notes say whether the run resumes a kept claim, and which.
# helm template skips NOTES.txt and Helm 3 install --dry-run needs a
# cluster, so a copy of the chart renders it through tpl.
cp -R "$CHART" "$OUT/notes-chart"
cp "$CHART/templates/NOTES.txt" "$OUT/notes-chart/notes.txt"
printf '%s\n' 'kind: Notes' 'text: {{ tpl (.Files.Get "notes.txt") . | toJson }}' \
    >"$OUT/notes-chart/templates/notes.yaml"
notes() {
    helm template publisher "$OUT/notes-chart" --values "$VALUES" "$@" \
        --show-only templates/notes.yaml | sed -n 's/^text: //p'
}
notes >"$OUT/notes-default.txt"
grep -F 'Initialization restores latest and fails closed.' "$OUT/notes-default.txt"
if grep -Fq 'resumes the stores' "$OUT/notes-default.txt"; then
    echo "default notes promised a kept claim" >&2
    exit 1
fi
notes --set persistence.enabled=true >"$OUT/notes-claim.txt"
grep -F '\nThe run resumes the stores kept on claim publisher-data.\n' "$OUT/notes-claim.txt"
notes --set persistence.enabled=true \
    --set persistence.existingClaim=mainnet-history \
    --set persistence.size= >"$OUT/notes-existing.txt"
grep -F '\nThe run resumes the stores kept on claim mainnet-history.\n' "$OUT/notes-existing.txt"
for mode in claim existing; do
    if grep -Fq 'Initialization restores latest' "$OUT/notes-$mode.txt"; then
        echo "$mode notes promised a fresh restore" >&2
        exit 1
    fi
done

if helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.enabled=true \
    --set persistence.size= >"$OUT/invalid.yaml" 2>&1; then
    echo "a created claim without a size was accepted" >&2
    exit 1
fi

if helm template publisher "$CHART" --values "$VALUES" \
    --set persistence.enabled=true \
    --set persistence.existingclaim=mainnet-history >"$OUT/invalid.yaml" 2>&1; then
    echo "an unknown persistence key was accepted" >&2
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

# The retained epochs are signed input: absent, zero or repeated fails.
for invalid in stateEpochs=null 'stateEpochs={0}' 'stateEpochs={3,3}'; do
    if helm template publisher "$CHART" --values "$VALUES" \
        --set "$invalid" >"$OUT/invalid.yaml" 2>&1; then
        echo "invalid state epochs accepted: $invalid" >&2
        exit 1
    fi
done

for invalid in image.repository=ghcr.io/txpipe/dolos repo= dolosToml= run=retry-one run=0 run=-1; do
    if helm template publisher "$CHART" --values "$VALUES" \
        --set-string "$invalid" >"$OUT/invalid.yaml" 2>&1; then
        echo "invalid configuration accepted: $invalid" >&2
        exit 1
    fi
done
