#!/bin/bash
# The schedules behind this report, in the order they ran, with a later session's
# faults beside the others. Every row in observations.jsonl comes from a line here, but
# for the first trial, whose rows in observations-early.jsonl carry their flags. Run in
# sections: each takes hours.
#
# Set ROOT, on the volume being measured, and OLD_DOLOS, a Dolos binary from before
# batches. Under ROOT:
#   bin/                        tools/batched-export/
#   stelae-publisher-candidate  built from the candidate, see provenance.json
#   stelae-publisher-old        built from the last release before batches
#   loopback, loopback-discard  the example, built from the candidate
#   loopback-recount            the same, as of the registry counting refused bodies
#   corpus/preprod/immutable    preprod's certified immutable files
#   state-epochs                the retained list in provenance.json, one to a line
: "${ROOT:?set ROOT}" "${OLD_DOLOS:?set OLD_DOLOS}"

# ---- base ----
# Replay to epoch 256 and seed a repository with its stele, under base/preprod. As run
# these were the same two commands by hand: `loopback replay`, then `publish`.
python3 "$ROOT/bin/measure.py" prepare --root "$ROOT" --corpus "$ROOT/corpus/preprod/immutable" --state-epochs "$ROOT/state-epochs" --loopback "$ROOT/loopback" --publisher "$ROOT/stelae-publisher-candidate" --base-epoch 256 || exit 1

# ---- extend-base ----
# A candidate batch takes the base to 276. Its stores and repository, less the chain
# files and the spool, are what every run on base/preprod-276 clones.
BASE="$ROOT/base/preprod-276"
if [ -e "$BASE" ]; then
  echo "extend-base: $BASE already exists" >&2; exit 1
fi
# A run the tool couldn't record exits 1.
python3 "$ROOT/bin/measure.py" run --root "$ROOT/extend" --corpus "$ROOT/corpus/preprod/immutable" --state-epochs "$ROOT/state-epochs" --loopback "$ROOT/loopback" --workload base-256-to-276 --base "$ROOT/base/preprod" --until 276 --first-file 5041 --last-file 5450 --timeout 7000 --keep --arm "{\"name\":\"w20\",\"binary\":\"$ROOT/stelae-publisher-candidate\",\"flags\":[\"--batch-epochs\",\"20\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}" || exit 1
# A run that failed or timed out exits 0 with partial stores, which aren't a base. The
# row names the run, so the base comes from the run it describes.
LAST=$(tail -1 "$ROOT/extend/observations.jsonl")
if ! printf '%s' "$LAST" | grep -q '"succeeded": true'; then
  echo "extend-base: the last run didn't reach 276, so there is no base to move" >&2; exit 1
fi
EXTENDED="$ROOT/extend/runs/$(printf '%s' "$LAST" | python3 -I -c 'import json, sys; print(json.load(sys.stdin)["run"])')"
# Cloned beside it and renamed into place, so the base is whole or absent, and the run
# it comes from stays as it was for another try.
STAGED="$BASE.partial"
CLONE=-c; [ "$(uname)" = Darwin ] || CLONE=--reflink=auto
rm -rf "$STAGED" && mkdir -p "$STAGED/data" || exit 1
cp "$CLONE" -R "$EXTENDED/registry" "$STAGED/registry" || exit 1
for part in archive cache state wal; do cp "$CLONE" -R "$EXTENDED/data/$part" "$STAGED/data/" || exit 1; done
mv "$STAGED" "$BASE" || exit 1

# ---- genesis-parity ----
# Real preprod from genesis to epoch 24: Byron boundary blocks, every era crossing
# on the way, retained dumps at 4, 5, 6, 7 and 12.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
flags() { echo "[\"--batch-epochs\",\"$1\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]"; }
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --workload genesis-to-24 --from-genesis --until 24 --first-file 0 --last-file 410 --timeout 3000)
python3 "$M" run "${common[@]}" --arm "{\"name\":\"old\",\"binary\":\"$O\"}"
python3 "$M" run "${common[@]}" --arm "{\"name\":\"w1\",\"binary\":\"$C\"}"
python3 "$M" run "${common[@]}" --arm "{\"name\":\"w4\",\"binary\":\"$C\",\"flags\":$(flags 4)}"
python3 "$M" run "${common[@]}" --keep --arm "{\"name\":\"w16\",\"binary\":\"$C\",\"flags\":$(flags 16)}"
# The Dolos binary from before batches restores each of the 24, and the publisher
# reproduces each from what it restored.
RUN=$(ls -t "$R/runs" | grep -- '-w16$' | head -1)
sequences=(); for n in $(seq 1 24); do sequences+=(--sequence "$n"); done
python3 "$M" restore --root "$R" --run "$RUN" "${sequences[@]}" --consumer "$OLD_DOLOS" --publisher "$C" --loopback "$R/loopback" --state-epochs "$R/state-epochs"

# ---- rounds ----
# Paired complete-pipeline runs: three rounds of three arms with each arm once in
# each position, then one run of each other window, all over the same 24 epochs
# (276 to 300, the last a retained dump) from clones of the same stores and stele.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
arm() { # name window
  if [ "$1" = old ]; then echo "{\"name\":\"old\",\"binary\":\"$O\"}";
  elif [ "$2" = 1 ]; then echo "{\"name\":\"$1\",\"binary\":\"$C\"}";
  else echo "{\"name\":\"$1\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"$2\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"; fi
}
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --workload epochs-276-to-300 --base base/preprod-276 --until 300 --first-file 5442 --last-file 5930 --timeout 7200)
run() { # round position name window
  python3 "$M" run "${common[@]}" --round "$1" --position "$2" --arm "$(arm "$3" "$4")"
}
run 1 1 old 0; run 1 2 w1 1;  run 1 3 w16 16
run 2 1 w1 1;  run 2 2 w16 16; run 2 3 old 0
run 3 1 w16 16; run 3 2 old 0; run 3 3 w1 1
run 4 1 w4 4;  run 4 2 w24 24

# ---- faults ----
# What a stop costs over six real epochs, and what an upgrade and a rollback do
# with a node ahead of the repository's latest.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
W1="{\"name\":\"w1\",\"binary\":\"$C\"}"
OLD="{\"name\":\"old\",\"binary\":\"$O\"}"
window() { echo "{\"name\":\"w$1\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"$1\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"; }
W6="$(window 6)"
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --workload faults-276-to-282 --base base/preprod-276 --until 282 --first-file 5442 --last-file 5570 --timeout 3600)
run() { python3 "$M" run "${common[@]}" "$@"; }
run --arm "$W6"
run --arm "$W1"
run --arm "$W6" --kill-on 'preparing boundary sequence=279'
run --arm "$W6" --kill-on 'traversal started traversal="exact"'
run --arm "$W6" --kill-on 'publishing pending boundary sequence=279'
run --arm "$W1" --kill-on 'publishing pending boundary sequence=279'
run --arm "$W6" --fail-every 40
# An upgrade: the old exporter stops mid-publish, and a batched candidate takes over.
run --arm "$OLD" --kill-on 'publishing pending boundary sequence=279' --rerun-arm "$W6"
# A rollback over a held repository: the node is ahead of latest when the old
# exporter arrives, which has to refuse. Then the candidate drains the batch.
run --arm "$W6" --kill-on 'batch frontiers published=Some\(276\) prepared=Some\(279\)' --rerun-arm "$OLD" --rerun-arm "$W6"
# The same with nothing published yet: nothing tells the old exporter what it skips.
python3 "$M" run --root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --workload rollback-over-an-empty-repository --from-genesis --until 6 --first-file 0 --last-file 110 --timeout 1800 --arm "$(window 4)" --kill-on 'batch frontiers published=None prepared=Some\(3\)' --rerun-arm "$OLD"

# ---- faults, second session ----
# The upgrade again with the old exporter part way through a publication, its publish
# record on disk, and refused uploads with a registry that counts refused bodies.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
OLD="{\"name\":\"old\",\"binary\":\"$O\"}"
W6="{\"name\":\"w6\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"6\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback-recount" --workload faults-276-to-282 --base base/preprod-276 --until 282 --first-file 5442 --last-file 5570 --timeout 3600)
python3 "$M" run "${common[@]}" --arm "$OLD" --kill-on 'layer 3000/4464:' --rerun-arm "$W6"
python3 "$M" run "${common[@]}" --arm "$W6" --fail-every 40

# ---- sensitivity ----
# The same arms with the registry's large blobs off the volume being measured, to
# tell the publisher's own time from the two competing for one disk. Then a sampled
# run of each, to see where a publication waits.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
window() { echo "{\"name\":\"w$1\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"$1\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"; }
OLD="{\"name\":\"old\",\"binary\":\"$O\"}"; W1="{\"name\":\"w1\",\"binary\":\"$C\"}"
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback-discard" --discard-above 2097152 --workload epochs-276-to-292-registry-off-the-disk --base base/preprod-276 --until 292 --first-file 5442 --last-file 5770 --timeout 3600)
python3 "$M" run "${common[@]}" --round 1 --position 1 --arm "$OLD"
python3 "$M" run "${common[@]}" --round 1 --position 2 --arm "$(window 16)"
python3 "$M" run "${common[@]}" --round 1 --position 3 --arm "$W1"
python3 "$M" run "${common[@]}" --round 1 --position 4 --arm "$(window 4)"

# ---- validate ----
# The limits the docs name, over eight real epochs, and a budget too small to hold
# a window, which has to close the batch early rather than fail.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"
limits() { echo "{\"name\":\"$1\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"$2\",\"--batch-scratch-mib\",\"$3\",\"--batch-reserve-mib\",\"$4\"]}"; }
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback-discard" --base base/preprod-276 --until 284 --first-file 5442 --last-file 5610 --timeout 3600)
python3 "$M" run "${common[@]}" --workload limits-as-documented --arm "$(limits w4-documented 4 2048 512)"
python3 "$M" run "${common[@]}" --workload limits-too-small-for-the-window --arm "$(limits w4-budget-1024 4 1024 256)"
python3 "$M" run "${common[@]}" --workload limits-that-hold-the-window --arm "$(limits w4-held 4 4096 512)"

# ---- profile ----
# Where a publication's time goes: 20 s samples of the process, early and late in a
# batch and in the old exporter. These runs are their own workload, because sampling
# costs them something. A first attempt at each of the first two watched the log
# differently and sampled the same way. `profile-summary.py` reads the shares from the
# samples into profiles.txt.
R="$ROOT"
M="$R/bin/measure.py"
C="$R/stelae-publisher-candidate"; O="$R/stelae-publisher-old"
P="$R/evidence/profiles"; mkdir -p "$P"
common=(--root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --workload profile --base base/preprod-276 --first-file 5442 --last-file 5650 --timeout 3600)
window() { echo "{\"name\":\"profile-w$1\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"$1\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"; }
sampled() { # arm-name sequence output, then the arguments of the run
  local name="$1" sequence="$2" output="$3"; shift 3
  # A directory from before this run is an earlier run's, and the sample it left isn't this one's.
  local before; before=$(ls -d "$R"/runs/*-"$name" 2>/dev/null || true)
  rm -f "$P/$output.txt"
  python3 "$M" run "${common[@]}" "$@" &
  local running=$! found=""
  while kill -0 "$running" 2>/dev/null; do
    found=$(ls -td "$R"/runs/*-"$name" 2>/dev/null | grep -vxF -- "$before" | head -1)
    if [ -n "$found" ] && [ -f "$found/publisher.log" ]; then break; fi
    found=""
    /bin/sleep 1
  done
  if [ -n "$found" ]; then
    "$R/bin/sample-run.sh" "$found" "$sequence" "$P/$output.txt" "$running" || echo "$name: not sampled" >&2
  else
    echo "$name: the run ended before its log appeared, so it wasn't sampled" >&2
  fi
  wait "$running"
}
sampled profile-w12 278 w12-early --until 288 --arm "$(window 12)"
sampled profile-w8 283 w8-late --until 284 --arm "$(window 8)"
sampled profile-old 279 old --until 280 --arm "{\"name\":\"profile-old\",\"binary\":\"$O\"}"
python3 "$R/bin/profile-summary.py" "$P/old.txt" "$P/w12-early.txt" "$P/w8-late.txt"

# ---- restore-real ----
# A batch over real full-history stores, restored at both ends and at a batch's edge
# by the Dolos binary from before batches, then reproduced from what it restored.
R="$ROOT"
M="$R/bin/measure.py"; C="$R/stelae-publisher-candidate"
python3 "$M" run --root "$R" --corpus "$R/corpus/preprod/immutable" --state-epochs "$R/state-epochs" --loopback "$R/loopback" --base base/preprod-276 --until 284 --first-file 5442 --last-file 5610 --timeout 3600 --keep --workload restored-by-the-old-consumer --arm "{\"name\":\"w4\",\"binary\":\"$C\",\"flags\":[\"--batch-epochs\",\"4\",\"--batch-scratch-mib\",\"400000\",\"--batch-reserve-mib\",\"1024\"]}"
RUN=$(ls -t "$R/runs" | grep -- '-w4$' | head -1)
# 280 and 281 were stopped by hand and never rerun, as the report says.
python3 "$M" restore --root "$R" --run "$RUN" --sequence 277 --sequence 280 --sequence 281 --sequence 284 --consumer "$OLD_DOLOS" --publisher "$C" --loopback "$R/loopback" --state-epochs "$R/state-epochs"

# ---- read them ----
R="$ROOT"
M="$R/bin/measure.py"
python3 "$M" compare --root "$R" --separate rollback-over-an-empty-repository
python3 "$M" summarize --root "$R" --pair old:w16 --pair w1:w16 --pair w1:w4
