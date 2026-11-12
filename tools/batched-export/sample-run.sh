#!/bin/bash
# Sample one run's publisher with macOS `sample`, 8 s into publishing a sequence,
# for 20 s. Start it beside `measure.py run --keep` or any run still in `runs/`.
# TOOL_PID is that `measure.py run`, so the wait ends if the run does. Without it the
# wait still ends once the run's row is in its root's observations.jsonl, which the
# tool writes as the run ends, or once a run without --keep removes its directory.
#
#   sample-run.sh RUN_DIR SEQUENCE OUTPUT [TOOL_PID]
RD="$1"; SEQ="$2"; OUT="$3"; RUNNING="${4:-}"
ROWS="$(/usr/bin/dirname "$(/usr/bin/dirname "$RD")")/observations.jsonl"
ROW="\"run\": \"$(/usr/bin/basename "$RD")\""
until /usr/bin/grep -q "publishing pending boundary sequence=$SEQ " "$RD/publisher.log" 2>/dev/null; do
  if [ ! -d "$RD" ] || { [ -n "$RUNNING" ] && ! kill -0 "$RUNNING" 2>/dev/null; } ||
     /usr/bin/grep -qF -- "$ROW" "$ROWS" 2>/dev/null; then
    echo "$RD: the run ended before publishing $SEQ, so nothing was sampled" >&2
    exit 1
  fi
  /bin/sleep 1
done
/bin/sleep 8
# pgrep reads a regex, and a run directory can hold anything.
PATTERN=$(printf '%s' "$RD/dolos.toml" | /usr/bin/sed 's/[][\\.*^$()+?{}|]/\\&/g')
PID=$(/usr/bin/pgrep -f "$PATTERN" | /usr/bin/head -1)
if [ -z "$PID" ]; then
  echo "$RD: no publisher running to sample at $SEQ" >&2
  exit 1
fi
/usr/bin/sample "$PID" 20 -file "$OUT" >/dev/null 2>&1
