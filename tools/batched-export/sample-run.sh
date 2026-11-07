#!/bin/bash
# Sample one run's publisher with macOS `sample`, 8 s into publishing a sequence,
# for 20 s. Start it beside `measure.py run --keep` or any run still in `runs/`.
#
#   sample-run.sh RUN_DIR SEQUENCE OUTPUT
RD="$1"; SEQ="$2"; OUT="$3"
until /usr/bin/grep -q "publishing pending boundary sequence=$SEQ " "$RD/publisher.log" 2>/dev/null; do /bin/sleep 1; done
/bin/sleep 8
PID=$(/usr/bin/pgrep -f "$RD/dolos.toml" | /usr/bin/head -1)
/usr/bin/sample "$PID" 20 -file "$OUT" >/dev/null 2>&1
