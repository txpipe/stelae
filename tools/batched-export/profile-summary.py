#!/usr/bin/env python3
"""What `sample-run.sh` caught: how much of a publication was saving its record.

Figures and function names only, because a raw sample names the host's paths. A
share is of samples of the thread driving the publication, so a save running on
any thread counts as one under way then.

    profile-summary.py SAMPLE...
"""

import collections
import re
import sys
from pathlib import Path

FRAME = re.compile(
    r"^[ +!:|]*(\d+) ((?:stelae_driver|dolos_snapshot|stelae|dolos_[a-z]+)::[A-Za-z0-9_:]+?)"
    r"(?:::h[0-9a-f]{16})?\s",
    re.M,
)
DRIVER = "stelae_driver::retry::bounded"
SAVE = "stelae_driver::publish::PublishRecord::save"


def summarize(path):
    """One sample's shares, or a line saying it has none, so the next still gets read."""
    try:
        text = Path(path).read_text(errors="replace")
    except OSError as error:
        print(f"{Path(path).stem}: no sample, {error.strerror}")
        return
    frames = collections.Counter()
    for count, name in FRAME.findall(text):
        frames[name] += int(count)
    samples = frames[DRIVER]
    if not samples:
        # Sampled before or after publishing, so there is nothing to take a share of.
        print(f"{Path(path).stem}: 0 samples of a publication, so no shares")
        return
    top = text.split("Sort by top of stack")[-1].split("Binary Images")[0]
    # F_FULLFSYNC, where a sync of the record spends its time on macOS.
    sync = sum(int(n) for n in re.findall(r"^\s*__fcntl\s+\(in [^)]*\)\s+(\d+)", top, re.M))
    print(f"{Path(path).stem}: {samples} samples, save {frames[SAVE] / samples:.0%}, "
          f"file sync {sync / samples:.0%}")
    for name, count in frames.most_common(8):
        print(f"  {count:>7} {name}")


for path in sys.argv[1:]:
    summarize(path)
