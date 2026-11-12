#!/usr/bin/env python3
"""What `measure.py` leaves behind when a run goes wrong, and what it concludes.

Stand-ins replace the publisher, the registry and the consumer, so this takes a
minute or so and needs nothing built. Nothing in CI runs it:
`python3 tools/batched-export/test_measure.py`.
"""

import hashlib
import importlib.util
import json
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

TOOL = Path(__file__).with_name("measure.py")
SAMPLER = Path(__file__).with_name("sample-run.sh")

REGISTRY = """#!{python}
import http.server, os
open({pid!r}, "w").write(str(os.getpid()))
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write({accounting!r})
    def log_message(self, *_):
        pass
server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
"""

# Exits 0 having published `tags`, then does whatever `then` says.
PUBLISHER = """#!{python}
import hashlib, json, sys, time
from pathlib import Path
run = Path(sys.argv[sys.argv.index("--config") + 1]).parent
repository = run / "registry" / "cardano" / "measured"
tags, blobs = repository / "manifests" / "tags", repository / "blobs" / "sha256"
tags.mkdir(parents=True, exist_ok=True)
blobs.mkdir(parents=True, exist_ok=True)
for tag, inscription in {tags!r}.items():
    digest = hashlib.sha256(inscription.encode()).hexdigest()
    (blobs / digest).write_text(inscription)
    manifest = {{"config": {{"digest": "sha256:" + digest}}, "layers": []}}
    (tags / tag).write_text(json.dumps(manifest))
{then}
"""

CONSUMER = """#!{python}
import sys
from pathlib import Path
config = Path(sys.argv[sys.argv.index("-c") + 1])
sequence = sys.argv[sys.argv.index("--point") + 1].removeprefix("epoch-")
config.with_name("restored").write_text(sequence)
"""

# Digests what the consumer restored, as `<prefix><sequence>`, and exits 4 unless it is
# chained from the inscription of the sequence before, or from nothing for the first.
DIGEST = """#!{python}
import sys
from pathlib import Path
config = Path(sys.argv[sys.argv.index("--config") + 1])
output = Path(sys.argv[sys.argv.index("--output") + 1])
sequence = int(config.with_name("restored").read_text())
chained = sys.argv[sys.argv.index("--chain-from") + 1] if "--chain-from" in sys.argv else None
found = Path(chained).read_text() if chained else None
if found != (None if sequence == 1 else "inscription-" + str(sequence - 1)):
    sys.exit(4)
output.write_text({prefix!r} + str(sequence))
"""

# `replay` makes the storage path its config names, records the epoch asked for there
# and in `calls`, and exits 1 while `fails` exists. Anything else serves as the registry.
REPLAYING_REGISTRY = """#!{python}
import re, sys
from pathlib import Path
if sys.argv[1] == "replay":
    storage = re.search(r'path = "(.*)"', Path(sys.argv[2]).read_text()).group(1)
    Path(storage).mkdir(parents=True, exist_ok=True)
    Path(storage, "epoch").write_text(sys.argv[4])
    with open({calls!r}, "a") as calls:
        calls.write(sys.argv[4] + "\\n")
    sys.exit(1 if Path({fails!r}).exists() else 0)
import http.server
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass
server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
"""

# Tags the seed beside its config with the epoch its stores were replayed to.
SEEDING_PUBLISHER = """#!{python}
import re, sys
from pathlib import Path
config = Path(sys.argv[sys.argv.index("--config") + 1])
storage = re.search(r'path = "(.*)"', config.read_text()).group(1)
tags = config.parent / "registry" / "cardano" / "measured" / "manifests" / "tags"
tags.mkdir(parents=True, exist_ok=True)
(tags / ("epoch-" + Path(storage, "epoch").read_text())).write_text("{{}}")
"""

# Timestamped lines, as the publisher's log has them.
def logged(seconds, text):
    return f"2026-01-01T00:{seconds // 60:02}:{seconds % 60:02}.000Z INFO {text}"


def gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def default_stop_signals():
    """A runner under nohup or `&` passes its ignored signals on, and these need them."""
    for number in (signal.SIGHUP, signal.SIGINT):
        signal.signal(number, signal.SIG_DFL)


def inscribed(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Harness(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        for path in ["base/data", "base/registry", "corpus"]:
            (self.root / path).mkdir(parents=True)
        (self.root / "state-epochs").write_text("4\n")
        self.registry_pid = self.root / "registry.pid"
        self.registry = self.registry_answering({})

    def registry_answering(self, accounting):
        return self.script("registry", REGISTRY.format(
            python=sys.executable, pid=str(self.registry_pid),
            accounting=json.dumps(accounting).encode(),
        ))

    def script(self, name, body):
        path = self.root / name
        path.write_text(body)
        path.chmod(0o755)
        return path

    def publisher(self, name, tags=None, then=""):
        tags = {"epoch-1": "one"} if tags is None else tags
        return self.script(name, PUBLISHER.format(python=sys.executable, tags=tags, then=then))

    def command(self, binary, *extra):
        # Run ids have one-second resolution, so the arm's name keeps them apart.
        arm = json.dumps({"name": Path(binary).name, "binary": str(binary)})
        return [sys.executable, str(TOOL), "run", "--root", str(self.root),
                "--corpus", str(self.root / "corpus"),
                "--state-epochs", str(self.root / "state-epochs"),
                "--loopback", str(self.registry), "--workload", "test", "--base", "base",
                "--until", "1", "--first-file", "0", "--last-file", "0", "--arm", arm, *extra]

    def run_tool(self, binary, *extra, **popen):
        return subprocess.run(self.command(binary, *extra), capture_output=True, text=True, **popen)

    def read_with(self, *arguments):
        return subprocess.run([sys.executable, str(TOOL), *arguments, "--root", str(self.root)],
                              capture_output=True, text=True)

    def rows(self):
        text = (self.root / "observations.jsonl").read_text()
        return [json.loads(line) for line in text.splitlines()]

    def wait_for(self, path):
        began = time.monotonic()
        while not path.exists() or not path.read_text().strip():
            self.assertLess(time.monotonic() - began, 60, f"{path} never appeared")
            time.sleep(0.05)


class Runs(Harness):
    def test_a_publisher_that_fails_leaves_its_exit_and_its_log(self):
        done = self.run_tool(self.script("fails", "#!/bin/sh\necho said >&2\nexit 3\n"))
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertEqual(row["legs"][0]["exit"], 3)
        self.assertNotIn("error", row)
        evidence = self.root / "evidence" / row["run"] / "publisher.log"
        self.assertEqual(evidence.read_text(), "said\n")

    def test_a_run_that_publishes_what_it_was_asked_for_succeeds(self):
        done = self.run_tool(self.publisher("ok"))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertTrue(self.rows()[0]["succeeded"])

    def test_a_run_that_exits_0_short_of_until_does_not_succeed(self):
        self.run_tool(self.publisher("short"), "--until", "2")
        row = self.rows()[0]
        self.assertEqual(row["legs"][0]["exit"], 0)
        self.assertIn("epoch-1", row["publications"])
        self.assertFalse(row["succeeded"])

    def test_a_publisher_that_fails_after_publishing_until_does_not_succeed(self):
        self.run_tool(self.publisher("fails-late", then="sys.exit(1)"))
        row = self.rows()[0]
        self.assertEqual(row["legs"][0]["exit"], 1)
        self.assertIn("epoch-1", row["publications"])
        self.assertFalse(row["succeeded"])

    def test_a_binary_that_is_not_there_leaves_a_row_and_no_legs(self):
        done = self.run_tool(self.root / "absent")
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertEqual(row["legs"], [])
        self.assertIn("FileNotFoundError", row["error"])

    def test_a_registry_that_does_not_start_leaves_a_row(self):
        self.registry = self.script("registry", "#!/bin/sh\nexit 1\n")
        done = self.run_tool(self.publisher("ok"))
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertIn("ValueError", row["error"])

    def test_evidence_that_cannot_be_kept_costs_the_evidence_and_not_the_row(self):
        (self.root / "evidence").write_text("in the way")
        done = self.run_tool(self.publisher("ok"))
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertTrue(row["succeeded"])
        self.assertTrue((self.root / "runs" / row["run"]).is_dir(), "the only copy went")

    def test_the_host_is_recorded_as_figures_with_no_process_names(self):
        self.run_tool(self.publisher("ok"))
        row = self.rows()[0]
        self.assertTrue(row["busiest_at_start"])
        self.assertTrue(all(isinstance(cpu, float) for cpu in row["busiest_at_start"]))
        self.assertIn("load", row["host_at_start"])

    def test_a_finished_run_leaves_only_its_evidence(self):
        done = self.run_tool(self.publisher("ok"))
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertFalse((self.root / "runs" / row["run"]).exists(), "the clones stayed")
        self.assertTrue((self.root / "evidence" / row["run"] / "epoch-1.manifest.json").is_file())

    def test_keep_leaves_the_stores_and_the_repository(self):
        done = self.run_tool(self.publisher("ok"), "--keep")
        self.assertEqual(done.returncode, 0, done.stderr)
        kept = self.root / "runs" / self.rows()[0]["run"]
        self.assertTrue((kept / "data").is_dir(), "--keep lost the stores")
        self.assertTrue((kept / "registry").is_dir(), "--keep lost the repository")

    def test_a_run_never_takes_an_id_another_run_had(self):
        # Another run of this arm kept evidence under the id of this second, and of the
        # next few in case the clock ticks over.
        now = time.time()
        taken = [self.root / "evidence" / f"{time.strftime('%Y%m%dT%H%M%S', time.gmtime(now + s))}-same"
                 for s in range(5)]
        for evidence in taken:
            evidence.mkdir(parents=True)
        done = self.run_tool(self.publisher("same"))
        self.assertEqual(done.returncode, 0, done.stderr)
        run = self.rows()[0]["run"]
        self.assertTrue(run.endswith("-same-2"), run)
        self.assertTrue((self.root / "evidence" / run / "epoch-1.manifest.json").is_file())
        for evidence in taken:
            self.assertEqual(list(evidence.iterdir()), [], f"{evidence.name} was written to")

    def test_a_log_that_cannot_be_read_still_stops_the_registry(self):
        # A registry left running holds the tool's stderr, so without the bound the
        # wait for it never ends.
        done = self.run_tool(self.publisher("unreadable", then=(
            'log = run / "publisher.log"\nlog.unlink()\nlog.mkdir()\n'
        )), timeout=60)
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertIn("IsADirectoryError", row["error"])
        self.assertTrue(gone(int(self.registry_pid.read_text())), "the registry outlived the run")


class Rows(Harness):
    def test_a_row_that_cannot_be_appended_goes_to_stderr_and_keeps_the_run(self):
        (self.root / "observations.jsonl").mkdir()
        done = self.run_tool(self.script("fails", "#!/bin/sh\nexit 3\n"))
        self.assertEqual(done.returncode, 1)
        self.assertIn("observation not written", done.stderr)
        row = json.loads(done.stderr[done.stderr.index('{"run"'):].splitlines()[0])
        self.assertEqual(row["legs"][0]["exit"], 3)
        self.assertTrue((self.root / "runs" / row["run"]).is_dir(), "the only copy went")

    def test_a_row_cut_short_does_not_take_the_next_with_it(self):
        cut = subprocess.run(
            self.command(self.publisher("cut")), capture_output=True, text=True,
            preexec_fn=lambda: resource.setrlimit(
                resource.RLIMIT_FSIZE, (256, resource.getrlimit(resource.RLIMIT_FSIZE)[1])
            ),
        )
        self.assertEqual(cut.returncode, 1)
        self.assertIn("observation not written", cut.stderr)
        fragment = (self.root / "observations.jsonl").read_bytes()
        self.assertEqual(len(fragment), 256)
        self.assertFalse(fragment.endswith(b"\n"), "the limit didn't cut the row")

        done = self.run_tool(self.publisher("whole"))
        self.assertEqual(done.returncode, 0, done.stderr)
        lines = (self.root / "observations.jsonl").read_text().splitlines()
        self.assertEqual(lines[0].encode(), fragment)
        self.assertEqual(json.loads(lines[1])["arm"], "whole")

        for command in ["summarize", "compare"]:
            read = self.read_with(command)
            self.assertEqual(read.returncode, 0, read.stderr)
            self.assertIn("observations.jsonl:1: not a whole row", read.stderr)
            self.assertIn("whole", read.stdout)


class Stops(Harness):
    def stopped_by(self, number, said):
        pid_file = self.root / "publisher.pid"
        publisher = self.script("sleeps", f'#!/bin/sh\necho $$ > "{pid_file}"\nexec sleep 600\n')
        tool = subprocess.Popen(
            self.command(publisher), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            preexec_fn=default_stop_signals,
        )
        self.addCleanup(tool.kill)
        self.wait_for(pid_file)
        tool.send_signal(number)
        self.assertEqual(tool.wait(timeout=60), 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertIn(said, row["error"])
        self.assertEqual(row["legs"][0]["killed"], "tool stopped")
        self.assertTrue(gone(int(pid_file.read_text())), "the publisher outlived the tool")
        self.assertTrue(gone(int(self.registry_pid.read_text())), "the registry outlived the tool")

    def test_sigterm_records_the_run_and_takes_its_children_with_it(self):
        self.stopped_by(signal.SIGTERM, "SIGTERM")

    def test_a_hang_up_records_the_run_and_takes_its_children_with_it(self):
        self.stopped_by(signal.SIGHUP, "SIGHUP")

    def test_ctrl_c_records_the_run_and_takes_its_children_with_it(self):
        self.stopped_by(signal.SIGINT, "KeyboardInterrupt")

    def test_a_hang_up_nohup_ignores_leaves_the_run_going(self):
        up, go = self.root / "up", self.root / "go"
        publisher = self.publisher("waits", then=(
            f"Path({str(up)!r}).write_text('up')\n"
            f"while not Path({str(go)!r}).exists():\n"
            "    time.sleep(0.05)\n"
        ))
        tool = subprocess.Popen(
            self.command(publisher), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            preexec_fn=lambda: signal.signal(signal.SIGHUP, signal.SIG_IGN),
        )
        self.addCleanup(tool.kill)
        self.wait_for(up)
        tool.send_signal(signal.SIGHUP)
        time.sleep(0.5)
        self.assertIsNone(tool.poll(), "a hang-up stopped a run under nohup")
        go.write_text("go")
        self.assertEqual(tool.wait(timeout=60), 0)
        self.assertTrue(self.rows()[0]["succeeded"])


class Faults(Harness):
    def test_a_planned_kill_runs_the_arm_named_next_and_records_both_legs(self):
        marker = self.root / "second-ran"
        first = self.script("first", '#!/bin/sh\necho "reached the point" >&2\nexec sleep 600\n')
        second = self.publisher("second", then=f"Path({str(marker)!r}).write_text('ran')")
        rerun = {"name": "second", "binary": str(second), "flags": ["--batch-epochs", "4"]}
        # The timeout bounds a rerun of the wrong arm, which would sleep.
        done = self.run_tool(first, "--kill-on", "reached the point", "--timeout", "30",
                             "--rerun-arm", json.dumps(rerun))
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertEqual(row["legs"][0]["killed"], "planned")
        self.assertEqual(row["legs"][0]["exit"], -9)
        self.assertEqual(row["legs"][1]["arm"], "second")
        self.assertEqual(row["legs"][1]["exit"], 0)
        self.assertEqual(row["legs"][1]["binary_sha256"],
                         hashlib.sha256(second.read_bytes()).hexdigest())
        self.assertEqual(row["legs"][1]["flags"], ["--batch-epochs", "4"])
        self.assertTrue(marker.exists(), "the arm named never ran")
        self.assertTrue(row["kill_fired"])
        self.assertTrue(row["succeeded"])

    def test_a_kill_that_never_comes_is_no_fault_measured(self):
        done = self.run_tool(self.publisher("untouched"), "--kill-on", "never logged")
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertEqual(len(row["legs"]), 1)
        self.assertEqual(row["legs"][0]["exit"], 0)
        self.assertFalse(row["kill_fired"])
        self.assertFalse(row["succeeded"])
        done = self.read_with("compare")
        self.assertIn("0 tags", done.stdout)

    def test_a_span_a_kill_cut_short_ends_with_its_process(self):
        marker = self.root / "once"
        first = [logged(0, 'index staging traversal started traversal="exact"'),
                 logged(5, "reached the point")]
        second = [logged(60, 'index staging traversal started traversal="exact"'),
                  logged(62, "index staging finished")]
        publisher = self.publisher("killed-staging", then=(
            f"if not Path({str(marker)!r}).exists():\n"
            f"    Path({str(marker)!r}).write_text('once')\n"
            f"    print({chr(10).join(first)!r}, file=sys.stderr, flush=True)\n"
            "    time.sleep(600)\n"
            f"print({chr(10).join(second)!r}, file=sys.stderr, flush=True)\n"
        ))
        done = self.run_tool(publisher, "--kill-on", "reached the point")
        self.assertEqual(done.returncode, 0, done.stderr)
        spans = self.rows()[0]["phases"]
        self.assertEqual(spans["stage_seconds"], 2)
        self.assertEqual(spans["interrupted_seconds"], 5)
        self.assertEqual(spans["covered_seconds"], 7)
        self.assertEqual(spans["traversals"], {"exact": 2})

    def test_a_planned_kill_with_no_arm_named_runs_the_same_arm_again(self):
        marker = self.root / "once"
        publisher = self.publisher("again", then=(
            f"if not Path({str(marker)!r}).exists():\n"
            f"    Path({str(marker)!r}).write_text('once')\n"
            "    print('reached the point', file=sys.stderr, flush=True)\n"
            "    time.sleep(600)\n"
        ))
        done = self.run_tool(publisher, "--kill-on", "reached the point")
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertEqual([leg["arm"] for leg in row["legs"][1:]], ["again"])
        self.assertEqual(row["legs"][0]["exit"], -9)
        self.assertEqual(row["legs"][1]["exit"], 0)

    def test_a_rerun_leg_the_tool_stopped_keeps_its_arm(self):
        pid_file = self.root / "second.pid"
        first = self.script("first", '#!/bin/sh\necho "reached the point" >&2\nexec sleep 600\n')
        second = self.script("second", f'#!/bin/sh\necho $$ > "{pid_file}"\nexec sleep 600\n')
        rerun = {"name": "second", "binary": str(second), "flags": ["--batch-epochs", "4"]}
        tool = subprocess.Popen(
            self.command(first, "--kill-on", "reached the point", "--rerun-arm", json.dumps(rerun)),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=default_stop_signals,
        )
        self.addCleanup(tool.kill)
        self.wait_for(pid_file)
        tool.send_signal(signal.SIGTERM)
        self.assertEqual(tool.wait(timeout=60), 1)
        leg = self.rows()[0]["legs"][1]
        self.assertEqual(leg["killed"], "tool stopped")
        self.assertEqual(leg["arm"], "second")
        self.assertEqual(leg["binary_sha256"], hashlib.sha256(second.read_bytes()).hexdigest())
        self.assertEqual(leg["flags"], ["--batch-epochs", "4"])
        self.assertTrue(gone(int(pid_file.read_text())), "the rerun outlived the tool")

    def test_a_rerun_that_never_starts_names_no_leg(self):
        first = self.script("first", '#!/bin/sh\necho "reached the point" >&2\nexec sleep 600\n')
        second = self.root / "not-executable"
        second.write_text("#!/bin/sh\nexit 0\n")
        rerun = {"name": "second", "binary": str(second)}
        done = self.run_tool(first, "--kill-on", "reached the point",
                             "--rerun-arm", json.dumps(rerun))
        self.assertEqual(done.returncode, 1, done.stderr)
        row = self.rows()[0]
        self.assertIn("PermissionError", row["error"])
        self.assertEqual(len(row["legs"]), 1)
        self.assertNotIn("arm", row["legs"][0])
        self.assertEqual(row["legs"][0]["killed"], "planned")

    def test_a_kill_that_finds_its_process_gone_is_not_a_planned_kill(self):
        spec = importlib.util.spec_from_file_location("measure_under_test", TOOL)
        measure = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(measure)
        # The kill misses, as one sent after the process exited does.
        sent = []
        measure.kill = sent.append
        data, log = self.root / "data", self.root / "missed.log"
        data.mkdir()
        legs = []
        measure.run_once(["/bin/sh", "-c", 'echo "reached the point" >&2; sleep 0.5'],
                         log, data, legs, ("reached the point", 1), timeout=30)
        self.assertEqual(len(sent), 1, "the pattern never matched")
        self.assertEqual(legs[0]["exit"], 0)
        self.assertIs(legs[0]["killed"], False)


class Figures(Harness):
    def test_the_batch_high_water_counts_bundles_and_staged_runs(self):
        publisher = self.publisher("holds", then=(
            'for name, size in [("prepared", 1000), ("staging", 2000)]:\n'
            '    held = run / "data" / ".stelae-batch" / name\n'
            "    held.mkdir(parents=True)\n"
            '    (held / "file").write_bytes(b"x" * size)\n'
            # Long enough for a sample, which comes once a second.
            "time.sleep(2.5)\n"
        ))
        done = self.run_tool(publisher)
        self.assertEqual(done.returncode, 0, done.stderr)
        leg = self.rows()[0]["legs"][0]
        self.assertEqual(leg["high_water"]["prepared"], 1000)
        self.assertEqual(leg["high_water"]["staged"], 2000)
        self.assertEqual(leg["batch_high_water"], 3000)

    def test_upload_figures_count_every_time_a_blob_went(self):
        self.registry = self.registry_answering({"cardano/measured": {
            "uploads": {"sha256:a": [10, 3], "sha256:b": [5, 1]},
            "received": 47,
            "tags": [["epoch-1", "x"], ["latest", "x"]],
        }})
        done = self.run_tool(self.publisher("ok"))
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertEqual(row["uploaded_bytes"], 35)
        self.assertEqual(row["received_bytes"], 47)
        self.assertEqual(row["blobs_uploaded"], 2)
        self.assertEqual(row["blobs_uploaded_more_than_once"], 1)
        self.assertEqual(row["tag_writes"], 2)


class Compare(Harness):
    def publish(self, name, tags, *extra):
        done = self.run_tool(self.publisher(name, tags), *extra)
        self.assertEqual(done.returncode, 0, done.stderr)

    def test_runs_with_the_same_tags_are_identical(self):
        self.publish("first", {"epoch-1": "one"})
        self.publish("second", {"epoch-1": "one"})
        done = self.read_with("compare")
        self.assertEqual(done.returncode, 0, done.stdout)
        self.assertIn("identical", done.stdout)
        self.assertIn("1 tags, 0 disagree", done.stdout)

    def test_a_tag_that_differs_fails(self):
        self.publish("first", {"epoch-1": "one"})
        self.publish("second", {"epoch-1": "two"})
        done = self.read_with("compare")
        self.assertEqual(done.returncode, 1, done.stdout)
        self.assertIn("DIFFERS at ['epoch-1']", done.stdout)

    def test_a_tag_that_is_missing_fails(self):
        self.publish("first", {"epoch-0": "zero", "epoch-1": "one"})
        self.publish("second", {"epoch-1": "one"})
        done = self.read_with("compare")
        self.assertEqual(done.returncode, 1, done.stdout)
        self.assertIn("DIFFERS at ['epoch-0']", done.stdout)

    def test_workloads_are_held_to_one_lineage_unless_separated(self):
        self.publish("first", {"epoch-1": "one"})
        self.publish("second", {"epoch-1": "two"}, "--workload", "elsewhere")
        done = self.read_with("compare")
        self.assertEqual(done.returncode, 1, done.stdout)
        self.assertIn("1 tags, 1 disagree", done.stdout)
        done = self.read_with("compare", "--separate", "elsewhere")
        self.assertEqual(done.returncode, 0, done.stdout)
        self.assertIn("1 tags, 0 disagree", done.stdout)


class Summarize(Harness):
    def paired(self, baseline_seconds, candidate_seconds):
        for name, position in [("baseline", "1"), ("candidate", "2")]:
            self.run_tool(self.publisher(name), "--round", "1", "--position", position)
        rows = self.rows()
        for row, seconds in zip(rows, [baseline_seconds, candidate_seconds]):
            row["wall_seconds_total"] = seconds
        (self.root / "observations.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        return self.read_with("summarize", "--pair", "baseline:candidate")

    def test_a_reduction_short_of_the_target_is_not_met(self):
        done = self.paired(100, 90)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("Median 10.0%, so the 25% target is NOT met.", done.stdout)

    def test_a_reduction_past_the_target_is_met(self):
        done = self.paired(100, 70)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("Median 30.0%, so the 25% target is met.", done.stdout)

    def test_a_reduction_on_the_target_is_met(self):
        done = self.paired(100, 75)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("Median 25.0%, so the 25% target is met.", done.stdout)

    def rounds(self, *walls, failed=()):
        """One baseline and one candidate wall time a round, from round 1 on. In a round
        named in `failed`, the candidate's run failed.

        As in the report's rounds, a third arm runs beside them, and the order turns
        every other round, so a pair is told by its arms and not by where they ran.
        """
        for name, position in [("baseline", "1"), ("candidate", "2")]:
            self.run_tool(self.publisher(name), "--round", "1", "--position", position)
        baseline, candidate = self.rows()
        rows = []
        for number, pair in enumerate(walls, start=1):
            arms = [(baseline, pair[0]), (candidate, pair[1]), (dict(baseline, arm="other"), 1)]
            if number % 2 == 0:
                arms.reverse()
            for position, (row, seconds) in enumerate(arms, start=1):
                failing = row is candidate and number in failed
                rows.append(dict(row, round=number, position_in_round=position,
                                 wall_seconds_total=seconds,
                                 succeeded=row["succeeded"] and not failing))
        (self.root / "observations.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        return self.read_with("summarize", "--pair", "baseline:candidate")

    def test_the_verdict_is_the_median_of_the_rounds(self):
        # The baselines differ, so a candidate held to another round's baseline gives other
        # figures. The far round would carry a mean over the target, and it comes first so
        # the median isn't the middle round.
        done = self.rounds((50, 15), (100, 90), (200, 176))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("3 pairs: 70.0%, 10.0%, 12.0%. Median 12.0%, so the 25% target is NOT met.",
                      done.stdout)

    def test_a_median_past_the_target_is_met_with_one_round_short(self):
        # Neither the lowest round, which comes last, nor the mean reaches it.
        done = self.rounds((200, 140), (50, 35), (100, 90))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("3 pairs: 30.0%, 30.0%, 10.0%. Median 30.0%, so the 25% target is met.",
                      done.stdout)

    def test_a_round_missing_its_candidate_pairs_nothing(self):
        # Two pairs left, so the median is their mean, and the next round's run fills no gap.
        done = self.rounds((100, 90), (200, 20), (50, 35), failed={2})
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("2 pairs: 10.0%, 30.0%. Median 20.0%, so the 25% target is NOT met.",
                      done.stdout)

    def test_runs_to_different_epochs_never_share_a_median(self):
        self.run_tool(self.publisher("shorter"))
        self.run_tool(self.publisher("longer", {"epoch-1": "one", "epoch-2": "two"}),
                      "--until", "2")
        done = self.read_with("summarize")
        self.assertEqual(done.returncode, 0, done.stderr)
        sections = done.stdout.split("\n### ")[1:]
        self.assertEqual([section.splitlines()[0] for section in sections],
                         ["test, to epoch 1", "test, to epoch 2"])
        self.assertIn("| shorter |", sections[0])
        self.assertNotIn("| longer |", sections[0])

    def test_replay_is_what_no_span_covers_whatever_the_arm_is_called(self):
        # A batch of one stages inside a publication, under a name other than `w1`.
        lines = [logged(0, "opened"), logged(10, "publishing pending boundary sequence=1"),
                 logged(12, 'index staging traversal started traversal="exact"'),
                 logged(15, "index staging finished"),
                 logged(20, "published pending boundary sequence=1"), logged(30, "done")]
        self.run_tool(self.publisher("renamed", then=(
            f"print({chr(10).join(lines)!r}, file=sys.stderr, flush=True)\n"
        )))
        done = self.read_with("summarize")
        self.assertEqual(done.returncode, 0, done.stderr)
        row = next(line for line in done.stdout.splitlines() if "| renamed |" in line)
        cells = [cell.strip() for cell in row.split("|")]
        replay, capture, stage, publish = cells[7:11]
        self.assertEqual((replay, capture, stage, publish), ("20", "0", "3", "10"))

    def test_every_kind_of_row_is_read(self):
        self.run_tool(self.publisher("ok"))
        self.run_tool(self.script("fails", "#!/bin/sh\nexit 3\n"))
        self.run_tool(self.root / "absent")
        done = self.read_with("compare")
        self.assertEqual(done.returncode, 0, done.stderr)
        done = self.read_with("summarize")
        self.assertEqual(done.returncode, 0, done.stderr)
        table = [line for line in done.stdout.splitlines() if line.startswith("| 2")]
        self.assertEqual(len(table), 3, done.stdout)
        self.assertIn("FileNotFoundError", table[2])
        self.assertEqual(len({line.count("|") for line in table}), 1, table)


class Restores(Harness):
    RUN = "20260101T000000-kept"

    def setUp(self):
        super().setUp()
        repository = self.root / "runs" / self.RUN / "registry" / "cardano" / "measured"
        tags, blobs = repository / "manifests" / "tags", repository / "blobs" / "sha256"
        tags.mkdir(parents=True)
        blobs.mkdir(parents=True)
        for sequence in [1, 2]:
            inscription = f"inscription-{sequence}"
            (blobs / inscribed(inscription)).write_text(inscription)
            manifest = {"config": {"digest": f"sha256:{inscribed(inscription)}"}, "layers": []}
            (tags / f"epoch-{sequence}").write_text(json.dumps(manifest))
        self.evidence = self.root / "evidence" / self.RUN
        self.evidence.mkdir(parents=True)
        self.consumer = self.script("consumer", CONSUMER.format(python=sys.executable))

    def restore(self, prefix="inscription-"):
        digest = self.script("digest", DIGEST.format(python=sys.executable, prefix=prefix))
        return subprocess.run(
            [sys.executable, str(TOOL), "restore", "--root", str(self.root), "--run", self.RUN,
             "--sequence", "1", "--sequence", "2", "--consumer", str(self.consumer),
             "--publisher", str(digest), "--loopback", str(self.registry),
             "--state-epochs", str(self.root / "state-epochs")],
            capture_output=True, text=True,
        )

    def lines(self):
        text = (self.root / "restores.jsonl").read_text()
        return [json.loads(line) for line in text.splitlines()]

    def test_a_digest_that_matches_the_inscription_is_reproduced(self):
        done = self.restore()
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual([line["reproduced"] for line in self.lines()], [True, True])
        self.assertTrue((self.evidence / "restore-1.log").is_file())
        self.assertFalse((self.root / "restores" / self.RUN / "1").exists())

    def test_a_digest_that_differs_is_not_reproduced(self):
        done = self.restore(prefix="something else ")
        self.assertEqual(done.returncode, 1)
        self.assertEqual([line["reproduced"] for line in self.lines()], [False, False])

    def test_a_consumer_that_restores_and_then_fails_is_not_reproduced(self):
        self.consumer = self.script("consumer", CONSUMER.format(python=sys.executable)
                                    + "sys.exit(2)\n")
        done = self.restore()
        self.assertEqual(done.returncode, 1)
        self.assertEqual([(line["restore_exit"], line["digest_exit"], line["reproduced"])
                          for line in self.lines()], [(2, 0, False), (2, 0, False)])

    def test_a_consumer_that_fails_is_not_reproduced(self):
        self.consumer = self.script("consumer", "#!/bin/sh\nexit 2\n")
        done = self.restore()
        self.assertEqual(done.returncode, 1)
        line = self.lines()[0]
        self.assertEqual(line["restore_exit"], 2)
        self.assertFalse(line["reproduced"])

    def test_a_line_that_cannot_be_appended_reaches_the_terminal_and_the_rest_still_run(self):
        (self.root / "restores.jsonl").mkdir()
        done = self.restore()
        self.assertEqual(done.returncode, 1)
        self.assertEqual(done.stderr.count("observation not written"), 2, done.stderr)
        printed = [json.loads(line) for line in done.stdout.splitlines()]
        self.assertEqual([line["sequence"] for line in printed], [1, 2])
        self.assertTrue(all(line["reproduced"] for line in printed))

    def test_evidence_that_cannot_be_kept_fails_and_the_rest_still_run(self):
        shutil.rmtree(self.evidence)
        self.evidence.write_text("in the way")
        done = self.restore()
        self.assertEqual(done.returncode, 1)
        self.assertEqual([line["reproduced"] for line in self.lines()], [True, True])
        self.assertTrue((self.root / "restores" / self.RUN / "2" / "log").is_file(),
                        "the only copy went")


class Prepare(Harness):
    def setUp(self):
        super().setUp()
        self.calls, self.fails = self.root / "replays", self.root / "replay-fails"
        self.registry = self.script("replaying", REPLAYING_REGISTRY.format(
            python=sys.executable, calls=str(self.calls), fails=str(self.fails)))
        self.publisher = self.script("publishes", SEEDING_PUBLISHER.format(python=sys.executable))
        self.base = self.root / "base" / "preprod"

    def prepare(self, epoch):
        return subprocess.run(
            [sys.executable, str(TOOL), "prepare", "--root", str(self.root),
             "--corpus", str(self.root / "corpus"),
             "--state-epochs", str(self.root / "state-epochs"), "--loopback", str(self.registry),
             "--publisher", str(self.publisher), "--base-epoch", str(epoch)],
            capture_output=True, text=True, timeout=60,
        )

    def prepared(self, epoch):
        done = self.prepare(epoch)
        self.assertEqual(done.returncode, 0, done.stderr)

    def seeded(self):
        return sorted(os.listdir(self.base / "registry/cardano/measured/manifests/tags"))

    def replay_stops(self, epoch):
        self.fails.write_text("")
        self.assertNotEqual(self.prepare(epoch).returncode, 0)
        self.fails.unlink()

    def test_an_earlier_replays_record_does_not_vouch_for_a_replay_that_stopped(self):
        self.prepared(256)
        shutil.rmtree(self.base / "data")
        self.replay_stops(256)
        done = self.prepare(256)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("isn't a finished replay to epoch 256", done.stderr)
        self.assertEqual(self.calls.read_text().split(), ["256", "256"])

    def test_a_replay_to_another_epoch_that_stopped_is_not_taken_for_the_first(self):
        self.prepared(256)
        self.assertNotEqual(self.prepare(260).returncode, 0)
        shutil.rmtree(self.base / "data")
        self.replay_stops(260)
        done = self.prepare(256)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("isn't a finished replay to epoch 256", done.stderr)
        self.assertEqual(self.calls.read_text().split(), ["256", "260"])

    def test_the_refusals_remedy_leaves_no_seed_from_another_epoch(self):
        self.prepared(256)
        self.assertEqual(self.seeded(), ["epoch-256"])
        done = self.prepare(200)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn(f"delete {self.base} and prepare again", done.stderr)
        shutil.rmtree(self.base)
        self.prepared(200)
        self.assertEqual(self.seeded(), ["epoch-200"])

    def test_a_new_replay_starts_a_new_seed(self):
        self.prepared(256)
        shutil.rmtree(self.base / "data")
        self.prepared(200)
        self.assertEqual(self.seeded(), ["epoch-200"])
        self.assertEqual((self.base / "replayed-to").read_text().split(), ["200"])

    def test_a_replay_that_stopped_is_not_taken_for_a_base(self):
        self.fails.write_text("")
        self.assertNotEqual(self.prepare(256).returncode, 0)
        self.assertTrue((self.root / "base" / "preprod" / "data").is_dir())
        self.fails.unlink()
        done = self.prepare(256)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("isn't a finished replay to epoch 256", done.stderr)
        self.assertEqual(self.calls.read_text().split(), ["256"])

    def test_a_finished_replay_is_kept_for_its_own_epoch(self):
        for epoch in [256, 256]:
            done = self.prepare(epoch)
            self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(self.calls.read_text().split(), ["256"])
        done = self.prepare(300)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("isn't a finished replay to epoch 300", done.stderr)


class Profiles(unittest.TestCase):
    """What `profile-summary.py` makes of the samples it is given."""

    def test_a_missing_sample_and_an_empty_one_leave_the_rest_read(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "empty.txt").write_text("Call graph:\n    5 Thread_1\n")
        (root / "real.txt").write_text(
            "Call graph:\n"
            "    10 stelae_driver::retry::bounded::h0123456789abcdef  (in stelae)\n"
            "    + 4 stelae_driver::publish::PublishRecord::save  (in stelae)\n"
            "Sort by top of stack, same collapsed (when >= 5):\n"
            "        __fcntl  (in libsystem_kernel.dylib)        2\n"
            "Binary Images:\n"
        )
        done = subprocess.run(
            [sys.executable, "-I", str(Path(__file__).with_name("profile-summary.py")),
             str(root / "missing.txt"), str(root / "empty.txt"), str(root / "real.txt")],
            capture_output=True, text=True,
        )
        self.assertEqual(done.returncode, 0, done.stderr)
        summaries = [line for line in done.stdout.splitlines() if not line.startswith(" ")]
        self.assertEqual(summaries[0], "missing: no sample, No such file or directory")
        self.assertEqual(summaries[1], "empty: 0 samples of a publication, so no shares")
        self.assertEqual(summaries[2], "real: 10 samples, save 40%, file sync 20%")


class Sample(unittest.TestCase):
    """The sampler's waits end. Reaching `sample` itself takes macOS and 28 s."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.run_dir = Path(directory.name) / "run"
        self.run_dir.mkdir()
        self.output = Path(directory.name) / "sample.txt"

    def sample(self, *extra):
        return subprocess.run(
            ["bash", str(SAMPLER), str(self.run_dir), "5", str(self.output), *extra],
            capture_output=True, text=True, timeout=60,
        )

    def test_a_run_that_ended_before_its_sequence_ends_the_wait(self):
        (self.run_dir / "publisher.log").write_text("nothing yet\n")
        ended = subprocess.Popen(["true"])
        ended.wait()
        done = self.sample(str(ended.pid))
        self.assertEqual(done.returncode, 1)
        self.assertIn("ended before publishing 5", done.stderr)

    def test_a_kept_run_whose_row_is_written_ends_the_wait(self):
        root = self.run_dir.parent / "root"
        kept = root / "runs" / "20260101T000000-kept"
        kept.mkdir(parents=True)
        (kept / "publisher.log").write_text("nothing yet\n")
        (root / "observations.jsonl").write_text(json.dumps({"run": kept.name}) + "\n")
        done = subprocess.run(["bash", str(SAMPLER), str(kept), "5", str(self.output)],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 1)
        self.assertIn("ended before publishing 5", done.stderr)

    def test_a_run_directory_that_went_ends_the_wait(self):
        shutil.rmtree(self.run_dir)
        done = self.sample()
        self.assertEqual(done.returncode, 1)
        self.assertIn("ended before publishing 5", done.stderr)

    def test_no_publisher_to_sample_is_an_error(self):
        (self.run_dir / "publisher.log").write_text("publishing pending boundary sequence=5 x\n")
        done = self.sample()
        self.assertEqual(done.returncode, 1)
        self.assertIn("no publisher running", done.stderr)
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
