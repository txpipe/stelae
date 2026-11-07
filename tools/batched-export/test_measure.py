#!/usr/bin/env python3
"""What `measure.py run` leaves behind when a run goes wrong.

Stand-ins replace the publisher and the registry, so this takes seconds and needs
nothing built. Nothing in CI runs it: `python3 tools/batched-export/test_measure.py`.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

TOOL = Path(__file__).with_name("measure.py")

REGISTRY = """#!{python}
import http.server, os
open({pid!r}, "w").write(str(os.getpid()))
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{{}}")
    def log_message(self, *_):
        pass
server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
"""


def gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


class Runs(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        for path in ["base/data", "base/registry", "corpus"]:
            (self.root / path).mkdir(parents=True)
        (self.root / "state-epochs").write_text("4\n")
        self.registry_pid = self.root / "registry.pid"
        self.registry = self.script(
            "registry", REGISTRY.format(python=sys.executable, pid=str(self.registry_pid))
        )

    def script(self, name, body):
        path = self.root / name
        path.write_text(body)
        path.chmod(0o755)
        return path

    def command(self, binary, *extra):
        arm = json.dumps({"name": "arm", "binary": str(binary)})
        return [sys.executable, str(TOOL), "run", "--root", str(self.root),
                "--corpus", str(self.root / "corpus"),
                "--state-epochs", str(self.root / "state-epochs"),
                "--loopback", str(self.registry), "--workload", "test", "--base", "base",
                "--until", "1", "--first-file", "0", "--last-file", "0", "--arm", arm, *extra]

    def run_tool(self, binary, *extra):
        return subprocess.run(self.command(binary, *extra), capture_output=True, text=True)

    def rows(self):
        text = (self.root / "observations.jsonl").read_text()
        return [json.loads(line) for line in text.splitlines()]

    def test_a_publisher_that_fails_leaves_its_exit_and_its_log(self):
        done = self.run_tool(self.script("fails", "#!/bin/sh\necho said >&2\nexit 3\n"))
        self.assertEqual(done.returncode, 0, done.stderr)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertEqual(row["legs"][0]["exit"], 3)
        self.assertNotIn("error", row)
        evidence = self.root / "evidence" / row["run"] / "publisher.log"
        self.assertEqual(evidence.read_text(), "said\n")

    def test_a_binary_that_is_not_there_leaves_a_row_and_no_legs(self):
        done = self.run_tool(self.root / "absent")
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertEqual(row["legs"], [])
        self.assertIn("FileNotFoundError", row["error"])

    def test_a_registry_that_does_not_start_leaves_a_row(self):
        self.registry = self.script("registry", "#!/bin/sh\nexit 1\n")
        done = self.run_tool(self.script("ok", "#!/bin/sh\nexit 0\n"))
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertIn("ValueError", row["error"])

    def test_a_stopped_tool_records_the_run_and_takes_its_children_with_it(self):
        pid_file = self.root / "publisher.pid"
        publisher = self.script("sleeps", f'#!/bin/sh\necho $$ > "{pid_file}"\nexec sleep 600\n')
        tool = subprocess.Popen(
            self.command(publisher), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        began = time.monotonic()
        while not pid_file.exists() or not pid_file.read_text().strip():
            self.assertLess(time.monotonic() - began, 60, "the publisher never started")
            time.sleep(0.05)
        tool.send_signal(signal.SIGTERM)
        self.assertEqual(tool.wait(timeout=60), 1)
        row = self.rows()[0]
        self.assertFalse(row["succeeded"])
        self.assertIn("SIGTERM", row["error"])
        self.assertEqual(row["legs"][0]["killed"], "tool stopped")
        self.assertTrue(gone(int(pid_file.read_text())), "the publisher outlived the tool")
        self.assertTrue(gone(int(self.registry_pid.read_text())), "the registry outlived the tool")

    def test_evidence_that_cannot_be_kept_costs_the_evidence_and_not_the_row(self):
        (self.root / "evidence").write_text("in the way")
        done = self.run_tool(self.script("ok", "#!/bin/sh\nexit 0\n"))
        self.assertEqual(done.returncode, 1)
        row = self.rows()[0]
        self.assertTrue(row["succeeded"])
        self.assertTrue((self.root / "runs" / row["run"]).is_dir(), "the only copy went")

    def test_the_host_is_recorded_as_figures_with_no_process_names(self):
        self.run_tool(self.script("ok", "#!/bin/sh\nexit 0\n"))
        row = self.rows()[0]
        self.assertTrue(row["busiest_at_start"])
        self.assertTrue(all(isinstance(cpu, float) for cpu in row["busiest_at_start"]))
        self.assertIn("load", row["host_at_start"])

    def test_summarize_and_compare_read_every_kind_of_row(self):
        self.run_tool(self.script("ok", "#!/bin/sh\nexit 0\n"))
        self.run_tool(self.script("fails", "#!/bin/sh\nexit 3\n"))
        self.run_tool(self.root / "absent")
        for command in ["summarize", "compare"]:
            done = subprocess.run([sys.executable, str(TOOL), command, "--root", str(self.root)],
                                  capture_output=True, text=True)
            self.assertEqual(done.returncode, 0, done.stderr)
            if command == "summarize":
                table = [line for line in done.stdout.splitlines() if line.startswith("| 2")]
                self.assertEqual(len(table), 3, done.stdout)
                self.assertIn("FileNotFoundError", table[2])
                widths = {line.count("|") for line in table}
                self.assertEqual(len(widths), 1, table)


if __name__ == "__main__":
    unittest.main()
