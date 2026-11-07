#!/usr/bin/env python3
"""Measure complete publisher runs on one host, over disposable local transports.

Every run starts from a copy-on-write clone of the same stores, chain files and
seeded repository. It ends with one line in observations.jsonl, whether the
publisher failed, this tool hit an error or it was told to stop, and nothing here
overwrites a line. SIGKILL of this tool leaves none, and on a volume too full to
take the line it goes to stderr instead. See docs/batched-export-measurement.md
for the procedure.

Standard library only. macOS and Linux.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPOSITORY = "cardano/measured"
SAMPLE_SECONDS = 1.0
TRAVERSAL = re.compile(r'index staging traversal started.*?traversal="?(\w+)')
COMMITTED = re.compile(r'committed sequence=(\d+)')
TIMESTAMP = re.compile(r'^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z')


class Stopped(Exception):
    """This tool was told to stop, so what it started has to stop with it."""


def stop_on_signal(number, _frame):
    raise Stopped(signal.Signals(number).name)


def clone(source, destination):
    """A copy-on-write clone where the filesystem has one, else a real copy."""
    flag = "-c" if sys.platform == "darwin" else "--reflink=auto"
    subprocess.run(["cp", flag, "-R", str(source), str(destination)], check=True)


def tree_bytes(path):
    """Bytes under `path`, read as it changes: anything may vanish mid-walk."""
    total = 0
    stack = [str(path)]
    while stack:
        try:
            with os.scandir(stack.pop()) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        pass
        except OSError:
            pass
    return total


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_config(path, network, storage):
    Path(path).write_text(
        'compatibility = "v2-alpha.2"\n\n'
        f'[chain]\nprotocol = "cardano"\nnetwork = "{network}"\n\n'
        f'[storage]\npath = "{storage}"\n'
    )


class Loopback:
    """The disposable registry and Mithril listing, for the span of one run."""

    def __init__(self, binary, root, environment=None):
        env = dict(os.environ, **(environment or {}))
        self.process = subprocess.Popen(
            [str(binary), str(root), "0"], stdout=subprocess.PIPE, text=True, env=env
        )
        try:
            self.port = int(self.process.stdout.readline())
        except BaseException:
            self.stop()
            raise

    def repository(self):
        return f"oci://127.0.0.1:{self.port}/{REPOSITORY}"

    def aggregator(self):
        return f"http://127.0.0.1:{self.port}/aggregator"

    def accounting(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/accounting") as answer:
            return json.load(answer).get(REPOSITORY, {"uploads": {}, "received": 0, "tags": []})

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        self.process.wait()


class Sampler(threading.Thread):
    """High-water disk use by what holds it, and the host's load, once a second."""

    def __init__(self, data):
        super().__init__(daemon=True)
        self.paths = {
            "prepared": data / ".stelae-batch" / "prepared",
            "staged": data / ".stelae-batch" / "staging",
            "spool": data / "scratch",
            "stores": data,
        }
        self.high = {name: 0 for name in self.paths}
        self.stores_at_start = tree_bytes(data)
        self.data = data
        self.volume_at_start = self.volume_low = shutil.disk_usage(data).free
        self.batch_high = 0
        self.samples = 0
        self.load = []
        self.halt = threading.Event()

    def run(self):
        while not self.halt.is_set():
            sizes = {name: tree_bytes(path) for name, path in self.paths.items()}
            for name, size in sizes.items():
                self.high[name] = max(self.high[name], size)
            self.batch_high = max(self.batch_high, sizes["prepared"] + sizes["staged"])
            # Unlinked layers staged for upload show only in the volume's free
            # space, which anything else on that volume moves too.
            self.volume_low = min(self.volume_low, shutil.disk_usage(self.data).free)
            self.load.append(os.getloadavg()[0])
            self.samples += 1
            self.halt.wait(SAMPLE_SECONDS)


def busiest():
    """The CPU of the host's busiest processes: what a noisy measurement shared.

    Figures only. Observations get committed, and a process's name says what else
    a host runs.
    """
    listing = subprocess.run(
        ["ps", "-Ao", "pcpu="], capture_output=True, text=True, check=False
    ).stdout.split()
    figures = []
    for figure in listing:
        try:
            figures.append(float(figure))
        except ValueError:
            pass
    return sorted(figures, reverse=True)[:5]


def host_state(path):
    """Free memory and volume space, where a run started from."""
    state = {"volume_free_bytes": shutil.disk_usage(path).free, "load": os.getloadavg()[0]}
    if sys.platform == "darwin":
        pages = subprocess.run(["vm_stat"], capture_output=True, text=True, check=False).stdout
        found = re.search(r"Pages free:\s+(\d+)", pages)
        size = re.search(r"page size of (\d+)", pages)
        if found and size:
            state["free_memory_bytes"] = int(found.group(1)) * int(size.group(1))
    return state


def phases(log_path):
    """What the log shows of a run, in the seconds its timestamps allow.

    `capture`, `stage` and `publish` are spans between markers. `stage` sits
    inside `publish` on a batch of one, which stages as it publishes. Replay and
    the stores' opening and closing are what's left of the log's whole span. The
    old exporter logs no traversal and walks its indexes beside its layers.
    """
    traversals = {}
    spans = {"capture": 0.0, "stage": 0.0, "publish": 0.0}
    opened = {}
    first = last = None
    committed = []

    def close(name, at):
        if name in opened:
            spans[name] += at - opened.pop(name)

    for line in Path(log_path).read_text(errors="replace").splitlines():
        stamp = TIMESTAMP.match(line)
        if not stamp:
            continue
        at = datetime.fromisoformat(stamp.group(1)).replace(tzinfo=timezone.utc).timestamp()
        first = first if first is not None else at
        last = at
        found = TRAVERSAL.search(line)
        if found:
            traversals[found.group(1)] = traversals.get(found.group(1), 0) + 1
            opened.setdefault("stage", at)
        elif "index staging finished" in line:
            close("stage", at)
        elif "preparing boundary" in line:
            opened["capture"] = at
        elif "batch frontiers" in line:
            close("capture", at)
        elif "publishing pending boundary" in line:
            opened["publish"] = at
        elif re.search(r"published (pending|prepared) boundary|nothing to publish", line):
            close("publish", at)
        found = COMMITTED.search(line)
        if found:
            committed.append((int(found.group(1)), at))
    return {
        "traversals": traversals,
        "capture_seconds": spans["capture"],
        "stage_seconds": spans["stage"],
        "publish_seconds": spans["publish"],
        "log_seconds": (last - first) if first is not None else None,
        "committed": committed,
    }


def publications(registry):
    """Each tag's manifest and inscription by digest, and the sizes they name."""
    tags = registry / REPOSITORY / "manifests" / "tags"
    blobs = registry / REPOSITORY / "blobs" / "sha256"
    found = {}
    for tag in sorted(os.listdir(tags)) if tags.is_dir() else []:
        body = (tags / tag).read_bytes()
        manifest = json.loads(body)
        config = manifest["config"]["digest"].split(":")[1]
        found[tag] = {
            "manifest": hashlib.sha256(body).hexdigest(),
            "inscription": config,
            "canonical_bytes": (blobs / config).stat().st_size,
            "layers": len(manifest["layers"]),
            "output_bytes": sum(layer["size"] for layer in manifest["layers"]),
        }
    return found


def keep_evidence(registry, out):
    """Manifests and inscriptions outlive the run, the layers don't need to."""
    tags = registry / REPOSITORY / "manifests" / "tags"
    blobs = registry / REPOSITORY / "blobs" / "sha256"
    out.mkdir(parents=True, exist_ok=True)
    for tag in os.listdir(tags) if tags.is_dir() else []:
        body = (tags / tag).read_bytes()
        (out / f"{tag}.manifest.json").write_bytes(body)
        config = json.loads(body)["config"]["digest"].split(":")[1]
        shutil.copyfile(blobs / config, out / f"{tag}.inscription.json")


def publisher_command(arm, config, loopback, args):
    command = [arm["binary"], "--config", str(config), "backfill"]
    command += ["--repo", loopback.repository(), "--insecure"]
    # Listing a snapshot builds a verifier from the network's key, which the
    # catalog supplies; nothing is fetched while the files on disk reach `until`.
    command += ["--state-epochs-file", str(args.state_epochs)]
    command += ["--mithril-aggregator", loopback.aggregator()]
    command += ["--until-epoch", str(args.until), "--concurrency", str(args.concurrency)]
    return command + arm.get("flags", [])


def kill(process):
    """SIGKILL, without the reaping `Popen.kill` may do behind `os.wait4`."""
    try:
        os.kill(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_once(command, log_path, data, legs, kill_on=None, timeout=None):
    """One process to its end, and what it cost, added to `legs`.

    `kill_on` is (pattern, nth). However this ends, the process is gone by the
    time it returns or raises, and a process that started has its leg.
    """
    sampler = Sampler(data)
    sampler.start()
    began = time.perf_counter()
    env = dict(os.environ, NO_COLOR="1")
    process = None
    killed = False
    status = None
    usage = None
    try:
        with open(log_path, "ab") as log:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=log, env=env)
        while status is None:
            pid, wait_status, wait_usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                status, usage = wait_status, wait_usage
                break
            if timeout and not killed and time.perf_counter() - began > timeout:
                kill(process)
                killed = "timeout"
            if kill_on and not killed:
                pattern, nth = kill_on
                text = Path(log_path).read_text(errors="replace")
                if len(re.findall(pattern, text)) >= nth:
                    kill(process)
                    killed = "planned"
            time.sleep(0.05)
    finally:
        if process is not None and status is None:
            kill(process)
            _, status, usage = os.wait4(process.pid, 0)
            killed = killed or "tool stopped"
        wall = time.perf_counter() - began
        sampler.halt.set()
        sampler.join()
        if process is not None:
            process.returncode = os.waitstatus_to_exitcode(status)
            legs.append({
                "exit": process.returncode,
                "killed": killed,
                "wall_seconds": wall,
                "user_seconds": usage.ru_utime,
                "system_seconds": usage.ru_stime,
                # Bytes on macOS, KiB on Linux.
                "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                "inblock": usage.ru_inblock,
                "oublock": usage.ru_oublock,
                "high_water": sampler.high,
                "stores_at_start": sampler.stores_at_start,
                "volume_drop_bytes": sampler.volume_at_start - sampler.volume_low,
                "batch_high_water": sampler.batch_high,
                "load_median": statistics.median(sampler.load) if sampler.load else None,
                "load_max": max(sampler.load) if sampler.load else None,
                "samples": sampler.samples,
            })


def immutable_slice(corpus, target, first, last):
    """Clone the numbered files a run replays, which it deletes as it goes."""
    target.mkdir(parents=True, exist_ok=True)
    flag = "-c" if sys.platform == "darwin" else "--reflink=auto"
    sources = []
    for number in range(first, last + 1):
        for extension in ["chunk", "primary", "secondary"]:
            source = corpus / f"{number:05}.{extension}"
            if source.exists():
                sources.append(str(source))
    for start in range(0, len(sources), 256):
        subprocess.run(["cp", flag, *sources[start:start + 256], str(target)], check=True)


def command_prepare(args):
    """Replay to the base epoch and seed a repository with its stele, once."""
    root = Path(args.root)
    base = root / "base" / args.network
    config = base / "dolos.toml"
    base.mkdir(parents=True, exist_ok=True)
    if not (base / "data").exists():
        write_config(config, args.network, base / "data")
        subprocess.run(
            [args.loopback, "replay", str(config), str(args.corpus), str(args.base_epoch)],
            check=True,
        )
    seed = base / "registry"
    loopback = Loopback(args.loopback, seed)
    try:
        subprocess.run(
            [args.publisher, "--config", str(config), "publish",
             "--repo", loopback.repository(), "--insecure",
             "--state-epochs-file", str(args.state_epochs),
             "--concurrency", str(args.concurrency)],
            check=True, env=dict(os.environ, NO_COLOR="1"),
        )
    finally:
        loopback.stop()


def record(root, observation):
    """Append a row, or failing that print it: the row is what a run leaves."""
    line = json.dumps(observation) + "\n"
    try:
        with open(root / "observations.jsonl", "a") as out:
            out.write(line)
    except OSError as error:
        print(f"observation not written, {error}: {line}", file=sys.stderr, flush=True)


def measure(args, arm, run_dir, observation):
    """Fill in `observation` as far as the run gets."""
    root = Path(args.root)
    run_dir.mkdir(parents=True)
    base = root / args.base
    data, registry = run_dir / "data", run_dir / "registry"
    observation["binary_sha256"] = sha256_of(arm["binary"])
    prepared_at = time.perf_counter()
    if args.from_genesis:
        data.mkdir()
        registry.mkdir()
    else:
        clone(base / "data", data)
        clone(base / "registry", registry)
    immutable_slice(
        Path(args.corpus), data / "mithril" / "immutable", args.first_file, args.last_file
    )
    config = run_dir / "dolos.toml"
    write_config(config, args.network, data)
    observation["clone_seconds"] = time.perf_counter() - prepared_at

    environment = {}
    if args.fail_every:
        environment["LOOPBACK_FAIL_UPLOAD_EVERY"] = str(args.fail_every)
    if args.discard_above:
        environment["LOOPBACK_DISCARD_ABOVE"] = str(args.discard_above)
    observation["busiest_at_start"] = busiest()
    observation["host_at_start"] = host_state(run_dir)
    loopback = Loopback(args.loopback, registry, environment)
    log = run_dir / "publisher.log"
    legs = observation["legs"]
    try:
        command = publisher_command(arm, config, loopback, args)
        kill_on = (args.kill_on, args.kill_nth) if args.kill_on else None
        run_once(command, log, data, legs, kill_on, args.timeout)
        if kill_on and legs[-1]["killed"] == "planned":
            # The same arm again, or the arms named: an upgrade or a rollback.
            for rerun in [json.loads(a) for a in args.rerun_arm] or [arm]:
                rerun_command = publisher_command(rerun, config, loopback, args)
                run_once(rerun_command, log, data, legs, None, args.timeout)
                legs[-1]["arm"] = rerun["name"]
        accounting = loopback.accounting()
    finally:
        if log.exists():
            observation["phases"] = phases(log)
        loopback.stop()
    observation["uploaded_bytes"] = sum(s * c for s, c in accounting["uploads"].values())
    observation["received_bytes"] = accounting["received"]
    observation["blobs_uploaded"] = len(accounting["uploads"])
    observation["blobs_uploaded_more_than_once"] = sum(
        1 for _, c in accounting["uploads"].values() if c > 1
    )
    observation["tag_writes"] = len(accounting["tags"])
    observation["publications"] = publications(registry)
    observation["succeeded"] = legs[-1]["exit"] == 0


def command_run(args):
    """One observation, appended whatever its outcome."""
    root = Path(args.root)
    arm = json.loads(args.arm)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{arm['name']}"
    run_dir = root / "runs" / run_id
    observation = {
        "run": run_id,
        "arm": arm["name"],
        "flags": arm.get("flags", []),
        "binary_sha256": None,
        "workload": args.workload,
        "round": args.round,
        "position_in_round": args.position,
        "fault": args.kill_on or (f"fail upload every {args.fail_every}" if args.fail_every else None),
        "until_epoch": args.until,
        "started_utc": run_id.split("-")[0],
        "clone_seconds": None,
        "busiest_at_start": [],
        "host_at_start": {},
        "legs": [],
        "wall_seconds_total": 0,
        "phases": None,
        "uploaded_bytes": None,
        "received_bytes": None,
        "blobs_uploaded": None,
        "blobs_uploaded_more_than_once": None,
        "tag_writes": None,
        "publications": {},
        "succeeded": False,
    }
    failure = None
    try:
        measure(args, arm, run_dir, observation)
    except (Exception, KeyboardInterrupt) as error:
        failure = error
        observation["error"] = f"{type(error).__name__}: {error}"
    observation["wall_seconds_total"] = sum(leg["wall_seconds"] for leg in observation["legs"])
    # Before anything that needs room: this is the run a full volume stopped.
    record(root, observation)
    print(json.dumps({key: observation[key] for key in
                      ["run", "arm", "workload", "round", "succeeded", "wall_seconds_total",
                       "uploaded_bytes"]}), flush=True)
    try:
        keep_evidence(run_dir / "registry", root / "evidence" / run_id)
        if (run_dir / "publisher.log").exists():
            shutil.copyfile(run_dir / "publisher.log", root / "evidence" / run_id / "publisher.log")
    except OSError as error:
        failure = failure or error
    if failure:
        print(f"{run_id}: {failure}. Its files stay in {run_dir}", file=sys.stderr, flush=True)
        return 1
    if not args.keep:
        shutil.rmtree(run_dir)
    return 0


def read_observations(root):
    path = Path(root) / "observations.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def command_compare(args):
    """Hold successful runs to one another, tag by tag.

    Runs of one workload to one epoch must hold the same tags. Every run on one
    lineage must agree on each `epoch-N` they both hold, whichever workload it
    came from. `--separate` names a workload that isn't on a shared lineage.
    """
    observations = [o for o in read_observations(args.root) if o["succeeded"]]
    failures = 0
    groups = {}
    for observation in observations:
        groups.setdefault((observation["workload"], observation["until_epoch"]), []).append(
            observation
        )
    for (workload, until), runs in sorted(groups.items()):
        expected = runs[0]["publications"]
        print(f"{workload} to {until}: held to {runs[0]['run']}, {len(expected)} tags")
        for observation in runs[1:]:
            found = observation["publications"]
            wrong = [
                tag for tag in sorted(set(found) | set(expected))
                if found.get(tag, {}).get("manifest") != expected.get(tag, {}).get("manifest")
            ]
            failures += bool(wrong)
            state = "identical" if not wrong else f"DIFFERS at {wrong}"
            print(f"  {observation['run']:<44} {state}")
    seen = {}
    for observation in observations:
        if observation["workload"] in args.separate:
            continue
        for tag, publication in observation["publications"].items():
            if tag != "latest":
                seen.setdefault(tag, {}).setdefault(publication["manifest"], []).append(
                    observation["run"]
                )
    disagreeing = {tag: runs for tag, runs in seen.items() if len(runs) > 1}
    print(f"across every workload: {len(seen)} tags, {len(disagreeing)} disagree")
    for tag, runs in sorted(disagreeing.items()):
        print(f"  {tag}: {runs}")
    failures += len(disagreeing)
    return 1 if failures else 0


def command_restore(args):
    """Restore kept publications with an unchanged consumer, and reproduce them."""
    root = Path(args.root)
    run_dir = root / "runs" / args.run
    work = root / "restores" / args.run
    loopback = Loopback(args.loopback, run_dir / "registry")
    blobs = run_dir / "registry" / REPOSITORY / "blobs" / "sha256"
    tags = run_dir / "registry" / REPOSITORY / "manifests" / "tags"

    def inscription_of(sequence):
        manifest = json.loads((tags / f"epoch-{sequence}").read_bytes())
        return blobs / manifest["config"]["digest"].split(":")[1]

    def restore_one(sequence, line):
        out = work / str(sequence)
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        config = out / "dolos.toml"
        write_config(config, args.network, out / "data")
        log = out / "log"
        began = time.perf_counter()
        with open(log, "wb") as sink:
            restored = subprocess.run(
                [args.consumer, "-c", str(config), "bootstrap", "stelae",
                 "--source", loopback.repository(), "--point", f"epoch-{sequence}",
                 "--insecure", "--skip-space-check"],
                stdout=sink, stderr=sink, check=False,
            )
        line["restore_exit"] = restored.returncode
        line["restore_seconds"] = time.perf_counter() - began
        digest = out / "digest.json"
        command = [args.publisher, "--config", str(config), "digest",
                   "--state-epochs-file", str(args.state_epochs), "--output", str(digest)]
        if (tags / f"epoch-{sequence - 1}").exists():
            command += ["--chain-from", str(inscription_of(sequence - 1))]
        began = time.perf_counter()
        with open(log, "ab") as sink:
            digested = subprocess.run(
                command, stdout=sink, stderr=sink, check=False,
                env=dict(os.environ, NO_COLOR="1"),
            )
        line["digest_exit"] = digested.returncode
        line["digest_seconds"] = time.perf_counter() - began
        line["reproduced"] = (
            restored.returncode == 0 and digested.returncode == 0
            and digest.read_bytes() == inscription_of(sequence).read_bytes()
        )
        return out

    failures = 0
    try:
        for sequence in args.sequence:
            line = {
                "run": args.run, "sequence": sequence, "restore_exit": None, "digest_exit": None,
                "reproduced": False, "restore_seconds": None, "digest_seconds": None,
            }
            try:
                out = restore_one(sequence, line)
            except (Exception, KeyboardInterrupt) as error:
                line["error"] = f"{type(error).__name__}: {error}"
                raise
            finally:
                failures += not line["reproduced"]
                with open(root / "restores.jsonl", "a") as sink:
                    sink.write(json.dumps(line) + "\n")
                print(json.dumps(line), flush=True)
            shutil.copyfile(out / "log", root / "evidence" / args.run / f"restore-{sequence}.log")
            shutil.rmtree(out)
    finally:
        loopback.stop()
    return 1 if failures else 0


NO_PHASES = {"traversals": {}, "capture_seconds": 0, "stage_seconds": 0, "publish_seconds": 0,
             "log_seconds": None, "committed": []}


def fmt_gib(value):
    return f"{value / 1024 ** 3:.2f}"


def command_summarize(args):
    """Tables over every observation, failed ones included, as Markdown."""
    observations = read_observations(args.root)
    print("| Run | Workload | Arm | Round/pos | Exit | Wall s | Replay s | Capture s | Stage s | "
          "Publish s | CPU s | Peak RSS GiB | Batch high-water GiB | Spool high-water GiB | "
          "Stores growth GiB | Uploaded GiB | Traversals tag/exact/entity | Load median |")
    print("|---|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|--:|")
    for o in observations:
        label = o["arm"] + (f" ({o['fault']})" if o["fault"] else "")
        if o.get("error"):
            label += f" (error: {o['error']})"
        if not o["legs"]:
            print(f"| {o['run']} | {o['workload']} | {label} | "
                  f"{o['round']}/{o['position_in_round']} | none |" + " |" * 13)
            continue
        leg = o["legs"][-1]
        t = o["phases"] or NO_PHASES
        counts = t["traversals"]
        cpu = sum(l["user_seconds"] + l["system_seconds"] for l in o["legs"])
        rss = max(l["peak_rss_bytes"] for l in o["legs"])
        batch = max(l["batch_high_water"] for l in o["legs"])
        spool = max(l["high_water"]["spool"] for l in o["legs"])
        growth = max(l["high_water"]["stores"] - o["legs"][0]["stores_at_start"] for l in o["legs"])
        stage_inside = t["stage_seconds"] if o["arm"] == "w1" else 0
        log = t["log_seconds"] or 0
        replay = log - t["capture_seconds"] - t["publish_seconds"]
        replay -= 0 if stage_inside else t["stage_seconds"]
        load = "" if leg["load_median"] is None else f"{leg['load_median']:.1f}"
        traversed = (f"{counts.get('tag', 0)}/{counts.get('exact', 0)}/{counts.get('entity', 0)}"
                     if counts else "not logged")
        print(f"| {o['run']} | {o['workload']} | {label} | {o['round']}/{o['position_in_round']} | "
              f"{leg['exit']} | {o['wall_seconds_total']:.0f} | {replay:.0f} | "
              f"{t['capture_seconds']:.0f} | {t['stage_seconds']:.0f} | {t['publish_seconds']:.0f} | "
              f"{cpu:.0f} | {fmt_gib(rss)} | {fmt_gib(batch)} | {fmt_gib(spool)} | "
              f"{fmt_gib(growth)} | {fmt_gib(o['uploaded_bytes'] or 0)} | {traversed} | {load} |")
    print()
    for workload in sorted({o["workload"] for o in observations}):
        runs = [o for o in observations if o["workload"] == workload and o["succeeded"]
                and not o["fault"]]
        by_arm = {}
        for run in runs:
            by_arm.setdefault(run["arm"], []).append(run)
        if not by_arm:
            continue
        print(f"\n### {workload}\n")
        print("| Arm | Runs | Median wall s | Min | Max | Median CPU s | Peak RSS GiB |")
        print("|---|--:|--:|--:|--:|--:|--:|")
        for arm, arm_runs in sorted(by_arm.items()):
            walls = [r["wall_seconds_total"] for r in arm_runs]
            cpus = [sum(l["user_seconds"] + l["system_seconds"] for l in r["legs"])
                    for r in arm_runs]
            rss = max(l["peak_rss_bytes"] for r in arm_runs for l in r["legs"])
            print(f"| {arm} | {len(arm_runs)} | {statistics.median(walls):.0f} | {min(walls):.0f} | "
                  f"{max(walls):.0f} | {statistics.median(cpus):.0f} | {fmt_gib(rss)} |")
        for baseline, candidate in [pair.split(":") for pair in args.pair]:
            rounds = {}
            for run in runs:
                if run["arm"] in (baseline, candidate) and run["round"]:
                    rounds.setdefault(run["round"], {})[run["arm"]] = run["wall_seconds_total"]
            reductions = []
            for number, walls in sorted(rounds.items()):
                if len(walls) == 2:
                    reductions.append(1 - walls[candidate] / walls[baseline])
            if reductions:
                median = statistics.median(reductions)
                each = ", ".join(f"{r * 100:.1f}%" for r in reductions)
                met = "met" if median >= args.target else "NOT met"
                print(f"\n{candidate} against {baseline}, {len(reductions)} pairs: {each}. Median "
                      f"{median * 100:.1f}%, so the {args.target * 100:.0f}% target is {met}.")


def command_provenance(args):
    """What a reader needs to tell these runs from any others."""
    info = {
        "host": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "binaries": {Path(b).name: sha256_of(b) for b in args.binary},
        "state_epochs": Path(args.state_epochs).read_text().split(),
        "corpus_highest_file": max(
            int(name.split(".")[0]) for name in os.listdir(args.corpus) if name.endswith(".chunk")
        ),
    }
    if sys.platform == "darwin":
        for key, name in [("machdep.cpu.brand_string", "cpu"), ("hw.memsize", "memory_bytes")]:
            info[name] = subprocess.run(
                ["sysctl", "-n", key], capture_output=True, text=True, check=False
            ).stdout.strip()
    print(json.dumps(info, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def shared(sub):
        sub.add_argument("--root", required=True)
        sub.add_argument("--network", default="preprod")
        sub.add_argument("--corpus", required=True, help="an immutable directory")
        sub.add_argument("--state-epochs", required=True)
        sub.add_argument("--loopback", required=True)
        sub.add_argument("--concurrency", default=4, type=int)

    sub = commands.add_parser("prepare")
    shared(sub)
    sub.add_argument("--publisher", required=True)
    sub.add_argument("--base-epoch", required=True, type=int)
    sub.set_defaults(handler=command_prepare)

    sub = commands.add_parser("run")
    shared(sub)
    sub.add_argument("--arm", required=True, help='JSON: {"name","binary","flags"}')
    sub.add_argument("--workload", required=True, help="what compares with what")
    sub.add_argument("--base", default="base/preprod", help="stores and repository to clone")
    sub.add_argument("--from-genesis", action="store_true", help="start with nothing instead")
    sub.add_argument("--until", required=True, type=int)
    sub.add_argument("--first-file", required=True, type=int)
    sub.add_argument("--last-file", required=True, type=int)
    sub.add_argument("--round", default=None, type=int)
    sub.add_argument("--position", default=None, type=int)
    sub.add_argument("--kill-on", default=None, help="a regex over the log")
    sub.add_argument("--kill-nth", default=1, type=int)
    sub.add_argument("--rerun-arm", default=[], action="append", help="JSON, as --arm, after a kill")
    sub.add_argument("--fail-every", default=None, type=int)
    sub.add_argument("--discard-above", default=None, type=int,
                     help="have the registry keep only the size of blobs past this")
    sub.add_argument("--timeout", default=None, type=float)
    sub.add_argument("--keep", action="store_true")
    sub.set_defaults(handler=command_run)

    sub = commands.add_parser("compare")
    sub.add_argument("--root", required=True)
    sub.add_argument("--separate", default=[], action="append")
    sub.set_defaults(handler=command_compare)

    sub = commands.add_parser("restore")
    sub.add_argument("--root", required=True)
    sub.add_argument("--network", default="preprod")
    sub.add_argument("--run", required=True, help="a run kept with --keep")
    sub.add_argument("--sequence", required=True, type=int, action="append")
    sub.add_argument("--consumer", required=True, help="a Dolos binary from before batches")
    sub.add_argument("--publisher", required=True)
    sub.add_argument("--loopback", required=True)
    sub.add_argument("--state-epochs", required=True)
    sub.set_defaults(handler=command_restore)

    sub = commands.add_parser("summarize")
    sub.add_argument("--root", required=True)
    sub.add_argument("--pair", action="append", default=[], help="BASELINE:CANDIDATE")
    sub.add_argument("--target", default=0.25, type=float)
    sub.set_defaults(handler=command_summarize)

    sub = commands.add_parser("provenance")
    sub.add_argument("--corpus", required=True)
    sub.add_argument("--state-epochs", required=True)
    sub.add_argument("--binary", action="append", default=[])
    sub.set_defaults(handler=command_provenance)

    for number in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(number, stop_on_signal)
    args = parser.parse_args()
    sys.exit(args.handler(args) or 0)


if __name__ == "__main__":
    main()
