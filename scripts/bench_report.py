#!/usr/bin/env python3
"""Collect, compare and publish CI benchmark results.

Used by .github/workflows/benchmarks.yml. Standard library only, so it runs on
a stock GitHub runner without installing anything.

Sub-commands
------------
collect     Criterion output (crit-protocol, crit-relay, crit-http) -> results.json
            (with the hardware/software environment of the machine that ran it).
baselines   Fetch the comparison baselines (last release, last `main` run)
            from `bench/` on `main` into baselines/.
compare     results.json (+ deltas against baselines/) -> comparison.md.
splice      Replace a marker-delimited block in BENCHMARKS.md (`ci`) or in a
            GitHub release body, body.md (`release`) with generated Markdown.

All file names are fixed (see the constants below); the command line carries
no paths.

Results are stored on `main`, under bench/:
    bench/latest.json            newest run on `main` (overwritten on every merge)
    bench/releases/<tag>.json    one immutable file per release

This file is kept byte-identical in OpenRTMP/librtmp2 and
OpenRTMP/librtmp2-server (the latter adds scripts/bench_sweep.py on top).
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path

SCHEMA = 1
DATA_REF = "main"
DATA_DIR = "bench"

CI_START = "<!-- ci-bench:start -->"
CI_END = "<!-- ci-bench:end -->"
RELEASE_START = "<!-- bench-release:start -->"
RELEASE_END = "<!-- bench-release:end -->"


# Fixed file names. The workflow and this script agree on them, so no file path
# ever comes from the command line (nothing to traverse, nothing to validate).
RESULTS_FILE = Path("results.json")
COMPARISON_FILE = Path("comparison.md")
BASELINE_DIR = Path("baselines")
CARGO_TOML = Path("Cargo.toml")
CARGO_LOCK = Path("Cargo.lock")
# Criterion output of each suite, moved aside by the workflow after its bench run.
SUITE_DIRS = {"protocol": Path("crit-protocol"), "relay": Path("crit-relay"), "http_api": Path("crit-http")}
# splice target -> (file to update, generated fragment, start marker, end marker)
def write_file(path: Path, content: str) -> None:
    """Write generated text to one of the fixed output files."""
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


SPLICE_TARGETS = {
    "ci": (Path("BENCHMARKS.md"), Path("ci-block.md"), CI_START, CI_END),
    "release": (Path("body.md"), Path("release-block.md"), RELEASE_START, RELEASE_END),
}


# --------------------------------------------------------------------------
# environment
# --------------------------------------------------------------------------

def _read(path: str) -> str:
    try:
        return Path(path).read_text(errors="replace")
    except OSError:
        return ""


def _run(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=False).stdout.strip()
    except OSError:
        return ""


def _cpu_model() -> str:
    for line in _read("/proc/cpuinfo").splitlines():
        if line.lower().startswith("model name"):
            model = line.split(":", 1)[1].strip()
            return re.sub(r"\s+", " ", model)
    return platform.processor() or "unknown CPU"


def _mem_gib() -> float:
    for line in _read("/proc/meminfo").splitlines():
        if line.startswith("MemTotal:"):
            return round(int(line.split()[1]) / 1048576, 1)
    return 0.0


def _vcpus() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count() or 0


def _runner() -> tuple[str, bool]:
    """(description, is a shared CI VM)"""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return "local machine", False
    hosted = os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
    runner = ("GitHub-hosted " if hosted else "self-hosted ") + (os.environ.get("ImageOS") or "runner")
    version = os.environ.get("ImageVersion")
    if version:
        runner += f" (image {version})"
    return runner, True


def environment() -> dict:
    """Describe the machine that produced the numbers."""
    runner, shared = _runner()
    rustc = _run("rustc", "--version")
    m = re.match(r"rustc (\S+)", rustc)
    return {
        "cpu": _cpu_model(),
        "vcpus": _vcpus(),
        "ram_gib": _mem_gib(),
        "kernel": f"{platform.system()} {platform.release()} {platform.machine()}",
        "rustc": m.group(1) if m else rustc,
        "runner": runner,
        "shared_vm": shared,
    }


def env_line(env: dict) -> str:
    return (
        f"{env['cpu']} · {env['vcpus']} vCPUs · {env['ram_gib']} GiB RAM · "
        f"{env['kernel']} · rustc {env['rustc']} · {env['runner']}"
    )


# --------------------------------------------------------------------------
# comparability of two runs
# --------------------------------------------------------------------------

def comparability(cur: dict, base: dict) -> tuple[str, float, str]:
    """(level, noise floor, explanation) for comparing two runs' environments."""
    a, b = cur["environment"], base["environment"]
    if a["cpu"] != b["cpu"] or a["vcpus"] != b["vcpus"]:
        return (
            "low",
            0.25,
            f"different hardware ({b['cpu']}, {b['vcpus']} vCPUs vs {a['cpu']}, "
            f"{a['vcpus']} vCPUs): only large changes (> 25 %) mean anything",
        )
    drift = [
        name
        for name, key in (("rustc", "rustc"), ("kernel", "kernel"), ("runner image", "runner"))
        if a[key] != b[key]
    ]
    if drift:
        return (
            "medium",
            0.10,
            f"same CPU model and vCPU count, but {' / '.join(drift)} changed: "
            "differences under 10 % may come from the toolchain or host, not the code",
        )
    return (
        "good",
        0.05,
        "same CPU model, vCPU count, rustc and runner image; shared-VM noise of a few percent remains",
    )


# --------------------------------------------------------------------------
# criterion
# --------------------------------------------------------------------------

def read_criterion(root: Path) -> dict:
    """Every benchmark below a `target/criterion` directory, keyed by full id."""
    out: dict = {}
    for bench_json in sorted(root.rglob("new/benchmark.json")):
        est_json = bench_json.with_name("estimates.json")
        if not est_json.exists():
            continue
        meta = json.loads(bench_json.read_text())
        est = json.loads(est_json.read_text())
        # Criterion's printed middle value: the slope if it has one, else the mean.
        point = est.get("slope") or est["mean"]
        ns = float(point["point_estimate"])
        lo = float(point["confidence_interval"]["lower_bound"])
        hi = float(point["confidence_interval"]["upper_bound"])
        thr = meta.get("throughput") or {}
        out[meta["full_id"]] = {
            "ns": ns,
            "ci_rel": ((hi - lo) / 2 / ns) if ns else 0.0,
            "bytes": thr.get("Bytes"),
            "elements": thr.get("Elements"),
        }
    return out


def cargo_version(cargo_toml: Path) -> str:
    for line in cargo_toml.read_text().splitlines():
        m = re.match(r'\s*version\s*=\s*"([^"]+)"', line)
        if m:
            return m.group(1)
    return "unknown"


def lock_version(cargo_lock: Path, package: str) -> str | None:
    if not cargo_lock.exists():
        return None
    text = cargo_lock.read_text()
    m = re.search(rf'name = "{re.escape(package)}"\nversion = "([^"]+)"', text)
    return m.group(1) if m else None


def cmd_collect(args: argparse.Namespace) -> None:
    suites = {}
    for name, directory in SUITE_DIRS.items():
        if name not in args.suite:
            continue
        suites[name] = read_criterion(directory)
        if not suites[name]:
            print(f"warning: no Criterion results under {directory}", file=sys.stderr)
    repo = os.environ.get("GITHUB_REPOSITORY", args.repo)
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_id = os.environ.get("GITHUB_RUN_ID")
    result = {
        "schema": SCHEMA,
        "repo": repo,
        "kind": args.kind,
        "version": cargo_version(CARGO_TOML),
        "tag": args.tag or None,
        # The checked-out commit, not GITHUB_SHA: in a reusable workflow GITHUB_SHA is the
        # caller's, which differs from the benchmarked tag on a manual release run.
        "commit": _run("git", "rev-parse", "HEAD") or os.environ.get("GITHUB_SHA", ""),
        "date": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_url": f"{server}/{repo}/actions/runs/{run_id}" if run_id else None,
        "deps": {},
        "environment": environment(),
        "suites": suites,
    }
    lib = lock_version(CARGO_LOCK, "librtmp2")
    if lib and result["repo"].endswith("-server"):
        result["deps"]["librtmp2"] = lib
    write_file(RESULTS_FILE, json.dumps(result, indent=1) + "\n")
    print(f"wrote {RESULTS_FILE}: {sum(len(s) for s in suites.values())} benchmarks")


# --------------------------------------------------------------------------
# baselines (bench/ on main)
# --------------------------------------------------------------------------

def _version_key(name: str) -> tuple:
    """Sort key following semver precedence: 1.0.0-rc.1 < 1.0.0 < 1.0.1."""
    core, _, pre = name.lstrip("v").partition("-")
    nums = tuple(int(p) if p.isdigit() else 0 for p in core.split("."))
    if not pre:
        return (nums, 1, ())
    # Numeric prerelease identifiers sort below alphanumeric ones.
    ids = tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in re.split(r"[.\-]", pre))
    return (nums, 0, ids)


def cmd_baselines(args: argparse.Namespace) -> None:
    """Write baselines/previous.json (last main run) and baselines/release.json (last release)."""
    out = BASELINE_DIR
    out.mkdir(parents=True, exist_ok=True)
    ref = f"origin/{DATA_REF}"
    # Public repositories: no credentials needed. A failed fetch is an error,
    # not "no data yet" (the data files are simply absent until the first run).
    for attempt in range(1, 4):
        fetch = subprocess.run(
            ["git", "fetch", "--quiet", "--depth", "1", "origin", f"{DATA_REF}:refs/remotes/{ref}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if fetch.returncode == 0:
            break
        if attempt == 3:
            sys.exit(f"could not fetch {DATA_REF} for the baselines: {fetch.stderr.strip()}")
        time.sleep(attempt * 3)

    def show(path: str) -> str | None:
        p = subprocess.run(
            ["git", "show", f"{ref}:{path}"], capture_output=True, text=True, encoding="utf-8"
        )
        return p.stdout if p.returncode == 0 else None

    prev = show(f"{DATA_DIR}/latest.json")
    if prev and not args.skip_previous:
        write_file(out / "previous.json", prev)
        print("baseline: previous main run")
    names = _run("git", "ls-tree", "--name-only", ref, f"{DATA_DIR}/releases/").splitlines()
    tags = sorted(
        (Path(n).stem for n in names if n.endswith(".json")),
        key=_version_key,
    )
    tags = [t for t in tags if t != (args.exclude_tag or "")]
    if tags:
        text = show(f"{DATA_DIR}/releases/{tags[-1]}.json")
        if text:
            write_file(out / "release.json", text)
            print(f"baseline: last release {tags[-1]}")


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def fmt_time(ns: float) -> str:
    if ns < 1e3:
        return f"{ns:.2f} ns" if ns < 10 else f"{ns:.0f} ns"
    if ns < 1e6:
        return f"{ns / 1e3:.2f} µs"
    if ns < 1e9:
        return f"{ns / 1e6:.1f} ms"
    return f"{ns / 1e9:.2f} s"


def fmt_throughput(b: dict) -> str:
    if b.get("bytes"):
        mib = b["bytes"] / (b["ns"] * 1e-9) / 1048576
        return f"~{mib:.0f} MiB/s"
    if b.get("elements"):
        return f"~{b['elements'] / (b['ns'] * 1e-9):.0f} elem/s"
    return "—"


def delta_cell(cur: float, base: float | None, noise: float, lower_better: bool = True) -> str:
    """e.g. `🟢 −12.3 %`, `🔴 +12.3 %`, `⚪ +1.2 %`."""
    if base is None or base == 0:
        return "—"
    change = cur / base - 1
    text = f"{change * 100:+.1f} %".replace("-", "−")
    if abs(change) <= noise:
        return f"⚪ {text}"
    better = (change < 0) == lower_better
    return f"{'🟢' if better else '🔴'} {text}"


def load_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def ref_name(r: dict) -> str:
    return r.get("tag") or f"v{r['version']}"


def baseline_label(kind: str, base: dict) -> str:
    when = base["date"][:10]
    if kind == "release":
        return f"vs release {ref_name(base)}"
    return f"vs previous run ({when})"


def header_md(cur: dict, extra_bases: list[tuple[str, dict]]) -> list[str]:
    env = cur["environment"]
    run = f" · [workflow run]({cur['run_url']})" if cur.get("run_url") else ""
    lines = [
        f"- **Measured:** {cur['date'].replace('T', ' ').replace('Z', ' UTC')} · "
        f"`{cur['commit'][:7]}` · crate version {cur['version']}{run}",
    ]
    if cur.get("deps"):
        deps = ", ".join(f"{k} {v}" for k, v in cur["deps"].items())
        lines.append(f"- **Built on:** {deps}")
    lines += [
        f"- **CPU:** {env['cpu']} · {env['vcpus']} vCPUs",
        f"- **RAM:** {env['ram_gib']} GiB",
        f"- **System:** {env['kernel']} · rustc {env['rustc']} · {env['runner']}",
    ]
    if env.get("shared_vm"):
        lines.append(
            "- **Noise:** shared CI VM — CPU model, neighbouring load and the runner "
            "image change between runs, so treat absolute numbers as indicative."
        )
    for label, base in extra_bases:
        level, _floor, why = comparability(cur, base)
        icon = {"good": "✅", "medium": "⚠️", "low": "❌"}[level]
        lines.append(f"- **Comparable {label}:** {icon} {why}")
    return lines


def _suite_row(suite: str, name: str, b: dict, bases: list[tuple[str, dict]], floors: list[float]) -> str:
    cells = ""
    for (_, base), floor in zip(bases, floors):
        old = base["suites"].get(suite, {}).get(name)
        noise = floor + b["ci_rel"] + (old["ci_rel"] if old else 0)
        cells += f" {delta_cell(b['ns'], old['ns'] if old else None, noise)} |"
    return f"| `{name}` | {fmt_time(b['ns'])} | {fmt_throughput(b)} |{cells}"


def suite_tables_md(cur: dict, bases: list[tuple[str, dict]]) -> list[str]:
    """One table per suite; one delta column per baseline."""
    lines: list[str] = []
    floors = [comparability(cur, b)[1] for _, b in bases]
    cols = "".join(f" {label} |" for label, _ in bases)
    for suite, benches in cur["suites"].items():
        if not benches:
            continue
        lines += [
            "",
            f"#### `{suite}`",
            "",
            f"| Benchmark | Time | Throughput |{cols}",
            f"|---|---|---|{'---|' * len(bases)}",
        ]
        lines += [_suite_row(suite, name, b, bases, floors) for name, b in sorted(benches.items())]
    return lines


def legend_md() -> list[str]:
    text = (
        "🟢 faster · 🔴 slower · ⚪ within the noise band (5–25 % depending on how "
        + "comparable the two environments are, plus both runs' Criterion confidence "
        + "intervals). Lower time is better."
    )
    return ["", text]


def render(cur: dict, release: dict | None, previous: dict | None, heading: str) -> str:
    bases: list[tuple[str, dict]] = []
    if release:
        bases.append((baseline_label("release", release), release))
    if previous:
        bases.append((baseline_label("main", previous), previous))
    lines = [heading, ""]
    lines += header_md(cur, bases)
    if not bases:
        lines += ["", "_No baseline yet (no release or previous `main` run has been recorded)._"]
    lines += suite_tables_md(cur, bases)
    lines += legend_md()
    for extra in EXTRA_SECTIONS:
        lines += extra(cur, bases)
    return "\n".join(lines) + "\n"


# Server-side sections (cross-server sweep) register themselves here.
EXTRA_SECTIONS: list = []


def cmd_compare(args: argparse.Namespace) -> None:
    cur = json.loads(RESULTS_FILE.read_text())
    release = load_json(BASELINE_DIR / "release.json")
    previous = load_json(BASELINE_DIR / "previous.json")
    if args.mode == "release":
        previous = None  # a release is compared to the last release only
    if release and release.get("tag") == cur.get("tag") and cur.get("tag"):
        release = None
    heading = args.heading or f"### CI benchmarks — {cur['repo'].split('/')[-1]}"
    write_file(COMPARISON_FILE, render(cur, release, previous, heading))
    print(f"wrote {COMPARISON_FILE}")


# --------------------------------------------------------------------------
# splice
# --------------------------------------------------------------------------

def _separator(text: str) -> str:
    """What to put between existing text and an appended block (one blank line)."""
    if not text or text.endswith("\n\n"):
        return ""
    return "\n" if text.endswith("\n") else "\n\n"


def splice_text(text: str, fragment: str, start: str, end: str) -> str:
    block = f"{start}\n{fragment.rstrip()}\n{end}"
    if start in text and end in text:
        head, rest = text.split(start, 1)
        _old, tail = rest.split(end, 1)
        return head + block + tail
    return text + _separator(text) + block + "\n"


def cmd_splice(args: argparse.Namespace) -> int:
    for target, (path, fragment, start, end) in SPLICE_TARGETS.items():
        if target != args.target:
            continue
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        if target == "ci" and (start not in text or end not in text):
            print(f"error: {path} has no {start} … {end} block", file=sys.stderr)
            return 1
        write_file(
            path, splice_text(text, fragment.read_text(encoding="utf-8"), start, end)
        )
    return 0


# Optional: the cross-server sweep, only present in librtmp2-server.
if importlib.util.find_spec("bench_sweep") is not None:
    import bench_sweep  # noqa: E402

    bench_sweep.register(sys.modules[__name__])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("collect", help="Criterion output (crit-*) -> results.json")
    p.add_argument("--suite", action="append", default=[], choices=sorted(SUITE_DIRS))
    p.add_argument("--kind", required=True, choices=["main", "release", "manual", "pr"])
    p.add_argument("--tag", default="")
    p.add_argument("--repo", default="OpenRTMP/librtmp2")
    p.set_defaults(fn=cmd_collect)

    p = sub.add_parser("baselines", help="fetch baselines/previous.json and baselines/release.json")
    p.add_argument("--exclude-tag", default="")
    p.add_argument("--skip-previous", action="store_true")
    p.set_defaults(fn=cmd_baselines)

    p = sub.add_parser("compare", help="results.json + baselines/ -> comparison.md")
    p.add_argument("--mode", choices=["main", "release", "manual", "pr"], default="manual")
    p.add_argument("--heading")
    p.set_defaults(fn=cmd_compare)

    p = sub.add_parser("splice", help="insert the generated fragment into BENCHMARKS.md or body.md")
    p.add_argument("--target", choices=sorted(SPLICE_TARGETS), required=True)
    p.set_defaults(fn=cmd_splice)

    args = ap.parse_args()
    return args.fn(args) or 0


if __name__ == "__main__":
    sys.exit(main())
