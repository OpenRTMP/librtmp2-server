#!/usr/bin/env python3
"""Cross-server sweep results for the CI benchmarks (librtmp2-server only).

`scripts/run_rtmp_benchmarks.sh` prints human-readable sections. This module

  * `merge`  parses sweep.log into the `sweep` key of results.json (written by
             `bench_report.py collect`), and
  * plugs the cross-server tables plus librtmp2-server's change against the
    last release / last `main` run into `bench_report.py compare`.

The `sweep` layout mirrors the arrays used by openrtmp.org's
`includes/benchmarks-data.php`, so the website can show a CI run next to the
release snapshots:

    handshake / play_handshake : {server: [handshakes/s, avg, p50, p95, p99]}
    join   : {viewers: {server: [avg ms, p95 ms, frames/viewer]}}
    load   : {viewers: {server: [join avg ms, join p95 ms, frames/viewer, CPU %, peak RSS MiB]}}
    versions: {server: {"version", "detail", "language"}}
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Section labels used by run_rtmp_benchmarks.sh -> website server keys.
SERVER_KEYS = {
    "librtmp2-server": "openrtmp",
    "lrtmp2-server": "openrtmp",
    "nginx-rtmp": "nginx",
    "nginx": "nginx",
    "mediamtx": "mediamtx",
    "srs": "srs",
    "liveforge": "liveforge",
}
SERVER_NAMES = {
    "openrtmp": "librtmp2-server",
    "mediamtx": "MediaMTX",
    "liveforge": "LiveForge",
    "srs": "SRS",
    "nginx": "nginx-rtmp",
}
ORDER = ["openrtmp", "mediamtx", "liveforge", "srs", "nginx"]

HEADER = re.compile(r"^=== (\S+) (play handshake|handshake|relay|load)(?:, players=(\d+))?")
NUM = r"([0-9]+(?:\.[0-9]+)?)"


def _kv(line: str, key: str) -> float | None:
    m = re.search(rf"\b{key}={NUM}", line)
    return float(m.group(1)) if m else None


def parse_log(text: str) -> dict:
    sweep: dict = {"handshake": {}, "play_handshake": {}, "join": {}, "load": {}}
    section = None  # (server, kind, players)
    cur: dict = {}

    def flush() -> None:
        nonlocal section, cur
        if section is None:
            return
        server, kind, players = section
        if kind in ("handshake", "play handshake"):
            row = [cur.get(k) for k in ("rate", "avg", "p50", "p95", "p99")]
            # Only complete samples: a server that dropped some of the handshakes must
            # not be published as an apparently valid result.
            if None not in row and cur.get("ok", 0) > 0 and cur.get("failed", 0) == 0:
                target = "handshake" if kind == "handshake" else "play_handshake"
                sweep[target][server] = row
        elif kind == "relay":
            if cur.get("avg") is not None and cur.get("fps") is not None:
                sweep["join"].setdefault(str(players), {})[server] = [cur["avg"], cur["p95"], cur["fps"]]
        elif kind == "load":
            need = ("avg", "p95", "fps", "cpu", "rss")
            if all(cur.get(k) is not None for k in need):
                sweep["load"].setdefault(str(players), {})[server] = [cur[k] for k in need]
        section, cur = None, {}

    for line in text.splitlines():
        m = HEADER.match(line)
        if m:
            flush()
            server = SERVER_KEYS.get(m.group(1).lower())
            if server:
                kind = m.group(2)
                section = (server, kind, int(m.group(3)) if m.group(3) else None)
            continue
        if line.startswith("--- ") or line.startswith("Done."):
            flush()
            continue
        if section is None:
            continue
        if m := re.search(r"latency ms.*?: avg=" + NUM + r" p50=" + NUM + r" p95=" + NUM + r" p99=" + NUM, line):
            cur["avg"], cur["p50"], cur["p95"], cur["p99"] = (float(x) for x in m.groups())
        elif line.startswith("ok="):
            cur["ok"] = int(_kv(line, "ok") or 0)
            cur["failed"] = int(_kv(line, "failed") or 0)
            cur["rate"] = _kv(line, "handshakes_per_s")
        elif line.startswith("steady-state"):
            cur["fps"] = _kv(line, "avg_fps_per_player")
        elif line.startswith("server resources:"):
            cur["cpu"] = _kv(line, "cpu_pct")
            cur["rss"] = _kv(line, "peak_rss_mib")
    flush()
    return sweep


SWEEP_LOG = Path("sweep.log")  # written by the workflow; fixed name, no path on the command line


def cmd_merge(args: argparse.Namespace) -> int:
    from bench_report import RESULTS_FILE  # same directory

    results = json.loads(RESULTS_FILE.read_text())
    sweep = parse_log(SWEEP_LOG.read_text(errors="replace"))
    versions = {}
    for spec in args.version:
        key, _, rest = spec.partition("=")
        version, detail, language = (rest.split("|") + ["", ""])[:3]
        versions[key] = {"version": version, "detail": detail, "language": language}
    sweep["versions"] = versions
    sweep["params"] = dict(p.split("=", 1) for p in args.param)
    results["sweep"] = sweep
    RESULTS_FILE.write_text(json.dumps(results, indent=1) + "\n")
    servers = sorted({s for k in ("handshake", "play_handshake") for s in sweep[k]})
    print(
        f"sweep: servers={servers} join={sorted(sweep['join'])} load={sorted(sweep['load'])}"
    )
    if "openrtmp" not in sweep["handshake"] or not sweep["load"]:
        print("error: librtmp2-server produced no handshake/load rows; see the sweep log", file=sys.stderr)
        return 1
    return 0


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def metrics(sweep: dict | None, server: str = "openrtmp") -> dict[str, tuple[float, bool]]:
    """name -> (value, lower_is_better) for one server."""
    out: dict[str, tuple[float, bool]] = {}
    if not sweep:
        return out
    for kind, label in (("handshake", "publish"), ("play_handshake", "play")):
        row = sweep.get(kind, {}).get(server)
        if row:
            out[f"{label} handshakes/s"] = (row[0], False)
            out[f"{label} handshake avg (ms)"] = (row[1], True)
            out[f"{label} handshake p95 (ms)"] = (row[3], True)
    for n, rows in sorted(sweep.get("join", {}).items(), key=lambda kv: int(kv[0])):
        if server in rows:
            out[f"join, {n} viewer{'' if n == '1' else 's'}: avg (ms)"] = (rows[server][0], True)
            out[f"join, {n} viewer{'' if n == '1' else 's'}: p95 (ms)"] = (rows[server][1], True)
    for n, rows in sorted(sweep.get("load", {}).items(), key=lambda kv: int(kv[0])):
        if server in rows:
            avg, p95, fps, cpu, rss = rows[server]
            out[f"load, {n} viewers: join avg (ms)"] = (avg, True)
            out[f"load, {n} viewers: join p95 (ms)"] = (p95, True)
            out[f"load, {n} viewers: frames/viewer"] = (fps, False)
            out[f"load, {n} viewers: CPU (% of one core)"] = (cpu, True)
            out[f"load, {n} viewers: peak RSS (MiB)"] = (rss, True)
    return out


def _num(v: float) -> str:
    return f"{v:,.0f}" if v >= 1000 else (f"{v:.1f}" if v >= 100 else f"{v:.2f}")


def render_sweep(cur: dict, bases: list) -> list[str]:
    from bench_report import comparability, delta_cell  # same directory

    sweep = cur.get("sweep")
    if not sweep:
        return []
    lines = ["", "#### Cross-server sweep", ""]
    params = sweep.get("params") or {}
    if params:
        lines.append(
            "Same RTMP client and ffmpeg source for every server, one server at a time: "
            + ", ".join(f"{k} {v}" for k, v in params.items())
            + ". One sweep per run, no repetitions — the shared runner makes single values noisy, "
            "so compare servers **within** this run rather than across runs."
        )
        lines.append("")
    versions = sweep.get("versions") or {}
    if versions:
        parts = []
        present = {k for sec in ("handshake", "play_handshake") for k in sweep.get(sec, {})}
        for key in ORDER:
            v = versions.get(key)
            if v and key in present:
                parts.append(f"{SERVER_NAMES[key]} {v['version']}" + (f" ({v['detail']})" if v["detail"] else ""))
        lines += ["Servers: " + " · ".join(parts), ""]

    # librtmp2-server against the baselines
    ours = metrics(sweep)
    if ours and bases:
        floors = [max(comparability(cur, b)[1], 0.10) for _, b in bases]
        cols = "".join(f" {label} |" for label, _ in bases)
        lines += [
            "##### librtmp2-server vs the baselines",
            "",
            f"| Metric | Now |{cols}",
            f"|---|---|{'---|' * len(bases)}",
        ]
        base_metrics = [metrics(b.get("sweep")) for _, b in bases]
        for name, (value, lower) in ours.items():
            cells = ""
            for bm, floor in zip(base_metrics, floors):
                old = bm.get(name)
                cells += f" {delta_cell(value, old[0] if old else None, floor, lower)} |"
            lines.append(f"| {name} | {_num(value)} |{cells}")
        lines.append("")

    # cross-server tables
    hs, ph = sweep.get("handshake", {}), sweep.get("play_handshake", {})
    keys = [k for k in ORDER if k in hs or k in ph]
    if keys:
        lines += [
            "##### Handshake latency",
            "",
            "| Server | publish /s | avg ms | p95 | p99 | play /s | avg ms | p95 | p99 |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for k in keys:
            a, b = hs.get(k), ph.get(k)
            fa = [_num(a[0]), _num(a[1]), _num(a[3]), _num(a[4])] if a else ["—"] * 4
            fb = [_num(b[0]), _num(b[1]), _num(b[3]), _num(b[4])] if b else ["—"] * 4
            name = f"**{SERVER_NAMES[k]}**" if k == "openrtmp" else SERVER_NAMES[k]
            lines.append(f"| {name} | " + " | ".join(fa + fb) + " |")
        lines.append("")
    join = sweep.get("join", {})
    if join:
        counts = sorted(join, key=int)
        lines += [
            "##### Join latency (avg / p95 ms)",
            "",
            "| Server | " + " | ".join(f"{n} viewer" + ("" if n == "1" else "s") for n in counts) + " |",
            "|---|" + "---|" * len(counts),
        ]
        for k in [k for k in ORDER if any(k in join[n] for n in counts)]:
            cells = [f"{_num(join[n][k][0])} / {_num(join[n][k][1])}" if k in join[n] else "—" for n in counts]
            name = f"**{SERVER_NAMES[k]}**" if k == "openrtmp" else SERVER_NAMES[k]
            lines.append(f"| {name} | " + " | ".join(cells) + " |")
        lines.append("")
    load = sweep.get("load", {})
    if load:
        lines += [
            "##### Many viewers on one stream",
            "",
            "| Viewers | Server | join avg / p95 ms | CPU % (one core) | peak RSS MiB | frames/viewer |",
            "|---|---|---|---|---|---|",
        ]
        for n in sorted(load, key=int):
            for k in [k for k in ORDER if k in load[n]]:
                avg, p95, fps, cpu, rss = load[n][k]
                name = f"**{SERVER_NAMES[k]}**" if k == "openrtmp" else SERVER_NAMES[k]
                lines.append(f"| {n} | {name} | {_num(avg)} / {_num(p95)} | {_num(cpu)} | {_num(rss)} | {_num(fps)} |")
        lines += [
            "",
            "A competitor delivering fewer frames per viewer than the others was overloaded at that step "
            + "(the delivered rate, not just latency, is the result).",
        ]
    return lines


def register(report_module) -> None:
    report_module.EXTRA_SECTIONS.append(render_sweep)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("merge")
    p.add_argument("--version", action="append", default=[], metavar="KEY=VERSION|DETAIL|LANGUAGE")
    p.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    p.set_defaults(fn=cmd_merge)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
