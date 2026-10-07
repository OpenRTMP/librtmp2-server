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


LATENCY = re.compile(r"latency ms.*?: avg=" + NUM + r" p50=" + NUM + r" p95=" + NUM + r" p99=" + NUM)


def _read_line(line: str, cur: dict) -> None:
    """Fold one output line of the current section into `cur`."""
    m = LATENCY.search(line)
    if m:
        cur["avg"], cur["p50"], cur["p95"], cur["p99"] = (float(x) for x in m.groups())
    elif line.startswith("ok="):
        cur["ok"] = int(_kv(line, "ok") or 0)
        cur["failed"] = int(_kv(line, "failed") or 0)
        cur["rate"] = _kv(line, "handshakes_per_s")
    elif line.startswith("connected="):
        cur["received"] = int(_kv(line, "received_frames") or 0)
    elif line.startswith("delivered:"):
        cur["received"] = int(_kv(line, "viewers") or 0)
    elif line.startswith("steady-state"):
        cur["fps"] = _kv(line, "avg_fps_per_player")
    elif line.startswith("server resources:"):
        cur["cpu"] = _kv(line, "cpu_pct")
        cur["rss"] = _kv(line, "peak_rss_mib")


def _handshake_row(cur: dict) -> list | None:
    """Only complete samples: a server that dropped some of the handshakes must
    not be published as an apparently valid result."""
    row = [cur.get(k) for k in ("rate", "avg", "p50", "p95", "p99")]
    complete = None not in row and cur.get("ok", 0) > 0 and cur.get("failed", 0) == 0
    return row if complete else None


def _join_row(cur: dict) -> list | None:
    """Only legs that actually delivered frames: the client prints zeroed
    latency/throughput for a leg in which no viewer received a frame, and that
    must not be published as a valid result."""
    if cur.get("avg") is None or cur.get("fps") is None or cur.get("received", 0) <= 0:
        return None
    return [cur["avg"], cur["p95"], cur["fps"]]


def _load_row(cur: dict) -> list | None:
    row = [cur.get(k) for k in ("avg", "p95", "fps", "cpu", "rss")]
    complete = None not in row and cur.get("received", 0) > 0
    return row if complete else None


def _store(sweep: dict, section: tuple, cur: dict) -> None:
    """File the finished section's row under its phase (and viewer count)."""
    server, kind, players = section
    if kind in ("handshake", "play handshake"):
        row = _handshake_row(cur)
        if row:
            sweep["handshake" if kind == "handshake" else "play_handshake"][server] = row
    elif kind == "relay":
        row = _join_row(cur)
        if row:
            sweep["join"].setdefault(str(players), {})[server] = row
    elif kind == "load":
        row = _load_row(cur)
        if row:
            sweep["load"].setdefault(str(players), {})[server] = row


def _section_of(match: re.Match) -> tuple | None:
    server = SERVER_KEYS.get(match.group(1).lower())
    if not server:
        return None
    players = int(match.group(3)) if match.group(3) else None
    return (server, match.group(2), players)


def parse_log(text: str) -> dict:
    sweep: dict = {"handshake": {}, "play_handshake": {}, "join": {}, "load": {}}
    section = None  # (server, kind, players)
    cur: dict = {}
    for line in text.splitlines():
        header = HEADER.match(line)
        if header or line.startswith(("--- ", "Done.")):
            if section is not None:
                _store(sweep, section, cur)
            section, cur = (_section_of(header) if header else None), {}
        elif section is not None:
            _read_line(line, cur)
    if section is not None:
        _store(sweep, section, cur)
    return sweep


SWEEP_LOG = Path("sweep.log")  # written by the workflow; fixed name, no path on the command line


def _parse_versions(specs: list[str]) -> dict:
    versions = {}
    for spec in specs:
        key, _, rest = spec.partition("=")
        version, detail, language = (rest.split("|") + ["", ""])[:3]
        versions[key] = {"version": version, "detail": detail, "language": language}
    return versions


def cmd_merge(args: argparse.Namespace) -> int:
    from bench_report import RESULTS_FILE, write_file  # same directory

    results = json.loads(RESULTS_FILE.read_text())
    sweep = parse_log(SWEEP_LOG.read_text(errors="replace"))
    sweep["versions"] = _parse_versions(args.version)
    sweep["params"] = {key: value for key, _, value in (p.partition("=") for p in args.param)}
    results["sweep"] = sweep
    write_file(RESULTS_FILE, json.dumps(results, indent=1) + "\n")
    servers = sorted({s for k in ("handshake", "play_handshake") for s in sweep[k]})
    print(f"sweep: servers={servers} join={sorted(sweep['join'])} load={sorted(sweep['load'])}")
    if "openrtmp" not in sweep["handshake"] or not sweep["load"]:
        print("error: librtmp2-server produced no handshake/load rows; see the sweep log", file=sys.stderr)
        return 1
    return 0


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def _viewers(n: str) -> str:
    return f"{n} viewer" if n == "1" else f"{n} viewers"


def _by_viewers(tiers: dict) -> list[tuple[str, dict]]:
    return sorted(tiers.items(), key=lambda kv: int(kv[0]))


def _handshake_metrics(sweep: dict, server: str) -> dict:
    out: dict = {}
    for kind, label in (("handshake", "publish"), ("play_handshake", "play")):
        row = sweep.get(kind, {}).get(server)
        if row:
            out[f"{label} handshakes/s"] = (row[0], False)
            out[f"{label} handshake avg (ms)"] = (row[1], True)
            out[f"{label} handshake p95 (ms)"] = (row[3], True)
    return out


def _join_metrics(sweep: dict, server: str) -> dict:
    out: dict = {}
    for n, rows in _by_viewers(sweep.get("join", {})):
        if server in rows:
            out[f"join, {_viewers(n)}: avg (ms)"] = (rows[server][0], True)
            out[f"join, {_viewers(n)}: p95 (ms)"] = (rows[server][1], True)
    return out


def _load_metrics(sweep: dict, server: str) -> dict:
    out: dict = {}
    for n, rows in _by_viewers(sweep.get("load", {})):
        if server in rows:
            avg, p95, fps, cpu, rss = rows[server]
            out[f"load, {n} viewers: join avg (ms)"] = (avg, True)
            out[f"load, {n} viewers: join p95 (ms)"] = (p95, True)
            out[f"load, {n} viewers: frames/viewer"] = (fps, False)
            out[f"load, {n} viewers: CPU (% of one core)"] = (cpu, True)
            out[f"load, {n} viewers: peak RSS (MiB)"] = (rss, True)
    return out


def metrics(sweep: dict | None, server: str = "openrtmp") -> dict[str, tuple[float, bool]]:
    """name -> (value, lower_is_better) for one server."""
    if not sweep:
        return {}
    return {**_handshake_metrics(sweep, server), **_join_metrics(sweep, server), **_load_metrics(sweep, server)}


def _num(v: float) -> str:
    if v >= 1000:
        return f"{v:,.0f}"
    return f"{v:.1f}" if v >= 100 else f"{v:.2f}"


def _name(key: str) -> str:
    return f"**{SERVER_NAMES[key]}**" if key == "openrtmp" else SERVER_NAMES[key]


def _intro_md(sweep: dict) -> list[str]:
    lines = ["", "#### Cross-server sweep", ""]
    params = sweep.get("params") or {}
    if params:
        lines += [
            "Same RTMP client and ffmpeg source for every server, one server at a time: "
            + ", ".join(f"{k} {v}" for k, v in params.items())
            + ". One sweep per run, no repetitions — the shared runner makes single values noisy, "
            + "so compare servers **within** this run rather than across runs.",
            "",
        ]
    versions = sweep.get("versions") or {}
    present = {k for sec in ("handshake", "play_handshake", "join", "load") for k in _server_keys(sweep.get(sec, {}))}
    parts = [
        f"{SERVER_NAMES[key]} {versions[key]['version']}" + (f" ({versions[key]['detail']})" if versions[key]["detail"] else "")
        for key in ORDER
        if key in versions and key in present
    ]
    if parts:
        lines += ["Servers: " + " · ".join(parts), ""]
    return lines


def _server_keys(rows: dict) -> set:
    """Server keys of {server: row} or of {viewers: {server: row}}."""
    keys: set = set()
    for k, v in rows.items():
        if k in SERVER_NAMES:
            keys.add(k)
        elif isinstance(v, dict):
            keys.update(v)
    return keys


def _baseline_md(cur: dict, bases: list, sweep: dict) -> list[str]:
    from bench_report import comparability, delta_cell  # same directory

    ours = metrics(sweep)
    if not ours or not bases:
        return []
    floors = [max(comparability(cur, b)[1], 0.10) for _, b in bases]
    cols = "".join(f" {label} |" for label, _ in bases)
    lines = [
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
    return lines + [""]


def _handshake_md(sweep: dict) -> list[str]:
    hs, ph = sweep.get("handshake", {}), sweep.get("play_handshake", {})
    keys = [k for k in ORDER if k in hs or k in ph]
    if not keys:
        return []
    lines = [
        "##### Handshake latency",
        "",
        "| Server | publish /s | avg ms | p95 | p99 | play /s | avg ms | p95 | p99 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for k in keys:
        cells: list[str] = []
        for row in (hs.get(k), ph.get(k)):
            cells += [_num(row[0]), _num(row[1]), _num(row[3]), _num(row[4])] if row else ["—"] * 4
        lines.append(f"| {_name(k)} | " + " | ".join(cells) + " |")
    return lines + [""]


def _join_md(sweep: dict) -> list[str]:
    join = sweep.get("join", {})
    if not join:
        return []
    counts = sorted(join, key=int)
    lines = [
        "##### Join latency (avg / p95 ms)",
        "",
        "| Server | " + " | ".join(_viewers(n) for n in counts) + " |",
        "|---|" + "---|" * len(counts),
    ]
    for k in [k for k in ORDER if any(k in join[n] for n in counts)]:
        cells = [f"{_num(join[n][k][0])} / {_num(join[n][k][1])}" if k in join[n] else "—" for n in counts]
        lines.append(f"| {_name(k)} | " + " | ".join(cells) + " |")
    return lines + [""]


def _load_md(sweep: dict) -> list[str]:
    load = sweep.get("load", {})
    if not load:
        return []
    lines = [
        "##### Many viewers on one stream",
        "",
        "| Viewers | Server | join avg / p95 ms | CPU % (one core) | peak RSS MiB | frames/viewer |",
        "|---|---|---|---|---|---|",
    ]
    for n, rows in _by_viewers(load):
        for k in [k for k in ORDER if k in rows]:
            avg, p95, fps, cpu, rss = rows[k]
            lines.append(f"| {n} | {_name(k)} | {_num(avg)} / {_num(p95)} | {_num(cpu)} | {_num(rss)} | {_num(fps)} |")
    return lines + [
        "",
        "A competitor delivering fewer frames per viewer than the others was overloaded at that step "
        + "(the delivered rate, not just latency, is the result).",
    ]


def render_sweep(cur: dict, bases: list) -> list[str]:
    sweep = cur.get("sweep")
    if not sweep:
        return []
    return _intro_md(sweep) + _baseline_md(cur, bases, sweep) + _handshake_md(sweep) + _join_md(sweep) + _load_md(sweep)


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
