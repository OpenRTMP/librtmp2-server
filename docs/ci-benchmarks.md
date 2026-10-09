# CI benchmarks

`.github/workflows/benchmarks.yml` runs the HTTP API microbenchmarks and the
cross-server sweep (`scripts/run_rtmp_benchmarks.sh`: librtmp2-server against
nginx-rtmp, MediaMTX, SRS and LiveForge, same client, same ffmpeg source) on a
GitHub-hosted runner and compares every run with the last release and the last
`main` run.

| Trigger | What happens | Published? |
|---|---|---|
| Merge to `main` | Microbenchmarks + sweep run, the *CI benchmarks* block in [`BENCHMARKS.md`](../BENCHMARKS.md) is rewritten (a `docs(bench)` commit by `github-actions[bot]`), the run is stored as `bench/latest.json` in the same commit | yes — `BENCHMARKS.md` + website "next release" preview |
| Release | `release.yml` calls the workflow in its own job after the GitHub Release exists, so the Docker image never waits for it. Results are attached to the release as `bench-results-<tag>.json` and `BENCHMARKS-<tag>.md`, appended to the release notes, and recorded as `bench/releases/<tag>.json` on `main` | yes — on the release |
| *Actions → Benchmarks → Run workflow* | Runs and shows the comparison in the **job summary** (and log). Nothing is committed, uploaded to a release, or recorded under `bench/`. Inputs: skip the sweep, pick servers, pick viewer counts | no |
| Pull request | Microbenchmarks only, same as a manual run | no |

## What the sweep uses

- **librtmp2-server:** the commit under test, built `--release --locked`.
- **Client:** `bench_handshake` / `bench_relay` from the librtmp2 version in
  `Cargo.lock` (tag `v<version>`).
- **Competitors**, pinned in the workflow's `env:` block so a run only changes
  when we change them, always the newest release: nginx-rtmp (latest nginx
  release plus nginx-rtmp-module, `worker_processes 1`), SRS and LiveForge
  built from source and cached, MediaMTX release binary (checksum-verified).
  If a competitor cannot be built, the sweep skips it and the tables simply
  have no row for it; librtmp2-server's rows are required.
- **Load steps:** 500 / 1000 / 2000 viewers by default.

## Reading the comparison

- 🟢 better, 🔴 worse, ⚪ within the noise band. Latency/CPU/memory: lower is
  better; handshakes per second and frames per viewer: higher is better.
- The noise band is 5 % when both runs used the same CPU model, vCPU count,
  rustc and runner image, 10 % when the toolchain or image changed, and 25 %
  when the hardware differs (the sweep uses at least 10 %, because it runs
  once without repetitions), plus the Criterion confidence intervals for the
  microbenchmarks.
- A *Comparable* line above the tables states which case applies, and the
  header lists CPU model, vCPUs, RAM, kernel, rustc and runner image of the
  run. A GitHub-hosted runner is a shared VM: compare servers **within** a
  run, and look for consistent changes across runs rather than single values.
- The CI numbers are not comparable with the hand-run tables in
  `BENCHMARKS.md` (different machine).

## Data

Results are plain files on `main` (committed by
[`scripts/push-bench-data.sh`](../scripts/push-bench-data.sh)):

```
bench/latest.json            newest run on main, overwritten on every merge
bench/releases/<tag>.json    one file per release, never rewritten
```

openrtmp.org reads `latest.json` (this repo and librtmp2) to offer the newest
CI run next to its release snapshots.

[`scripts/bench_report.py`](../scripts/bench_report.py) collects Criterion
output, fetches the baselines, renders the Markdown and splices it into
`BENCHMARKS.md` / the release notes; [`scripts/bench_sweep.py`](../scripts/bench_sweep.py)
parses the sweep log. Both use only the Python standard library.

## Notes

- The release tag is **not** moved after the benchmark finished: the Docker
  image and the release tarballs are built from the tagged commit, and
  re-pointing a published tag would make the tag disagree with them. The
  release numbers are therefore attached to the GitHub Release (and recorded under
  `bench/releases/` on `main`) instead of living in the tagged source tree.
- Pushing the `BENCHMARKS.md` and `bench/` updates to `main` needs `contents: write` and a
  `main` that accepts pushes from `github-actions[bot]`. If branch protection
  blocks it, the job prints a warning and the numbers are still in the job
  summary and, if the push worked, in `bench/`.
- A sweep takes a while (five servers, three load steps each); a new merge to
  `main` cancels a still-running sweep of the previous merge.
