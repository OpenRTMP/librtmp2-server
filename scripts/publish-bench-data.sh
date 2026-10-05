#!/usr/bin/env bash
# Store one benchmark results file on the orphan `bench-data` branch.
#
#   scripts/publish-bench-data.sh <path-on-branch> <local-file>
#
# e.g. `latest.json` (overwritten by every merge to main) or
# `releases/v0.11.0.json` (one immutable file per release). Creates the branch
# on first use. Needs GH_TOKEN with contents: write and GITHUB_REPOSITORY.
set -euo pipefail

dest="${1:?destination path on the bench-data branch}"
src="${2:?local results file}"
: "${GH_TOKEN:?GH_TOKEN is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

url="${GITHUB_SERVER_URL:-https://github.com}/$GITHUB_REPOSITORY"
auth="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)"
work="$(mktemp -d)"
src="$(realpath "$src")"

for attempt in 1 2 3 4; do
  rm -rf "$work/data"
  if git -c http.extraheader="$auth" clone --quiet --depth 1 --branch bench-data "$url" "$work/data" 2>/dev/null; then
    :
  else
    git init --quiet -b bench-data "$work/data"
    git -C "$work/data" remote add origin "$url"
    printf '# bench-data\n\nMachine-written benchmark results. `latest.json` is the newest run on `main`; `releases/<tag>.json` are the release runs. Do not edit by hand.\n' > "$work/data/README.md"
  fi
  mkdir -p "$work/data/$(dirname "$dest")"
  cp "$src" "$work/data/$dest"
  git -C "$work/data" config user.name "github-actions[bot]"
  git -C "$work/data" config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  git -C "$work/data" add -A
  if git -C "$work/data" diff --cached --quiet; then
    echo "bench-data: $dest unchanged"
    exit 0
  fi
  git -C "$work/data" commit --quiet -m "bench: $dest (${GITHUB_SHA:-local})"
  if git -C "$work/data" -c http.extraheader="$auth" push --quiet origin bench-data; then
    echo "bench-data: stored $dest"
    exit 0
  fi
  echo "push raced with another run (attempt $attempt); retrying"
  sleep $((attempt * 3))
done
echo "::warning::could not store $dest on bench-data"
exit 1
