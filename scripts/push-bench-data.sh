#!/usr/bin/env bash
# Commit benchmark result files in the current checkout and push them to main.
#
#   scripts/push-bench-data.sh "<commit message>" <path>...
#
# Run from the root of a checkout of `main` (benchmarks.yml: latest.json goes in
# with the BENCHMARKS.md update, a release adds bench/releases/<tag>.json).
# Needs GH_TOKEN with contents: write. If another run pushed first, our commit
# is replayed on top of the new main. A push that cannot succeed (branch
# protection, repeated races) only warns: the numbers are still in the job
# summary.
set -euo pipefail

msg="${1:?commit message}"
shift
: "${GH_TOKEN:?GH_TOKEN is required}"

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add -- "$@"
if git diff --cached --quiet; then
  echo "bench data unchanged"
  exit 0
fi
git commit --quiet -m "$msg"

auth="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)"
for attempt in 1 2 3 4; do
  if git -c http.extraheader="$auth" push --quiet origin HEAD:main; then
    echo "bench data pushed to main"
    exit 0
  fi
  echo "push rejected (attempt $attempt); replaying on the new main"
  if ! git -c http.extraheader="$auth" pull --quiet --rebase origin main; then
    git rebase --abort || true
    break
  fi
  sleep $((attempt * 3))
done
echo "::warning::could not push the benchmark data to main (branch protection?). The results are still in the job summary."
