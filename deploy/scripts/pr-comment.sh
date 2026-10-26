#!/usr/bin/env bash
# Creates or updates this repo's one change-report comment on a GitHub pull request.
#   GH_TOKEN=... pr-comment.sh <owner/repo> <pull request number> <report.md>
set -euo pipefail
repo="$1"
pr="$2"
marker="<!-- mpo-change-report -->"
body="$(printf '%s\n%s' "$marker" "$(cat "$3")")"
existing="$(gh api --paginate "repos/$repo/issues/$pr/comments" \
  --jq ".[] | select(.body | startswith(\"$marker\")) | .id" | tail -n 1)"
if [ -n "$existing" ]; then
  gh api -X PATCH "repos/$repo/issues/comments/$existing" -f body="$body" >/dev/null
else
  gh api "repos/$repo/issues/$pr/comments" -f body="$body" >/dev/null
fi
