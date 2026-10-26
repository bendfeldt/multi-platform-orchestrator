#!/usr/bin/env bash
# What a pull request changes in the Dagster definitions (deploy/change_report.py), identical in
# GitHub Actions and Azure DevOps. No cloud login: only definitions are loaded.
#   change-report.sh <checkout of the base branch> <report.md>
set -euo pipefail
cd "$(dirname "$0")/../.."
repo="$PWD"
base="$(cd "$1" && pwd)"
out="$2"
work="$(mktemp -d)"

uv sync --frozen --all-packages --all-extras
# The base branch is loaded by its own packages, in its own environment.
(cd "$base" && uv sync --frozen --all-packages --all-extras &&
  DAGSTER_HOME="$(mktemp -d)" uv run python "$repo/deploy/change_report.py" snapshot \
    --repo "$base" --out "$work/base.json")
DAGSTER_HOME="$(mktemp -d)" uv run python deploy/change_report.py snapshot --repo "$repo" \
  --out "$work/head.json"
uv run python deploy/change_report.py diff "$work/base.json" "$work/head.json" >"$out"
