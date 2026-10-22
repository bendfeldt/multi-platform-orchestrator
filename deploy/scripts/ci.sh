#!/usr/bin/env bash
# Pull-request checks, identical in GitHub Actions and Azure DevOps. No cloud login.
set -euo pipefail
cd "$(dirname "$0")/../.."

uv sync --frozen --all-packages
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
for location in code_locations/*/src/*/definitions.py; do
  module="$(basename "$(dirname "$location")").definitions"
  DAGSTER_HOME="$(mktemp -d)" uv run dagster definitions validate -m "$module"
done
