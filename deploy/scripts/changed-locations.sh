#!/usr/bin/env bash
# Domain code locations a pull request changes: its own folder, or every domain when shared
# code changed. mpo_ops is never previewed. Usage: changed-locations.sh <base commit>
set -euo pipefail
cd "$(dirname "$0")/../.."
changed="$(git diff --name-only "$1"...HEAD)"
mapfile -t domains < <(find code_locations -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | grep -vx mpo_ops | sort)
if grep -qE '^(libs/|containers/|uv\.lock$|pyproject\.toml$)' <<<"$changed"; then
  printf '%s\n' "${domains[@]}"
  exit 0
fi
for location in "${domains[@]}"; do
  if grep -q "^code_locations/$location/" <<<"$changed"; then
    echo "$location"
  fi
done
