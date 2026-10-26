#!/usr/bin/env bash
# Removes every preview in dev whose GitHub pull request is no longer open.
#   GH_TOKEN=... preview-cleanup.sh <owner/repo>   (needs `az login` and CONTRACT_URL, as preview.sh)
set -euo pipefail
cd "$(dirname "$0")/../.."
repo="$1"
mkdir -p build
az storage blob download --auth-mode login --blob-url "$CONTRACT_URL" --file build/contract.json --only-show-errors >/dev/null
rg="$(jq -r .resource_group build/contract.json)"
out=build/aca
# shellcheck source=deploy/scripts/lib.sh
source deploy/scripts/lib.sh
for preview in $(previews | tr , ' '); do
  location="${preview%:*}" pr="${preview#*:}"
  state="$(gh api "repos/$repo/pulls/$pr" --jq .state)"
  if [ "$state" != open ]; then
    deploy/scripts/preview.sh remove "$location" "$pr"
  fi
done
