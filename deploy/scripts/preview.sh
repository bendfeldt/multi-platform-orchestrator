#!/usr/bin/env bash
# Adds or removes one pull request's preview of a code location in dev
# (docs/PRO_FEATURES_PLAN.md §7.2, orchestrator_core.preview). Other apps keep their images;
# only the preview's code server and the readers of the workspace file change.
#   preview.sh deploy <location> <pr> <images.json of the pull request's build>
#   preview.sh remove <location> <pr>
# Needs `az login` as dev's app deployer and CONTRACT_URL (dev's contract).
set -euo pipefail
cd "$(dirname "$0")/../.."
action="$1" location="$2" pr="$3"
out="build/aca"
name="cl-${location//_/-}-pr$pr"

mkdir -p build
az storage blob download --auth-mode login --blob-url "$CONTRACT_URL" --file build/contract.json --only-show-errors >/dev/null
if [ "$(jq -r .environment build/contract.json)" != dev ]; then
  echo "previews run in dev only" >&2
  exit 1
fi
rg="$(jq -r .resource_group build/contract.json)"
# shellcheck source=deploy/scripts/lib.sh
source deploy/scripts/lib.sh

others="$(previews | tr , '\n' | grep -vx "$location:$pr" | paste -sd, - || true)"
running_images >build/images.json
case "$action" in
  deploy)
    jq --arg k "${location}_pr$pr" --slurpfile pr "$4" --arg loc "$location" \
      '. + {($k): $pr[0][$loc]}' build/images.json >build/images.next.json
    mv build/images.next.json build/images.json
    wanted="${others:+$others,}$location:$pr"
    ;;
  remove)
    az containerapp delete --name "$name" --resource-group "$rg" --yes --only-show-errors >/dev/null 2>&1 || true
    echo "removed $name"
    wanted="$others"
    ;;
  *)
    echo "usage: preview.sh deploy|remove <location> <pr> [images.json]" >&2
    exit 2
    ;;
esac
rm -rf "$out"
uv run --frozen --all-packages python deploy/render.py --contract build/contract.json \
  --images build/images.json --previews "$wanted" --out "$out"

if [ "$action" = deploy ]; then
  apply app "${name}"
fi
# The workspace file changed: the webserver, daemon and health job pick it up.
apply app dagster-daemon
apply job dagster-health
if [ -f "$out/job-dagster-catalog.yaml" ]; then apply job dagster-catalog; fi
apply app dagster-web
