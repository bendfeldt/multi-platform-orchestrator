#!/usr/bin/env bash
# Deploys one environment: render from its contract, migrate the database, update apps.
# Usage: deploy.sh <images.json>. Needs `az login` and CONTRACT_URL (the contract blob URL).
set -euo pipefail
cd "$(dirname "$0")/../.."
images="$1"
out="build/aca"

mkdir -p build
az storage blob download --auth-mode login --blob-url "$CONTRACT_URL" --file build/contract.json --only-show-errors >/dev/null
uv run --frozen --all-packages python deploy/render.py --contract build/contract.json --images "$images" --out "$out"
rg="$(jq -r .resource_group build/contract.json)"

apply() {  # apply <app|job> <name>
  local kind="$1" name="$2"
  local file="$out/$kind-$name.yaml" cmd=(az containerapp)
  if [ "$kind" = job ]; then cmd+=(job); fi
  if "${cmd[@]}" show --name "$name" --resource-group "$rg" --only-show-errors >/dev/null 2>&1; then
    "${cmd[@]}" update --name "$name" --resource-group "$rg" --yaml "$file" --only-show-errors >/dev/null
  else
    "${cmd[@]}" create --name "$name" --resource-group "$rg" --yaml "$file" --only-show-errors >/dev/null
  fi
  echo "applied $name"
}

# 1. Database logins and schema first; nothing else changes if this fails.
apply job dagster-migrate
execution="$(az containerapp job start --name dagster-migrate --resource-group "$rg" --query name -o tsv)"
while true; do
  status="$(az containerapp job execution show --name dagster-migrate --resource-group "$rg" \
    --job-execution-name "$execution" --query properties.status -o tsv)"
  case "$status" in
    Succeeded) break ;;
    Failed | Stopped | Degraded) echo "migration $execution ended $status" >&2; exit 1 ;;
  esac
  sleep 10
done

# 2. Code servers before the webservers and daemon that load them.
for file in "$out"/app-cl-*.yaml; do
  name="$(basename "$file" .yaml)"
  apply app "${name#app-}"
done
apply app dagster-daemon
apply job dagster-cleanup
apply job dagster-health

# 3. The webserver. A new one starts reachable only inside the environment; Front Door can
# reach it only after Entra sign-in is on (docs/PLAN.md §6.2; Spike E confirms the flags).
if ! az containerapp show --name dagster-web --resource-group "$rg" --only-show-errors >/dev/null 2>&1; then
  az containerapp create --name dagster-web --resource-group "$rg" \
    --yaml "$out/app-dagster-web.initial.yaml" --only-show-errors >/dev/null
fi
tenant="$(jq -r .auth.tenant_id build/contract.json)"
az containerapp auth microsoft update --name dagster-web --resource-group "$rg" \
  --client-id "$(jq -r .auth.ui_app_client_id build/contract.json)" --tenant-id "$tenant" \
  --issuer "https://login.microsoftonline.com/$tenant/v2.0" --yes --only-show-errors >/dev/null
az containerapp auth update --name dagster-web --resource-group "$rg" --enabled true \
  --unauthenticated-client-action RedirectToLoginPage --redirect-provider azureactivedirectory \
  --require-https true --proxy-convention Standard --only-show-errors >/dev/null
apply app dagster-web
