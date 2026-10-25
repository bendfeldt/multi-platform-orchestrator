#!/usr/bin/env bash
# Pull-request checks, identical in GitHub Actions and Azure DevOps. No cloud login: every
# test runs against mocked providers.
set -euo pipefail
cd "$(dirname "$0")/.."

terraform fmt -check -recursive
for root in modules/domain_access environment bootstrap; do
  echo "== $root"
  (cd "$root" && terraform init -input=false -backend=false >/dev/null && terraform validate && terraform test)
done
if command -v node >/dev/null; then
  echo "== KQL (alert rules, workbook, Fabric function)"
  scripts/check-kql.sh
fi
if command -v trivy >/dev/null; then
  trivy config --exit-code 1 --severity HIGH,CRITICAL --skip-dirs .terraform .
fi
