#!/usr/bin/env bash
# Parses every alert rule's KQL with Microsoft's Kusto parser against the Container Apps log
# table, so a broken query fails here and not silently in Azure (rules skip validation because
# the table only exists after the first log line).
set -euo pipefail
cd "$(dirname "$0")/.."

(cd scripts/kql && npm install --no-audit --no-fund --silent)
(cd environment && terraform init -input=false -backend=false >/dev/null &&
  terraform test -verbose -no-color) | python3 scripts/kql/extract.py >scripts/kql/queries.json
node scripts/kql/check.js ./queries.json
