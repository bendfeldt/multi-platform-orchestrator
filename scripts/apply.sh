#!/usr/bin/env bash
# Plans and applies one environment. Usage: apply.sh <dev|test|prod> [--plan-only]
# Needs `az login` as that environment's infra deployer (the pipeline does it).
set -euo pipefail
cd "$(dirname "$0")/../environment"
env="$1"

terraform init -input=false -backend-config="envs/$env.backend.hcl"
terraform plan -input=false -var-file="envs/$env.tfvars" -out=tfplan
if [ "${2:-}" != "--plan-only" ]; then
  terraform apply -input=false tfplan
fi
