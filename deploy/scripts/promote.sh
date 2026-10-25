#!/usr/bin/env bash
# Copies the exact images (same digests) from one environment's ACR to the next.
# Usage: promote.sh <source images.json> <target-acr-login-server> <images.json out>
set -euo pipefail
source_images="$1" acr="$2" out="$3"

images="{}"
for key in $(jq -r 'keys[]' "$source_images"); do
  ref="$(jq -r --arg k "$key" '.[$k]' "$source_images")"  # registry/repo@sha256:...
  repo_digest="${ref#*/}"
  repo="${repo_digest%@*}" digest="${ref#*@}"
  az acr import --name "${acr%%.*}" --source "$ref" --image "$repo:${digest#sha256:}" --force
  images="$(jq --arg k "$key" --arg v "$acr/$repo@$digest" '. + {($k): $v}' <<<"$images")"
done
echo "$images" >"$out"
