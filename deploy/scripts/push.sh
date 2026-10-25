#!/usr/bin/env bash
# Pushes the images build.sh made to ACR and writes their digests.
# Usage: push.sh <acr-login-server> <tag> <images.json out>. Needs `az login`.
set -euo pipefail
cd "$(dirname "$0")/../.."
acr="$1" tag="$2" out="$3"
engine="$(command -v podman || command -v docker)"

az acr login --name "${acr%%.*}" --expose-token --output tsv --query accessToken |
  "$engine" login "$acr" --username 00000000-0000-0000-0000-000000000000 --password-stdin

declare -A repos=([platform]=mpo-platform)
for dir in code_locations/*/; do
  location="$(basename "$dir")"
  repos[$location]="mpo-${location//_/-}"
done

images="{}"
for key in "${!repos[@]}"; do
  repo="${repos[$key]}"
  "$engine" tag "$repo:$tag" "$acr/$repo:$tag"
  "$engine" push "$acr/$repo:$tag"
  digest="$(az acr manifest show-metadata --registry "${acr%%.*}" --name "$repo:$tag" --query digest -o tsv)"
  images="$(jq --arg k "$key" --arg v "$acr/$repo@$digest" '. + {($k): $v}' <<<"$images")"
done
echo "$images" >"$out"
