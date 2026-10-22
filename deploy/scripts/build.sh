#!/usr/bin/env bash
# Builds every image with Podman (Docker as fallback). Usage: build.sh [tag]
set -euo pipefail
cd "$(dirname "$0")/../.."
tag="${1:-local}"
engine="$(command -v podman || command -v docker)"

"$engine" build -f containers/Containerfile \
  --build-arg SYNC_ARGS="--package orchestrator-core --extra webserver" \
  -t "mpo-platform:$tag" .
for dir in code_locations/*/; do
  package="$(basename "$dir" | tr _ -)"
  "$engine" build -f containers/Containerfile \
    --build-arg SYNC_ARGS="--package $package" \
    -t "mpo-$package:$tag" .
done
