#!/usr/bin/env bash
# shellcheck disable=SC2154 # rg and out are set by the script that sources this file
# Shared by deploy.sh and preview.sh. Needs `rg` (resource group) and `out` (rendered files).

apply() { # apply <app|job> <name>: create or update from "$out/<kind>-<name>.yaml"
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

previews() { # running previews as "<location>:<pr>,..." (tag mpo-preview, deploy/render.py)
  az containerapp list --resource-group "$rg" --only-show-errors -o json |
    jq -r '[.[] | .tags["mpo-preview"] // empty] | sort | join(",")'
}

running_images() { # {"<name>": image} of the running apps: platform, code locations, previews
  az containerapp list --resource-group "$rg" --only-show-errors -o json | jq '
    map(select(.name == "dagster-daemon" or (.name | startswith("cl-"))))
    | map({(if .name == "dagster-daemon" then "platform"
            else .name | ltrimstr("cl-") | gsub("-"; "_") end):
           .properties.template.containers[0].image})
    | add // {}'
}
