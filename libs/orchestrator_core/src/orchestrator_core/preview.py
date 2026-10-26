"""Pull-request previews of a code location in dev (docs/PRO_FEATURES_PLAN.md §7.2).

A preview runs the pull request's code with the domain's dev identity against dev data, next
to the real code location, and must not change anything that one owns:

- every asset it defines gets the key `preview/pr<N>/<key>`, so its runs never write into the
  real assets' history (dependencies on other assets keep their real keys);
- no automation: automation conditions are cleared, schedules, sensors and jobs are dropped,
  so it only runs when someone launches it.

The code server runs `-m orchestrator_core.preview` with MPO_PREVIEW_MODULE (the domain's
definitions module) and MPO_PREVIEW_PR (the pull request number).
"""

import importlib
import os

import dagster as dg

MODULE_ENV = "MPO_PREVIEW_MODULE"
PR_ENV = "MPO_PREVIEW_PR"


def preview_key(key: dg.AssetKey, pr: int) -> dg.AssetKey:
    return dg.AssetKey(["preview", f"pr{pr}", *key.path])


def preview_definitions(defs: dg.Definitions, pr: int) -> dg.Definitions:
    # What this location defines, not resolve_all_asset_keys(): that also lists stubs for
    # other domains' upstream assets, which must keep their real keys.
    own: set[dg.AssetKey] = set()
    for asset in defs.assets or []:
        if isinstance(asset, dg.AssetsDefinition):
            own |= asset.keys
        elif isinstance(asset, dg.AssetSpec):
            own.add(asset.key)
    replacements = {key: preview_key(key, pr) for key in own}
    group = f"preview_pr{pr}"
    assets: list[dg.AssetsDefinition | dg.AssetSpec] = []
    for asset in defs.assets or []:
        if isinstance(asset, dg.AssetSpec):
            assets.append(
                asset.replace_attributes(
                    key=replacements[asset.key],
                    deps=[replacements.get(d.asset_key, d.asset_key) for d in asset.deps],
                    automation_condition=None,
                    group_name=group,
                )
            )
        elif isinstance(asset, dg.AssetsDefinition):
            renamed = asset.with_attributes(asset_key_replacements=replacements)
            assets.append(
                renamed.map_asset_specs(
                    lambda s: s.replace_attributes(automation_condition=None, group_name=group)
                )
            )
        else:
            raise TypeError(f"previews support assets and asset specs, not {type(asset).__name__}")
    checks = [
        c.with_attributes(asset_key_replacements=replacements)
        for c in defs.asset_checks or []
        if isinstance(c, dg.AssetChecksDefinition)
    ]
    return dg.Definitions(assets=assets, asset_checks=checks, resources=defs.resources)


def _load() -> dg.Definitions:
    module, pr = os.environ.get(MODULE_ENV), os.environ.get(PR_ENV)
    if not module or not pr:
        raise RuntimeError(f"a preview needs {MODULE_ENV} and {PR_ENV}")
    return preview_definitions(importlib.import_module(module).defs, int(pr))


if MODULE_ENV in os.environ:
    defs = _load()
