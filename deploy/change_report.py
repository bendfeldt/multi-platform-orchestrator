"""What a pull request changes in the Dagster definitions (docs/PRO_FEATURES_PLAN.md §7.1).

Loads every code location's definitions, without any cloud login, and compares two snapshots:

    python deploy/change_report.py snapshot --repo <checkout> --out base.json   (in its venv)
    python deploy/change_report.py diff base.json head.json > report.md

CI takes the base branch's snapshot in a checkout of the base branch with that branch's own
packages, so each side is loaded by its own code.
"""

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import dagster as dg

_KIND_TAG = "dagster/kind/"
_FIELDS = (
    "deps", "group", "kinds", "owners", "tags", "code_version", "partitions", "automation",
    "columns", "checks",
)  # fmt: skip


def _asset(spec: dg.AssetSpec, checks: list[str]) -> dict[str, Any]:
    schema = spec.metadata.get("dagster/column_schema")
    columns = schema.columns if isinstance(schema, dg.TableSchema) else []
    return {
        "deps": sorted(d.asset_key.to_user_string() for d in spec.deps),
        "group": spec.group_name,
        "kinds": sorted(spec.kinds),
        "owners": sorted(spec.owners),
        "tags": {k: v for k, v in sorted(spec.tags.items()) if not k.startswith(_KIND_TAG)},
        "code_version": spec.code_version,
        "partitions": str(spec.partitions_def) if spec.partitions_def else None,
        "automation": str(spec.automation_condition) if spec.automation_condition else None,
        "columns": [f"{c.name}:{c.type}" for c in columns],
        "checks": checks,
    }


def snapshot_definitions(defs: dg.Definitions) -> dict[str, Any]:
    graph = defs.resolve_asset_graph()
    checks: dict[str, list[str]] = {}
    for check_key in graph.asset_check_keys:
        checks.setdefault(check_key.asset_key.to_user_string(), []).append(check_key.name)
    return {
        "assets": {
            spec.key.to_user_string(): _asset(
                spec, sorted(checks.get(spec.key.to_user_string(), []))
            )
            for spec in defs.resolve_all_asset_specs()
        },
        "schedules": {s.name: getattr(s, "cron_schedule", None) for s in defs.schedules or []},
        "sensors": sorted(s.name for s in defs.sensors or []),
    }


def snapshot(repo: Path) -> dict[str, Any]:
    """Every code location of a checkout, by name, or the error that stopped it loading."""
    result: dict[str, Any] = {}
    for path in sorted((repo / "code_locations").iterdir()):
        if not (path / "src" / path.name / "definitions.py").exists():
            continue
        try:
            module = importlib.import_module(f"{path.name}.definitions")
            result[path.name] = snapshot_definitions(module.defs)
        except Exception as error:  # a broken location is a finding, not a crash
            result[path.name] = {"error": f"{type(error).__name__}: {error}"}
    return result


def _changed(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    lines = []
    for field in _FIELDS:
        if before.get(field) != after.get(field):
            lines.append(f"{field}: `{before.get(field)}` → `{after.get(field)}`")
    return lines


def diff(base: dict[str, Any], head: dict[str, Any]) -> str:
    out = ["## Dagster changes", ""]
    found = False
    for location in sorted(set(base) | set(head)):
        b, h = base.get(location), head.get(location)
        section: list[str] = []
        if b is None:
            section.append("- **new code location**")
            b = {"assets": {}, "schedules": {}, "sensors": []}
        if h is None:
            section.append("- **code location removed**")
            h = {"assets": {}, "schedules": {}, "sensors": []}
        if "error" in h:
            section.append(f"- :x: **fails to load:** `{h['error']}`")
        elif "error" not in b:
            assets_b, assets_h = b["assets"], h["assets"]
            for key in sorted(set(assets_h) - set(assets_b)):
                section.append(f"- added asset `{key}`")
            for key in sorted(set(assets_b) - set(assets_h)):
                section.append(f"- **removed asset** `{key}`")
            for key in sorted(set(assets_b) & set(assets_h)):
                for line in _changed(assets_b[key], assets_h[key]):
                    section.append(f"- `{key}` {line}")
            for kind in ("schedules", "sensors"):
                old, new = b.get(kind), h.get(kind)
                if old != new:
                    section.append(f"- {kind}: `{old}` → `{new}`")
        if section:
            found = True
            out += [f"### `{location}`", "", *section, ""]
    if not found:
        out.append("No changes to assets, checks, schedules or sensors.")
    return "\n".join(out) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("--repo", type=Path, default=Path.cwd())
    snap.add_argument("--out", type=Path, required=True)
    cmp = sub.add_parser("diff")
    cmp.add_argument("base", type=Path)
    cmp.add_argument("head", type=Path)
    args = parser.parse_args()
    if args.command == "snapshot":
        args.out.write_text(json.dumps(snapshot(args.repo), indent=1, sort_keys=True))
    else:
        sys.stdout.write(diff(json.loads(args.base.read_text()), json.loads(args.head.read_text())))


if __name__ == "__main__":
    main()
