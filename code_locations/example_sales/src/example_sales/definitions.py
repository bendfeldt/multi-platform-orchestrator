"""Example domain: the end-to-end lineage target from docs/PLAN.md §4.3, as YAML components in
`defs/`. The IDs in the YAML are placeholders."""

from pathlib import Path

import dagster as dg
from orchestrator_core.resources import platform_resources

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(resources=platform_resources()),
)
