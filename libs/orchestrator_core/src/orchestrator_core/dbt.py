"""dbt projects whose models get canonical asset keys, so dbt lineage joins the other platforms.

Needs the `dbt` extra. Sources that live on another platform set
`meta: {dagster: {asset_key: [...]}}` to point at that platform's key (docs/PLAN.md §4.2).
"""

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, DbtProjectComponent

from orchestrator_core.asset_keys import (
    databricks_key,
    fabric_table_key,
    key_from_string,
    snowflake_key,
)
from orchestrator_core.auth import get_token
from orchestrator_core.resources.databricks import DATABRICKS_SCOPE

# The env var each dbt profile reads its token from, e.g.
# `token: "{{ env_var('SNOWFLAKE_TOKEN', 'unset') }}"`. The default keeps `dbt parse` working
# at image build time, when no token exists.
TOKEN_ENV = {
    "snowflake": "SNOWFLAKE_TOKEN",
    "databricks": "DATABRICKS_TOKEN",
    "fabric": "FABRIC_TOKEN",
}
_FABRIC_SQL_SCOPE = "https://database.windows.net/.default"


def canonical_dbt_key(
    props: Mapping[str, Any], platform: str, fabric_workspace: str | None
) -> dg.AssetKey:
    """The canonical key of a dbt model, seed, snapshot or source."""
    explicit = props.get("meta", {}).get("dagster", {}).get("asset_key")
    if explicit:
        return key_from_string("/".join(explicit))
    table = props.get("alias") or props.get("identifier") or props["name"]
    database, schema = props["database"], props["schema"]
    if platform == "snowflake":
        return snowflake_key(database, schema, table)
    if platform == "databricks":
        return databricks_key(database, schema, table)
    if platform == "fabric" and fabric_workspace:
        return fabric_table_key(fabric_workspace, database, schema, table)
    raise ValueError(f"unsupported dbt platform {platform!r} (fabric needs fabric_workspace)")


@contextmanager
def _token_env(platform: str, oauth_scope: str | None) -> Iterator[None]:
    scope = {"databricks": DATABRICKS_SCOPE, "fabric": _FABRIC_SQL_SCOPE}.get(platform, oauth_scope)
    if not scope:
        raise ValueError("snowflake dbt projects need oauth_scope")
    name = TOKEN_ENV[platform]
    # dbt runs as a subprocess that inherits os.environ; each Dagster run is its own process.
    # shortcut: one token per dbt invocation (~1 h lifetime); split very long builds.
    os.environ[name] = get_token(scope)
    try:
        yield
    finally:
        os.environ.pop(name, None)


@dataclass
class CanonicalDbtProject(DbtProjectComponent):
    """`DbtProjectComponent` with canonical keys and an Entra token handed to the dbt profile."""

    platform: str = "snowflake"
    fabric_workspace: str | None = None
    # Snowflake only: the External OAuth scope, as on SnowflakeResource.
    oauth_scope: str | None = None

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        return spec.replace_attributes(
            key=canonical_dbt_key(props, self.platform, self.fabric_workspace)
        )

    def execute(self, context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
        with _token_env(self.platform, self.oauth_scope):
            yield from super().execute(context, dbt)
