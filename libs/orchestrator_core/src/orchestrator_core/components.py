"""YAML-configured components: each runs one platform job and materializes the tables it writes.

Lineage is declared, not discovered (docs/PLAN.md §4.2): `writes` are the assets the job
produces, `reads` the upstream assets it depends on, both as canonical keys.
"""

from typing import Any, ClassVar

import dagster as dg

from orchestrator_core.asset_keys import key_from_string
from orchestrator_core.query_tag import query_tag_for

DATABRICKS_RUN_ID = "databricks_run_id"


class _PlatformJob(dg.Component, dg.Model, dg.Resolvable):
    name: str
    writes: list[str]
    reads: list[str] = []
    group_name: str | None = None

    kind: ClassVar[str]
    resource_key: ClassVar[str]

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        """Runs the job on the platform and returns metadata for the materializations."""
        raise NotImplementedError

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deps = [key_from_string(key) for key in self.reads]
        specs = [
            dg.AssetSpec(
                key_from_string(key), deps=deps, kinds={self.kind}, group_name=self.group_name
            )
            for key in self.writes
        ]

        # pool: one concurrency pool per platform, so a backfill cannot flood one platform.
        @dg.multi_asset(
            name=self.name, specs=specs, required_resource_keys={self.resource_key}, pool=self.kind
        )
        def _job(context: dg.AssetExecutionContext):
            metadata = self.run(getattr(context.resources, self.resource_key), context)
            for spec in specs:
                yield dg.MaterializeResult(asset_key=spec.key, metadata=metadata)

        return dg.Definitions(assets=[_job])


class FabricItemJob(_PlatformJob):
    """Any Fabric item with a job type: `Pipeline`, `RunNotebook`, `sparkjob`, ..."""

    kind: ClassVar[str] = "fabric"
    resource_key: ClassVar[str] = "fabric"

    workspace_id: str
    item_id: str
    job_type: str
    execution_data: dict[str, Any] | None = None

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        job = resource.run_item_job(
            self.workspace_id, self.item_id, self.job_type, self.execution_data
        )
        return {
            "fabric_job_instance_id": job.get("id", ""),
            "fabric_workspace_id": self.workspace_id,
            "fabric_item_id": self.item_id,
        }


class FabricSemanticModelRefresh(_PlatformJob):
    kind: ClassVar[str] = "fabric"
    resource_key: ClassVar[str] = "fabric"

    workspace_id: str
    dataset_id: str
    objects: list[dict[str, str]] | None = None

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        refresh = resource.refresh_semantic_model(self.workspace_id, self.dataset_id, self.objects)
        return {
            "refresh_request_id": refresh.get("requestId", ""),
            "fabric_workspace_id": self.workspace_id,
            "fabric_item_id": self.dataset_id,
        }


class DatabricksJob(_PlatformJob):
    kind: ClassVar[str] = "databricks"
    resource_key: ClassVar[str] = "databricks"

    job_id: int
    job_parameters: dict[str, str] | None = None

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        run = resource.run_job(self.job_id, self.job_parameters)
        return {
            # Billing (system.billing.usage.usage_metadata.job_run_id) joins on this.
            DATABRICKS_RUN_ID: str(run["run_id"]),
            "run_page_url": dg.MetadataValue.url(run["run_page_url"] or ""),
        }


class SnowflakeSql(_PlatformJob):
    kind: ClassVar[str] = "snowflake"
    resource_key: ClassVar[str] = "snowflake"

    sql: str

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        return {"query_ids": resource.execute(self.sql, query_tag=query_tag_for(context))}
