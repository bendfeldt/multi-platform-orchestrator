"""YAML-configured components: each runs one platform job and materializes the tables it writes.

Lineage is declared, not discovered (docs/PLAN.md §4.2): `writes` are the assets the job
produces, `reads` the upstream assets it depends on, both as canonical keys.

Catalog fields (docs/PRO_FEATURES_PLAN.md §6): `owners` (emails or `team:<name>`) and `tags`
filter the asset catalog; `columns` gives a written asset its column schema and, through
`derived_from: ["<asset key>:<column>"]`, its column lineage.
"""

from typing import Any, ClassVar

import dagster as dg

from orchestrator_core.asset_keys import key_from_string
from orchestrator_core.query_tag import query_tag_for

DATABRICKS_RUN_ID = "databricks_run_id"


class Column(dg.Model):
    name: str
    type: str = "string"
    description: str | None = None
    tags: dict[str, str] = {}
    derived_from: list[str] = []  # "<asset key>:<column>", e.g. "databricks/sales/silver/orders:id"


class _PlatformJob(dg.Component, dg.Model, dg.Resolvable):
    name: str
    writes: list[str]
    reads: list[str] = []
    group_name: str | None = None
    owners: list[str] = []
    tags: dict[str, str] = {}
    columns: dict[str, list[Column]] = {}  # written asset key -> its columns

    kind: ClassVar[str]
    resource_key: ClassVar[str]

    def run(self, resource: Any, context: dg.AssetExecutionContext) -> dict[str, Any]:
        """Runs the job on the platform and returns metadata for the materializations."""
        raise NotImplementedError

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deps = [key_from_string(key) for key in self.reads]
        specs = [
            dg.AssetSpec(
                key_from_string(key),
                deps=deps,
                kinds={self.kind},
                group_name=self.group_name,
                owners=self.owners,
                tags=self.tags,
                metadata=self._column_metadata(key),
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

    def _column_metadata(self, key: str) -> dict[str, Any]:
        unknown = set(self.columns) - set(self.writes)
        if unknown:
            raise ValueError(
                f"{self.name}: columns for assets it does not write: {sorted(unknown)}"
            )
        columns = self.columns.get(key)
        if not columns:
            return {}
        known = set(self.reads) | set(self.writes)
        lineage: dict[str, list[dg.TableColumnDep]] = {}
        for column in columns:
            for source in column.derived_from:
                asset, sep, name = source.rpartition(":")
                if not sep or asset not in known:
                    raise ValueError(
                        f"{self.name}: {key}.{column.name} derived_from {source!r} must be "
                        "'<asset key>:<column>' of an asset in reads or writes"
                    )
                lineage.setdefault(column.name, []).append(
                    dg.TableColumnDep(asset_key=key_from_string(asset), column_name=name)
                )
        metadata: dict[str, Any] = {
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn(c.name, c.type, c.description, tags=c.tags or None)
                    for c in columns
                ]
            )
        }
        if lineage:
            metadata["dagster/column_lineage"] = dg.TableColumnLineage(lineage)
        return metadata


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
