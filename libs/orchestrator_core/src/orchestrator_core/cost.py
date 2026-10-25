"""Daily platform cost per asset (docs/PRO_FEATURES_PLAN.md §5.2), run in mpo_ops.

For one UTC day: the finished Dagster runs, the assets they materialized, and what Snowflake,
Databricks and Fabric charged for them:

- Snowflake: QUERY_ATTRIBUTION_HISTORY by query tag (query_tag.py). A tag with a dbt node goes
  to that model's asset; a tag with only a step is split over the assets that step wrote.
- Databricks: system.billing.usage by job run ID (stored on materializations as
  `databricks_run_id`), priced with the effective list price. Runs that fail record no
  materialization, so their Databricks cost cannot be attributed here.
- Fabric: capacity operation events in an Eventhouse (made outside Terraform), read through the
  KQL function `MpoCapacityOperations` (infra repo, fabric/capacity_operations.kql). An event
  whose operationId is the job instance or refresh ID a materialization recorded goes to it;
  otherwise an event of the same item that started during the run. CU of Preview operations
  is reported but not priced (Fabric does not bill them).

Results go to the asset pages as observations (numeric metadata, charted under "Plots") and to
Log Analytics (MpoCost_CL).
"""

import os
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster._core.events import DagsterEventType
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunRecord, RunsFilter

from orchestrator_core import log_ingestion
from orchestrator_core.components import DATABRICKS_RUN_ID
from orchestrator_core.query_tag import PREFIX, parse_query_tag
from orchestrator_core.resources.databricks import DatabricksResource
from orchestrator_core.resources.snowflake import SnowflakeResource

CREDIT_PRICE_ENV = "SNOWFLAKE_CREDIT_PRICE_USD"  # your contract's price; list prices vary
COST_WAREHOUSE_ENV = "DATABRICKS_COST_WAREHOUSE_ID"
FABRIC_URI_ENV = "FABRIC_EVENTS_QUERY_URI"
FABRIC_DATABASE_ENV = "FABRIC_EVENTS_DATABASE"
CU_HOUR_PRICE_ENV = "FABRIC_CU_HOUR_PRICE_USD"
# What FabricItemJob / FabricSemanticModelRefresh record (components.py).
FABRIC_OPERATION_KEYS = ("fabric_job_instance_id", "refresh_request_id")
_GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
LOCATION_TAG = "dagster/code_location"
_FINISHED = [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED]

# Queries start before the run finishes, possibly the day before.
SNOWFLAKE_SQL = """
SELECT QUERY_TAG,
       SUM(CREDITS_ATTRIBUTED_COMPUTE)
         + SUM(COALESCE(CREDITS_USED_QUERY_ACCELERATION, 0)) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE START_TIME >= %(start)s AND START_TIME < %(end)s AND STARTSWITH(QUERY_TAG, %(prefix)s)
GROUP BY QUERY_TAG
"""

DATABRICKS_SQL = """
SELECT u.usage_metadata.job_run_id AS job_run_id,
       SUM(u.usage_quantity) AS dbus,
       SUM(u.usage_quantity * p.pricing.effective_list.default) AS list_cost,
       MAX(p.currency_code) AS currency
FROM system.billing.usage AS u
JOIN system.billing.list_prices AS p
  ON u.cloud = p.cloud AND u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.workspace_id = :workspace_id
  AND u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_metadata.job_run_id IS NOT NULL
GROUP BY u.usage_metadata.job_run_id
"""

# Columns MpoCapacityOperations returns, whatever the Eventhouse table looks like.
FABRIC_KQL = """
MpoCapacityOperations
| where OperationStartTime >= datetime({start}) and OperationStartTime < datetime({end})
| where WorkspaceId in ({workspaces})
| project OperationId, WorkspaceId, ItemId, OperationStartTime, CapacityUnitMs, ReleaseType
"""


@dataclass(frozen=True)
class Materialization:
    asset: dg.AssetKey
    run_id: str
    step_key: str
    dbt_node: str | None = None
    databricks_run_id: str | None = None
    fabric_workspace_id: str | None = None
    fabric_item_id: str | None = None
    fabric_operation_id: str | None = None


@dataclass(frozen=True)
class FabricOperation:
    operation_id: str
    workspace_id: str
    item_id: str
    start: float  # epoch seconds
    capacity_unit_ms: float
    billed: bool  # releaseType Public; Preview operations are not billed


@dataclass(frozen=True)
class DatabricksCost:
    dbus: float
    list_cost: float
    currency: str


@dataclass
class Cost:
    snowflake_credits: float = 0.0
    databricks_dbus: float = 0.0
    databricks_list_cost: float = 0.0
    fabric_capacity_unit_ms: float = 0.0
    fabric_billed_capacity_unit_ms: float = 0.0
    currencies: set[str] = field(default_factory=set)
    runs: set[str] = field(default_factory=set)

    def usd(self, credit_price: float | None, cu_hour_price: float | None = None) -> float | None:
        """Total in USD, or None when a part cannot be priced."""
        if self.snowflake_credits and credit_price is None:
            return None
        if self.databricks_list_cost and self.currencies - {"USD"}:
            return None
        if self.fabric_billed_capacity_unit_ms and cu_hour_price is None:
            return None
        fabric_cu_hours = self.fabric_billed_capacity_unit_ms / 3_600_000
        return (
            self.snowflake_credits * (credit_price or 0.0)
            + self.databricks_list_cost
            + fabric_cu_hours * (cu_hour_price or 0.0)
        )


@dataclass
class Attribution:
    assets: dict[dg.AssetKey, Cost]
    runs: dict[str, Cost]  # everything a run cost
    unattributed: dict[str, Cost]  # the part no asset could be found for


def attribute(
    run_ids: Iterable[str],
    materializations: Sequence[Materialization],
    snowflake_credits: Mapping[str, float],
    databricks: Mapping[str, DatabricksCost],
    fabric: Sequence[FabricOperation] = (),
    run_windows: Mapping[str, tuple[float, float]] | None = None,
) -> Attribution:
    """Splits platform cost over the assets that caused it. Pure, so it is tested directly."""
    ours = set(run_ids)
    by_step: dict[tuple[str, str], set[dg.AssetKey]] = defaultdict(set)
    by_node: dict[tuple[str, str], dg.AssetKey] = {}
    by_databricks_run: dict[str, set[tuple[str, dg.AssetKey]]] = defaultdict(set)
    by_fabric_operation: dict[str, set[tuple[str, dg.AssetKey]]] = defaultdict(set)
    by_fabric_item: dict[tuple[str, str], set[tuple[str, dg.AssetKey]]] = defaultdict(set)
    windows = run_windows or {}
    for m in materializations:
        by_step[(m.run_id, m.step_key)].add(m.asset)
        if m.dbt_node:
            by_node[(m.run_id, m.dbt_node)] = m.asset
        if m.databricks_run_id:
            by_databricks_run[m.databricks_run_id].add((m.run_id, m.asset))
        if m.fabric_operation_id:
            by_fabric_operation[m.fabric_operation_id.lower()].add((m.run_id, m.asset))
        if m.fabric_item_id and m.fabric_workspace_id:
            item = (m.fabric_workspace_id.lower(), m.fabric_item_id.lower())
            by_fabric_item[item].add((m.run_id, m.asset))

    result = Attribution(defaultdict(Cost), defaultdict(Cost), defaultdict(Cost))

    for tag, credits in snowflake_credits.items():
        fields = parse_query_tag(tag)
        if not fields or fields.get("mpo_run") not in ours:
            continue
        run = fields["mpo_run"]
        result.runs[run].snowflake_credits += credits
        node = fields.get("mpo_dbt_node")
        if node and (run, node) in by_node:
            targets = {by_node[(run, node)]}
        else:
            targets = by_step.get((run, fields.get("mpo_step", "")), set())
        if not targets:
            result.unattributed[run].snowflake_credits += credits
            continue
        for asset in targets:
            cost = result.assets[asset]
            cost.snowflake_credits += credits / len(targets)
            cost.runs.add(run)

    for job_run_id, spend in databricks.items():
        targets = {t for t in by_databricks_run.get(job_run_id, set()) if t[0] in ours}
        for run, asset in targets:
            for cost in (result.assets[asset], result.runs[run]):
                cost.databricks_dbus += spend.dbus / len(targets)
                cost.databricks_list_cost += spend.list_cost / len(targets)
                cost.currencies.add(spend.currency)
            result.assets[asset].runs.add(run)

    for op in fabric:
        targets = by_fabric_operation.get(op.operation_id.lower(), set())
        if not targets:  # the same item, started while one of our runs ran
            same_item = by_fabric_item.get((op.workspace_id.lower(), op.item_id.lower()), set())
            targets = {
                (run, asset)
                for run, asset in same_item
                if run in windows and windows[run][0] <= op.start <= windows[run][1]
            }
        targets = {t for t in targets if t[0] in ours}
        for run, asset in targets:
            for cost in (result.assets[asset], result.runs[run]):
                cost.fabric_capacity_unit_ms += op.capacity_unit_ms / len(targets)
                if op.billed:
                    cost.fabric_billed_capacity_unit_ms += op.capacity_unit_ms / len(targets)
            result.assets[asset].runs.add(run)

    return Attribution(dict(result.assets), dict(result.runs), dict(result.unattributed))


# ---- Reading Dagster


def finished_runs(instance: dg.DagsterInstance, start: datetime, end: datetime) -> list[RunRecord]:
    """Runs that finished in [start, end)."""
    records = instance.get_run_records(
        RunsFilter(statuses=_FINISHED, updated_after=start - timedelta(seconds=1))
    )
    return [
        r
        for r in records
        if r.end_time is not None and start.timestamp() <= r.end_time < end.timestamp()
    ]


def run_location(run: DagsterRun) -> str | None:
    origin = run.remote_job_origin
    return run.tags.get(LOCATION_TAG) or (origin.location_name if origin else None)


def materializations(
    instance: dg.DagsterInstance, records: Iterable[RunRecord]
) -> list[Materialization]:
    found = []
    for run in (record.dagster_run for record in records):
        for entry in instance.all_logs(run.run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION):
            event = entry.dagster_event
            m = event.event_specific_data.materialization if event else None  # type: ignore[union-attr]
            if not isinstance(m, dg.AssetMaterialization):
                continue
            found.append(
                Materialization(
                    asset=m.asset_key,
                    run_id=run.run_id,
                    step_key=entry.step_key or "",
                    dbt_node=_text(m.metadata.get("unique_id")),
                    databricks_run_id=_text(m.metadata.get(DATABRICKS_RUN_ID)),
                    fabric_workspace_id=_text(m.metadata.get("fabric_workspace_id")),
                    fabric_item_id=_text(m.metadata.get("fabric_item_id")),
                    fabric_operation_id=next(
                        (_text(m.metadata[k]) for k in FABRIC_OPERATION_KEYS if m.metadata.get(k)),
                        None,
                    ),
                )
            )
    return found


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


# ---- Reading the platforms (need the mpo_ops grants; see the infra repo README)


def fetch_snowflake_credits(
    snowflake: SnowflakeResource, start: datetime, end: datetime
) -> dict[str, float]:
    rows = snowflake.fetch(
        SNOWFLAKE_SQL, {"start": start - timedelta(days=1), "end": end, "prefix": PREFIX}
    )
    return {row["QUERY_TAG"]: float(row["CREDITS"] or 0) for row in rows}


def fetch_databricks_costs(
    databricks: DatabricksResource, warehouse_id: str, day: date
) -> dict[str, DatabricksCost]:
    from databricks.sdk.service.sql import (
        Disposition,
        ExecuteStatementRequestOnWaitTimeout,
        Format,
        StatementParameterListItem,
        StatementState,
    )

    from orchestrator_core.resources._wait import wait_until_done

    client = databricks.client()
    response = client.statement_execution.execute_statement(
        statement=DATABRICKS_SQL,
        warehouse_id=warehouse_id,
        parameters=[
            StatementParameterListItem(name="workspace_id", value=str(client.get_workspace_id())),
            StatementParameterListItem(
                name="start_date", value=(day - timedelta(days=1)).isoformat(), type="DATE"
            ),
            StatementParameterListItem(name="end_date", value=day.isoformat(), type="DATE"),
        ],
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    statement_id = response.statement_id or ""
    done = {
        StatementState.SUCCEEDED,
        StatementState.FAILED,
        StatementState.CANCELED,
        StatementState.CLOSED,
    }
    response = wait_until_done(
        poll=lambda: client.statement_execution.get_statement(statement_id),
        is_done=lambda r: r.status is not None and r.status.state in done,
        cancel=lambda: client.statement_execution.cancel_execution(statement_id),
        interval=5,
    )
    if response.status is None or response.status.state != StatementState.SUCCEEDED:
        error = response.status.error if response.status else None
        raise RuntimeError(f"Databricks cost query {statement_id} failed: {error}")

    rows: list[list[Any]] = []
    chunk = response.result
    while chunk is not None:
        rows.extend(chunk.data_array or [])
        if chunk.next_chunk_index is None:
            break
        chunk = client.statement_execution.get_statement_result_chunk_n(
            statement_id, chunk.next_chunk_index
        )
    return {
        str(run_id): DatabricksCost(float(dbus or 0), float(cost or 0), str(currency or ""))
        for run_id, dbus, cost, currency in rows
    }


def fetch_fabric_operations(
    query_uri: str, database: str, start: datetime, end: datetime, workspace_ids: Iterable[str]
) -> list[FabricOperation]:
    """Capacity operations of the given workspaces that started in [start - 1 day, end)."""
    workspaces = sorted({w.lower() for w in workspace_ids})
    if not workspaces:
        return []
    # The IDs go into the query, so only GUIDs (what Fabric uses) are accepted.
    bad = [w for w in workspaces if not _GUID.match(w)]
    if bad:
        raise ValueError(f"not Fabric workspace IDs: {bad}")
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    from orchestrator_core.auth import credential

    query = FABRIC_KQL.format(
        start=_kql_time(start - timedelta(days=1)),
        end=_kql_time(end),
        workspaces=", ".join(f'"{w}"' for w in workspaces),
    )
    # The client asks the Eventhouse for its token audience; the identity needs read access.
    builder = KustoConnectionStringBuilder.with_azure_token_credential(query_uri, credential())
    with KustoClient(builder) as client:
        table = client.execute_query(database, query).primary_results[0]
        return [
            FabricOperation(
                operation_id=str(row["OperationId"] or ""),
                workspace_id=str(row["WorkspaceId"] or ""),
                item_id=str(row["ItemId"] or ""),
                start=_epoch(row["OperationStartTime"]),
                capacity_unit_ms=float(row["CapacityUnitMs"] or 0),
                billed=str(row["ReleaseType"] or "") != "Preview",
            )
            for row in table
        ]


def _kql_time(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


# ---- The job


# end_offset=1: today can be run by hand too (partial); the schedules pick settled days.
DAILY = dg.DailyPartitionsDefinition(start_date="2026-10-01", end_offset=1)


def _day_bounds(partition_key: str) -> tuple[date, datetime, datetime]:
    day = date.fromisoformat(partition_key)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return day, start, start + timedelta(days=1)


@dg.op
def report_cost(context: dg.OpExecutionContext) -> None:
    day, start, end = _day_bounds(context.partition_key)
    instance = context.instance
    records = finished_runs(instance, start, end)
    found = materializations(instance, records)
    windows = {r.dagster_run.run_id: (r.start_time or 0.0, r.end_time or 0.0) for r in records}

    snowflake_credits: dict[str, float] = {}
    databricks: dict[str, DatabricksCost] = {}
    fabric: list[FabricOperation] = []
    # Built from the values here, not platform_resources(): its EnvVar fields only resolve when
    # Dagster initialises the resource, and this op reads whichever platforms are configured.
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        snowflake = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            role=os.environ["SNOWFLAKE_ROLE"],
            user=os.environ.get("SNOWFLAKE_USER"),
        )
        snowflake_credits = fetch_snowflake_credits(snowflake, start, end)
    if os.environ.get("DATABRICKS_HOST") and os.environ.get(COST_WAREHOUSE_ENV):
        databricks = fetch_databricks_costs(
            DatabricksResource(host=os.environ["DATABRICKS_HOST"]),
            os.environ[COST_WAREHOUSE_ENV],
            day,
        )
    if os.environ.get(FABRIC_URI_ENV) and os.environ.get(FABRIC_DATABASE_ENV):
        fabric = fetch_fabric_operations(
            os.environ[FABRIC_URI_ENV],
            os.environ[FABRIC_DATABASE_ENV],
            start,
            end,
            {m.fabric_workspace_id for m in found if m.fabric_workspace_id},
        )

    result = attribute(list(windows), found, snowflake_credits, databricks, fabric, windows)
    prices = (_price(CREDIT_PRICE_ENV), _price(CU_HOUR_PRICE_ENV))
    for asset, cost in result.assets.items():
        metadata: dict[str, Any] = {
            "cost_date": day.isoformat(),
            "snowflake_credits": round(cost.snowflake_credits, 6),
            "databricks_dbus": round(cost.databricks_dbus, 6),
            "databricks_list_cost": round(cost.databricks_list_cost, 6),
            "fabric_cu_seconds": round(cost.fabric_capacity_unit_ms / 1000, 3),
            "cost_runs": len(cost.runs),
        }
        usd = cost.usd(*prices)
        if usd is not None:
            metadata["cost_usd"] = round(usd, 6)
        context.log_event(dg.AssetObservation(asset_key=asset, metadata=metadata))

    by_id = {r.dagster_run.run_id: r.dagster_run for r in records}
    rows = [
        _row("asset", day, None, asset.to_user_string(), cost, prices)
        for asset, cost in result.assets.items()
    ] + [
        _row("run", day, by_id[run_id], "", cost, prices, result.unattributed.get(run_id))
        for run_id, cost in result.runs.items()
    ]
    sent = log_ingestion.send(log_ingestion.COST_STREAM, rows)
    context.log.info(
        f"{day}: {len(records)} runs, {len(result.assets)} assets with cost, "
        f"{sent} rows to Log Analytics"
    )


def _price(env: str) -> float | None:
    raw = os.environ.get(env)
    return float(raw) if raw else None


def _row(
    scope: str,
    day: date,
    run: DagsterRun | None,
    asset: str,
    cost: Cost,
    prices: tuple[float | None, float | None],
    unattributed: Cost | None = None,
) -> dict[str, Any]:
    return {
        "TimeGenerated": datetime.now(UTC).isoformat(),
        "Day": day.isoformat(),
        "Scope": scope,
        "Location": (run_location(run) if run else None) or "",
        "Job": run.job_name if run else "",
        "RunId": run.run_id if run else "",
        "Asset": asset,
        "SnowflakeCredits": cost.snowflake_credits,
        "DatabricksDbus": cost.databricks_dbus,
        "DatabricksListCost": cost.databricks_list_cost,
        "FabricCuSeconds": cost.fabric_capacity_unit_ms / 1000,
        "CostUsd": cost.usd(*prices),
        "UnattributedSnowflakeCredits": unattributed.snowflake_credits if unattributed else 0.0,
    }


@dg.job(name="mpo_cost", partitions_def=DAILY)
def cost_job() -> None:
    report_cost()


@dg.schedule(
    job=cost_job, cron_schedule="23 6 * * *", default_status=dg.DefaultScheduleStatus.RUNNING
)
def cost_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Every morning, the day before yesterday: Snowflake attribution can lag up to 8 hours and
    Databricks billing several hours, so that day is complete."""
    scheduled = context.scheduled_execution_time or datetime.now(UTC)
    day = (scheduled.astimezone(UTC) - timedelta(days=2)).date().isoformat()
    return dg.RunRequest(run_key=day, partition_key=day)
