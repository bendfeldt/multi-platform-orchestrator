"""Daily platform cost per asset (docs/PRO_FEATURES_PLAN.md §5.2), run in mpo_ops.

For one UTC day: the finished Dagster runs, the assets they materialized, and what Snowflake
and Databricks charged for them:

- Snowflake: QUERY_ATTRIBUTION_HISTORY by query tag (query_tag.py). A tag with a dbt node goes
  to that model's asset; a tag with only a step is split over the assets that step wrote.
- Databricks: system.billing.usage by job run ID (stored on materializations as
  `databricks_run_id`), priced with the effective list price. Runs that fail record no
  materialization, so their Databricks cost cannot be attributed here.

Results go to the asset pages as observations (numeric metadata, charted under "Plots") and to
Log Analytics (MpoCost_CL). Fabric follows in phase D.
"""

import os
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster._core.events import DagsterEventType
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunsFilter

from orchestrator_core import log_ingestion
from orchestrator_core.components import DATABRICKS_RUN_ID
from orchestrator_core.query_tag import PREFIX, parse_query_tag
from orchestrator_core.resources.databricks import DatabricksResource
from orchestrator_core.resources.snowflake import SnowflakeResource

CREDIT_PRICE_ENV = "SNOWFLAKE_CREDIT_PRICE_USD"  # your contract's price; list prices vary
COST_WAREHOUSE_ENV = "DATABRICKS_COST_WAREHOUSE_ID"
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


@dataclass(frozen=True)
class Materialization:
    asset: dg.AssetKey
    run_id: str
    step_key: str
    dbt_node: str | None = None
    databricks_run_id: str | None = None


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
    currencies: set[str] = field(default_factory=set)
    runs: set[str] = field(default_factory=set)

    def usd(self, credit_price: float | None) -> float | None:
        """Total in USD, or None when a part cannot be priced."""
        if self.snowflake_credits and credit_price is None:
            return None
        if self.databricks_list_cost and self.currencies - {"USD"}:
            return None
        return self.snowflake_credits * (credit_price or 0.0) + self.databricks_list_cost


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
) -> Attribution:
    """Splits platform cost over the assets that caused it. Pure, so it is tested directly."""
    ours = set(run_ids)
    by_step: dict[tuple[str, str], set[dg.AssetKey]] = defaultdict(set)
    by_node: dict[tuple[str, str], dg.AssetKey] = {}
    by_databricks_run: dict[str, set[tuple[str, dg.AssetKey]]] = defaultdict(set)
    for m in materializations:
        by_step[(m.run_id, m.step_key)].add(m.asset)
        if m.dbt_node:
            by_node[(m.run_id, m.dbt_node)] = m.asset
        if m.databricks_run_id:
            by_databricks_run[m.databricks_run_id].add((m.run_id, m.asset))

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

    return Attribution(dict(result.assets), dict(result.runs), dict(result.unattributed))


# ---- Reading Dagster


def finished_runs(instance: dg.DagsterInstance, start: datetime, end: datetime) -> list[DagsterRun]:
    """Runs that finished in [start, end)."""
    records = instance.get_run_records(
        RunsFilter(statuses=_FINISHED, updated_after=start - timedelta(seconds=1))
    )
    return [
        r.dagster_run
        for r in records
        if r.end_time is not None and start.timestamp() <= r.end_time < end.timestamp()
    ]


def run_location(run: DagsterRun) -> str | None:
    origin = run.remote_job_origin
    return run.tags.get(LOCATION_TAG) or (origin.location_name if origin else None)


def materializations(
    instance: dg.DagsterInstance, runs: Iterable[DagsterRun]
) -> list[Materialization]:
    found = []
    for run in runs:
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

    rows: list[list[str | None]] = []
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


# ---- The job


# end_offset=1: today can be run by hand too (partial); the schedules pick settled days.
DAILY = dg.DailyPartitionsDefinition(start_date="2026-10-01", end_offset=1)


def _day_bounds(partition_key: str) -> tuple[date, datetime, datetime]:
    day = date.fromisoformat(partition_key)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return day, start, start + timedelta(days=1)


def _credit_price() -> float | None:
    raw = os.environ.get(CREDIT_PRICE_ENV)
    return float(raw) if raw else None


@dg.op
def report_cost(context: dg.OpExecutionContext) -> None:
    day, start, end = _day_bounds(context.partition_key)
    instance = context.instance
    runs = finished_runs(instance, start, end)
    found = materializations(instance, runs)

    snowflake_credits: dict[str, float] = {}
    databricks: dict[str, DatabricksCost] = {}
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

    result = attribute([r.run_id for r in runs], found, snowflake_credits, databricks)
    price = _credit_price()
    for asset, cost in result.assets.items():
        metadata: dict[str, Any] = {
            "cost_date": day.isoformat(),
            "snowflake_credits": round(cost.snowflake_credits, 6),
            "databricks_dbus": round(cost.databricks_dbus, 6),
            "databricks_list_cost": round(cost.databricks_list_cost, 6),
            "cost_runs": len(cost.runs),
        }
        usd = cost.usd(price)
        if usd is not None:
            metadata["cost_usd"] = round(usd, 6)
        context.log_event(dg.AssetObservation(asset_key=asset, metadata=metadata))

    by_id = {r.run_id: r for r in runs}
    rows = [
        _row("asset", day, None, asset.to_user_string(), cost, price)
        for asset, cost in result.assets.items()
    ] + [
        _row("run", day, by_id[run_id], "", cost, price, result.unattributed.get(run_id))
        for run_id, cost in result.runs.items()
    ]
    sent = log_ingestion.send(log_ingestion.COST_STREAM, rows)
    context.log.info(
        f"{day}: {len(runs)} runs, {len(result.assets)} assets with cost, "
        f"{sent} rows to Log Analytics"
    )


def _row(
    scope: str,
    day: date,
    run: DagsterRun | None,
    asset: str,
    cost: Cost,
    price: float | None,
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
        "CostUsd": cost.usd(price),
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
