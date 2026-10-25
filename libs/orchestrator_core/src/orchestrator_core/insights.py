"""Daily operational insights from Dagster's own data (docs/PRO_FEATURES_PLAN.md §5.1), run in
mpo_ops: per job runs, outcomes and durations; per asset materializations, check results and
freshness failures. Rows go to Log Analytics (MpoInsights_CL) for the workbook.
"""

import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import dagster as dg
from dagster._core.definitions.freshness import FreshnessState, FreshnessStateChange
from dagster._core.events import DagsterEventType
from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter

from orchestrator_core import log_ingestion
from orchestrator_core.cost import DAILY, LOCATION_TAG, _day_bounds

_FINISHED = [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED]
_BATCH = 1000


def percentile(values: Sequence[float], q: float) -> float | None:
    """Inclusive percentile (q in 0..100); None without values."""
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


def job_rows(instance: dg.DagsterInstance, day: date, start: datetime, end: datetime) -> list[dict]:
    records = instance.get_run_records(
        RunsFilter(statuses=_FINISHED, updated_after=start - timedelta(seconds=1))
    )
    by_job: dict[tuple[str, str], list] = defaultdict(list)
    for record in records:
        if record.end_time is None or not start.timestamp() <= record.end_time < end.timestamp():
            continue
        run = record.dagster_run
        origin = run.remote_job_origin
        location = run.tags.get(LOCATION_TAG) or (origin.location_name if origin else "") or ""
        by_job[(location, run.job_name)].append(record)

    rows = []
    for (location, job), runs in sorted(by_job.items()):
        statuses = Counter(r.dagster_run.status for r in runs)
        durations = [r.end_time - r.start_time for r in runs if r.start_time and r.end_time]
        rows.append(
            _row(
                day,
                Scope="job",
                Location=location,
                Job=job,
                Runs=len(runs),
                Succeeded=statuses[DagsterRunStatus.SUCCESS],
                Failed=statuses[DagsterRunStatus.FAILURE],
                Canceled=statuses[DagsterRunStatus.CANCELED],
                DurationP50=percentile(durations, 50),
                DurationP95=percentile(durations, 95),
            )
        )
    return rows


def _events(
    instance: dg.DagsterInstance, event_type: DagsterEventType, start: datetime, end: datetime
) -> Iterable[dg.EventLogRecord]:
    """Every event of a type in [start, end), in pages."""
    cursor = None
    while True:
        records = instance.event_log_storage.get_event_records(
            dg.EventRecordsFilter(
                event_type,
                after_timestamp=start.timestamp(),
                before_timestamp=end.timestamp(),
                after_cursor=cursor,
            ),
            limit=_BATCH,
            ascending=True,
        )
        yield from records
        if len(records) < _BATCH:
            return
        cursor = records[-1].storage_id


def asset_rows(
    instance: dg.DagsterInstance, day: date, start: datetime, end: datetime
) -> list[dict]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for record in _events(instance, DagsterEventType.ASSET_MATERIALIZATION, start, end):
        if record.asset_key:
            counts[record.asset_key.to_user_string()]["materializations"] += 1
    for record in _events(instance, DagsterEventType.ASSET_CHECK_EVALUATION, start, end):
        event = record.event_log_entry.dagster_event
        evaluation = event.event_specific_data if event else None
        if isinstance(evaluation, dg.AssetCheckEvaluation):
            outcome = "checks_passed" if evaluation.passed else "checks_failed"
            counts[evaluation.asset_key.to_user_string()][outcome] += 1
    for record in _events(instance, DagsterEventType.FRESHNESS_STATE_CHANGE, start, end):
        event = record.event_log_entry.dagster_event
        change = event.event_specific_data if event else None
        if isinstance(change, FreshnessStateChange) and change.new_state == FreshnessState.FAIL:
            counts[change.key.to_user_string()]["freshness_failures"] += 1
    return [
        _row(
            day,
            Scope="asset",
            Asset=asset,
            Materializations=c["materializations"],
            ChecksPassed=c["checks_passed"],
            ChecksFailed=c["checks_failed"],
            FreshnessFailures=c["freshness_failures"],
        )
        for asset, c in sorted(counts.items())
    ]


# The columns of MpoInsights_CL (infra repo, environment/insights.tf).
TEXT_COLUMNS = ("Scope", "Location", "Job", "Asset")
NUMBER_COLUMNS = (
    "Runs", "Succeeded", "Failed", "Canceled", "DurationP50", "DurationP95",
    "Materializations", "ChecksPassed", "ChecksFailed", "FreshnessFailures",
)  # fmt: skip


def _row(day: date, **values: Any) -> dict[str, Any]:
    unknown = set(values) - set(TEXT_COLUMNS) - set(NUMBER_COLUMNS)
    assert not unknown, f"not a column of MpoInsights_CL: {unknown}"
    return {
        "TimeGenerated": datetime.now(UTC).isoformat(),
        "Day": day.isoformat(),
        **{column: values.get(column, "") for column in TEXT_COLUMNS},
        **{column: values.get(column) for column in NUMBER_COLUMNS},
    }


@dg.op
def report_insights(context: dg.OpExecutionContext) -> None:
    day, start, end = _day_bounds(context.partition_key)
    rows = job_rows(context.instance, day, start, end) + asset_rows(
        context.instance, day, start, end
    )
    sent = log_ingestion.send(log_ingestion.INSIGHTS_STREAM, rows)
    context.log.info(f"{day}: {len(rows)} insight rows, {sent} sent to Log Analytics")


@dg.job(name="mpo_insights", partitions_def=DAILY)
def insights_job() -> None:
    report_insights()


@dg.schedule(
    job=insights_job, cron_schedule="7 1 * * *", default_status=dg.DefaultScheduleStatus.RUNNING
)
def insights_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Every night, for yesterday (Dagster's own data is complete at once)."""
    scheduled = context.scheduled_execution_time or datetime.now(UTC)
    day = (scheduled.astimezone(UTC) - timedelta(days=1)).date().isoformat()
    return dg.RunRequest(run_key=day, partition_key=day)
