"""Alert sensors (docs/PRO_FEATURES_PLAN.md §4). They only detect: each alert is one JSON line
on stdout, which Container Apps sends to Log Analytics, where Azure Monitor alert rules
(infra repo) deliver it. No webhook URL or token exists anywhere.

    {"mpo_alert": "run_failure" | "check_failed" | "freshness" | ..., "severity": ...,
     "location": <code location>, ...}
"""

import json
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster._core.definitions.freshness import FreshnessState, FreshnessStateChange
from dagster._core.events import DagsterEventType
from dagster._core.events.log import EventLogEntry

from orchestrator_core.run_coordinator import LAUNCHED_BY_TAG

ALERT_KEY = "mpo_alert"
_LOCATION_TAG = "dagster/code_location"
_BATCH = 500


def emit(kind: str, **fields: Any) -> None:
    record = {ALERT_KEY: kind, "time": datetime.now(UTC).isoformat(), **fields}
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)


def ui_url(path: str) -> str | None:
    base = os.environ.get("MPO_UI_URL")
    return f"{base.rstrip('/')}{path}" if base else None


def _run_location(instance: dg.DagsterInstance, run_id: str | None) -> str | None:
    run = instance.get_run_by_id(run_id) if run_id else None
    return run.tags.get(_LOCATION_TAG) if run else None


@dg.run_failure_sensor(
    name="mpo_run_failure_alerts",
    monitor_all_code_locations=True,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def run_failure_alerts(context: dg.RunFailureSensorContext) -> None:
    run = context.dagster_run
    origin = run.remote_job_origin
    emit(
        "run_failure",
        severity="error",
        location=run.tags.get(_LOCATION_TAG) or (origin.location_name if origin else None),
        job=run.job_name,
        run_id=run.run_id,
        launched_by=run.tags.get(LAUNCHED_BY_TAG),
        summary=context.failure_event.message,
        url=ui_url(f"/runs/{run.run_id}"),
    )


def _check_alert(instance: dg.DagsterInstance, entry: EventLogEntry) -> dict | None:
    evaluation = entry.dagster_event.event_specific_data if entry.dagster_event else None
    if not isinstance(evaluation, dg.AssetCheckEvaluation) or evaluation.passed:
        return None
    asset = evaluation.asset_key.to_user_string()
    return {
        "kind": "check_failed",
        "severity": "error" if evaluation.severity == dg.AssetCheckSeverity.ERROR else "warning",
        "location": _run_location(instance, entry.run_id),
        "asset": asset,
        "check": evaluation.check_name,
        "run_id": entry.run_id or None,
        "summary": evaluation.description,
        "url": ui_url(f"/assets/{asset}?view=checks"),
    }


def _freshness_alert(instance: dg.DagsterInstance, entry: EventLogEntry) -> dict | None:
    change = entry.dagster_event.event_specific_data if entry.dagster_event else None
    if not isinstance(change, FreshnessStateChange) or change.new_state not in (
        FreshnessState.FAIL,
        FreshnessState.WARN,
    ):
        return None
    asset = change.key.to_user_string()
    latest = instance.get_latest_materialization_event(change.key)
    return {
        "kind": "freshness",
        "severity": "error" if change.new_state == FreshnessState.FAIL else "warning",
        "location": _run_location(instance, latest.run_id if latest else None),
        "asset": asset,
        "state": change.new_state.value,
        "previous_state": change.previous_state.value,
        "url": ui_url(f"/assets/{asset}"),
    }


def _event_sensor(
    name: str,
    event_type: DagsterEventType,
    to_alert: Callable[[dg.DagsterInstance, EventLogEntry], dict | None],
) -> dg.SensorDefinition:
    @dg.sensor(
        name=name, minimum_interval_seconds=60, default_status=dg.DefaultSensorStatus.RUNNING
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
        instance = context.instance
        # The instance's get_event_records is deprecated and its fetch_* replacements cover no
        # check or freshness events, so read the event log storage directly.
        events = instance.event_log_storage
        if context.cursor is None:
            # First evaluation: start at the newest event instead of alerting on history.
            latest = events.get_event_records(dg.EventRecordsFilter(event_type), limit=1)
            context.update_cursor(str(latest[0].storage_id if latest else 0))
            return dg.SkipReason("cursor initialised")
        records = events.get_event_records(
            dg.EventRecordsFilter(event_type, after_cursor=int(context.cursor)),
            limit=_BATCH,
            ascending=True,
        )
        sent = 0
        for record in records:
            alert = to_alert(instance, record.event_log_entry)
            if alert:
                emit(alert.pop("kind"), **alert)
                sent += 1
        if records:
            context.update_cursor(str(records[-1].storage_id))
        return dg.SkipReason(f"{sent} alerts from {len(records)} events")

    return _sensor


check_failure_alerts = _event_sensor(
    "mpo_check_failure_alerts", DagsterEventType.ASSET_CHECK_EVALUATION, _check_alert
)
freshness_alerts = _event_sensor(
    "mpo_freshness_alerts", DagsterEventType.FRESHNESS_STATE_CHANGE, _freshness_alert
)

ALERT_SENSORS = [run_failure_alerts, check_failure_alerts, freshness_alerts]
