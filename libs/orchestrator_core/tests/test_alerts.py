"""Alert sensors and the health check against a real Dagster instance (no mocks)."""

import json
import time

import dagster as dg
import pytest
from dagster._core.definitions.freshness import FreshnessState, FreshnessStateChange
from dagster._core.remote_origin import InProcessCodeLocationOrigin
from dagster._core.scheduler.instigation import (
    InstigatorState,
    InstigatorStatus,
    InstigatorType,
    TickData,
    TickStatus,
)
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import InProcessWorkspaceLoadTarget
from dagster_shared.error import SerializableErrorInfo
from orchestrator_core import alerts, health

ORDERS = dg.AssetKey(["snowflake", "sales", "mart", "fct_orders"])


def alert_lines(capsys) -> list[dict]:
    return [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith('{"mpo_alert"')
    ]


@pytest.fixture
def instance(tmp_path):
    # Like Postgres in Azure, a non-run-sharded event log (global event cursors work).
    storage = {
        "module": "dagster.core.storage.event_log",
        "class": "ConsolidatedSqliteEventLogStorage",
        "config": {"base_dir": str(tmp_path / "events")},
    }
    with dg.instance_for_test(overrides={"event_log_storage": storage}) as instance:
        yield instance


def evaluate(sensor: dg.SensorDefinition, instance, cursor: str | None) -> str | None:
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    sensor(context)
    return context.cursor


def test_failed_run_alerts_with_location_and_user(instance, capsys, monkeypatch):
    monkeypatch.setenv("MPO_UI_URL", "https://dagster.example.com/")

    @dg.op
    def boom():
        raise RuntimeError("platform said no")

    @dg.job(tags={"dagster/code_location": "example_sales", "mpo/launched_by": "anna@contoso.com"})
    def nightly():
        boom()

    result = nightly.execute_in_process(instance=instance, raise_on_error=False)
    failure = next(e for e in result.all_events if e.event_type_value == "PIPELINE_FAILURE")
    context = dg.build_run_status_sensor_context(
        sensor_name="mpo_run_failure_alerts",
        dagster_event=failure,
        dagster_instance=instance,
        dagster_run=result.dagster_run,
    ).for_run_failure()
    alerts.run_failure_alerts(context)

    [alert] = [a for a in alert_lines(capsys) if a["mpo_alert"] == "run_failure"]
    assert alert["location"] == "example_sales"
    assert alert["launched_by"] == "anna@contoso.com"
    assert alert["job"] == "nightly"
    assert alert["url"] == f"https://dagster.example.com/runs/{result.run_id}"


def test_failed_check_alerts_once_and_passing_check_not(instance, capsys):
    cursor = evaluate(alerts.check_failure_alerts, instance, None)  # starts at "now"
    for passed in (True, False):
        instance.report_runless_asset_event(
            dg.AssetCheckEvaluation(
                asset_key=ORDERS,
                check_name="row_count",
                passed=passed,
                severity=dg.AssetCheckSeverity.WARN,
                description="0 rows",
            )
        )
    cursor = evaluate(alerts.check_failure_alerts, instance, cursor)
    assert [(a["mpo_alert"], a["severity"], a["check"]) for a in alert_lines(capsys)] == [
        ("check_failed", "warning", "row_count")
    ]
    evaluate(alerts.check_failure_alerts, instance, cursor)  # nothing new: no repeat
    assert alert_lines(capsys) == []


def test_history_before_the_first_evaluation_is_not_replayed(instance, capsys):
    instance.report_runless_asset_event(
        dg.AssetCheckEvaluation(asset_key=ORDERS, check_name="old", passed=False)
    )
    evaluate(alerts.check_failure_alerts, instance, None)
    assert alert_lines(capsys) == []


def test_freshness_failure_alerts(instance, capsys):
    cursor = evaluate(alerts.freshness_alerts, instance, None)
    for new, previous in (
        (FreshnessState.FAIL, FreshnessState.PASS),
        (FreshnessState.PASS, FreshnessState.FAIL),
    ):
        instance.report_runless_asset_event(
            FreshnessStateChange(
                key=ORDERS,
                new_state=new,
                previous_state=previous,
                state_change_timestamp=time.time(),
            )
        )
    evaluate(alerts.freshness_alerts, instance, cursor)
    assert [(a["mpo_alert"], a["severity"], a["state"]) for a in alert_lines(capsys)] == [
        ("freshness", "error", "FAIL")
    ]


@dg.sensor(job=dg.define_asset_job("dummy"))
def flaky_sensor():
    return dg.SkipReason("never runs in this test")


def test_health_reports_daemons_failed_ticks_and_broken_locations(instance, capsys, tmp_path):
    origin = InProcessCodeLocationOrigin(
        loadable_target_origin=LoadableTargetOrigin(
            python_file=__file__, attribute="sensor_defs", working_directory=str(tmp_path)
        ),
        location_name="example_sales",
    )
    with WorkspaceProcessContext(instance, InProcessWorkspaceLoadTarget(origin)) as context:
        location = context.create_request_context().get_code_location("example_sales")
        remote = next(iter(location.get_repositories().values())).get_sensor("flaky_sensor")
    instance.add_instigator_state(
        InstigatorState(remote.get_remote_origin(), InstigatorType.SENSOR, InstigatorStatus.RUNNING)
    )
    now = time.time()
    instance.create_tick(
        TickData(
            instigator_origin_id=remote.get_remote_origin_id(),
            instigator_name="flaky_sensor",
            instigator_type=InstigatorType.SENSOR,
            status=TickStatus.FAILURE,
            timestamp=now - 60,
            selector_id=remote.selector_id,
            error=SerializableErrorInfo("Snowflake unreachable", [], "Exception"),
        )
    )
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(
        "load_from:\n  - python_module:\n      module_name: no_such_module_for_test\n"
        "      location_name: broken\n"
    )

    health.check(instance, str(workspace), now=now)

    lines = alert_lines(capsys)
    kinds = {a["mpo_alert"] for a in lines}
    assert {"daemon_unhealthy", "tick_failure", "code_location_error", "health_check"} <= kinds
    [tick] = [a for a in lines if a["mpo_alert"] == "tick_failure"]
    assert (tick["location"], tick["instigator"], tick["summary"]) == (
        "example_sales",
        "flaky_sensor",
        "Snowflake unreachable",
    )
    assert [a["location"] for a in lines if a["mpo_alert"] == "code_location_error"] == ["broken"]


def test_old_failed_ticks_are_not_reported_again(instance, capsys):
    health.check(instance, None, now=time.time())
    assert not [a for a in alert_lines(capsys) if a["mpo_alert"] == "tick_failure"]


sensor_defs = dg.Definitions(sensors=[flaky_sensor])
