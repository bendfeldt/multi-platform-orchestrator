"""Cost attribution and insights on a real instance, with the example domain's own components;
only the platforms' answers are stand-ins."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from orchestrator_core import cost, insights, log_ingestion
from orchestrator_core.cost import Cost, DatabricksCost, Materialization, attribute
from orchestrator_core.query_tag import parse_query_tag, query_tag

MART = dg.AssetKey(["snowflake", "sales", "mart", "fct_orders"])
SILVER = dg.AssetKey(["databricks", "sales", "silver", "orders"])
A, B, C = dg.AssetKey("a"), dg.AssetKey("b"), dg.AssetKey("c")


# ---- attribution rules


def test_dbt_node_tags_go_to_their_model_and_step_tags_are_split():
    mats = [
        Materialization(A, "r1", "dbt_step", dbt_node="model.p.a"),
        Materialization(B, "r1", "dbt_step", dbt_node="model.p.b"),
    ]
    node_a = query_tag("r1", "dbt_step")[:-1] + ',"mpo_dbt_node":"model.p.a"}'
    result = attribute(["r1"], mats, {node_a: 3.0, query_tag("r1", "dbt_step"): 1.0}, databricks={})
    assert result.assets[A].snowflake_credits == pytest.approx(3.5)  # own 3 + half of 1
    assert result.assets[B].snowflake_credits == pytest.approx(0.5)
    assert result.runs["r1"].snowflake_credits == pytest.approx(4.0)


def test_cost_without_a_materialization_is_kept_as_unattributed():
    mats = [Materialization(A, "r1", "load")]
    tags = {query_tag("r1", "load"): 1.0, query_tag("r1", "failed_step"): 2.0}
    result = attribute(["r1"], mats, tags, databricks={})
    assert result.assets[A].snowflake_credits == 1.0
    assert result.unattributed["r1"].snowflake_credits == 2.0
    assert result.runs["r1"].snowflake_credits == 3.0


def test_other_runs_and_foreign_tags_are_ignored():
    mats = [Materialization(A, "r1", "s", databricks_run_id="111")]
    result = attribute(
        ["r1"],
        mats,
        {query_tag("r9", "s"): 5.0, "someone else's tag": 7.0},
        {"111": DatabricksCost(2.0, 1.2, "USD"), "999": DatabricksCost(50.0, 30.0, "USD")},
    )
    assert result.assets[A].snowflake_credits == 0
    assert result.assets[A].databricks_list_cost == pytest.approx(1.2)
    assert set(result.runs) == {"r1"} and result.runs["r1"].databricks_dbus == 2.0


def test_one_databricks_run_is_split_over_the_assets_it_wrote():
    mats = [Materialization(x, "r1", "s", databricks_run_id="111") for x in (A, B, C)]
    result = attribute(["r1"], mats, {}, {"111": DatabricksCost(3.0, 0.9, "USD")})
    assert [result.assets[x].databricks_dbus for x in (A, B, C)] == pytest.approx([1.0] * 3)
    assert result.runs["r1"].databricks_list_cost == pytest.approx(0.9)


def test_usd_needs_a_credit_price_and_usd_list_prices():
    assert Cost(snowflake_credits=2).usd(None) is None
    assert Cost(snowflake_credits=2, databricks_list_cost=1, currencies={"USD"}).usd(3.0) == 7.0
    assert Cost(databricks_list_cost=1, currencies={"EUR"}).usd(3.0) is None
    assert Cost().usd(None) == 0.0


def test_query_tags_round_trip_and_reject_foreign_tags():
    tag = query_tag("run-1", "mart_orders")
    assert parse_query_tag(tag) == {"mpo_run": "run-1", "mpo_step": "mart_orders"}
    with_node = tag[:-1] + ',"mpo_dbt_node":"model.p.x"}'  # what dbt_macros/ builds
    assert parse_query_tag(with_node) == {
        "mpo_run": "run-1",
        "mpo_step": "mart_orders",
        "mpo_dbt_node": "model.p.x",
    }
    assert parse_query_tag('{"mpo_run": broken') is None
    assert parse_query_tag("finance nightly") is None


# ---- end to end on an instance


@pytest.fixture
def instance(tmp_path):
    storage = {
        "module": "dagster.core.storage.event_log",
        "class": "ConsolidatedSqliteEventLogStorage",
        "config": {"base_dir": str(tmp_path / "events")},
    }
    with dg.instance_for_test(overrides={"event_log_storage": storage}) as instance:
        yield instance


def _example_assets():
    import example_sales

    defs = dg.load_from_defs_folder(path_within_project=Path(example_sales.__file__).parent)
    wanted = {MART, SILVER}
    return [a for a in defs.assets or [] if isinstance(a, dg.AssetsDefinition) and a.keys & wanted]


def test_cost_reaches_the_assets_through_the_tags_the_components_set(instance, monkeypatch):
    seen_tags: list[str | None] = []

    class _Snowflake:
        def execute(self, sql, query_tag=None):
            seen_tags.append(query_tag)
            return ["q1"]

    class _Databricks:
        def run_job(self, job_id, job_parameters=None):
            return {"run_id": 4242, "run_page_url": "https://adb/run/4242"}

    result = dg.materialize(
        _example_assets(),
        instance=instance,
        resources={"snowflake": _Snowflake(), "databricks": _Databricks()},
    )
    assert result.success
    [tag] = seen_tags
    assert parse_query_tag(tag) == {"mpo_run": result.run_id, "mpo_step": "mart_orders"}

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb")
    monkeypatch.setenv("DATABRICKS_COST_WAREHOUSE_ID", "wh")
    monkeypatch.setenv("SNOWFLAKE_CREDIT_PRICE_USD", "4")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "OPS_WH")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "MPO_MPO_OPS_DEV")
    clients = []

    def snowflake_credits(snowflake, start, end):
        clients.append((snowflake.account, snowflake.warehouse, snowflake.role))
        return {tag: 0.5}

    def databricks_costs(databricks, warehouse_id, day):
        clients.append((databricks.host, warehouse_id))
        return {"4242": DatabricksCost(10.0, 2.5, "USD")}

    monkeypatch.setattr(cost, "fetch_snowflake_credits", snowflake_credits)
    monkeypatch.setattr(cost, "fetch_databricks_costs", databricks_costs)
    sent: list[tuple[str, list]] = []
    monkeypatch.setattr(log_ingestion, "send", lambda stream, rows: sent.append((stream, rows)))

    today = datetime.now(UTC).date().isoformat()
    report = cost.cost_job.execute_in_process(instance=instance, partition_key=today)
    assert report.success

    observations = [
        e.asset_observation_data.asset_observation
        for e in report.all_events
        if e.event_type_value == "ASSET_OBSERVATION"
    ]
    observed = {o.asset_key: o.metadata for o in observations}
    assert observed[MART]["snowflake_credits"].value == 0.5
    assert observed[MART]["cost_usd"].value == 2.0  # 0.5 credits x 4 USD
    assert observed[SILVER]["databricks_dbus"].value == 10.0
    assert observed[SILVER]["cost_usd"].value == 2.5
    # The observations land on the asset pages, where "Plots" charts them.
    latest = instance.fetch_observations(MART, limit=1).records[0]
    assert latest.asset_observation and latest.asset_observation.metadata["cost_usd"].value == 2.0

    # Real settings, not EnvVar placeholders.
    assert clients == [("acct", "OPS_WH", "MPO_MPO_OPS_DEV"), ("https://adb", "wh")]
    [(stream, rows)] = sent
    assert stream == log_ingestion.COST_STREAM
    run_row = next(r for r in rows if r["Scope"] == "run")
    assert run_row["RunId"] == result.run_id and run_row["CostUsd"] == pytest.approx(4.5)
    assert set(run_row) == COST_COLUMNS


# Must equal MpoCost_CL / MpoInsights_CL in the infra repo (environment/insights.tf).
COST_COLUMNS = {
    "TimeGenerated", "Day", "Scope", "Location", "Job", "RunId", "Asset", "SnowflakeCredits",
    "DatabricksDbus", "DatabricksListCost", "CostUsd", "UnattributedSnowflakeCredits",
}  # fmt: skip
INSIGHTS_COLUMNS = {
    "TimeGenerated", "Day", "Scope", "Location", "Job", "Asset", "Runs", "Succeeded", "Failed",
    "Canceled", "DurationP50", "DurationP95", "Materializations", "ChecksPassed",
    "ChecksFailed", "FreshnessFailures",
}  # fmt: skip


def test_insights_count_outcomes_per_job_and_asset(instance, monkeypatch):
    @dg.asset(check_specs=[dg.AssetCheckSpec("not_empty", asset="orders")])
    def orders():
        yield dg.MaterializeResult()
        yield dg.AssetCheckResult(passed=False)

    @dg.op
    def boom():
        raise RuntimeError("no")

    @dg.job
    def nightly():
        boom()

    assert dg.materialize([orders], instance=instance).success
    assert dg.materialize([orders], instance=instance).success
    nightly.execute_in_process(instance=instance, raise_on_error=False)

    sent: list[tuple[str, list]] = []
    monkeypatch.setattr(log_ingestion, "send", lambda stream, rows: sent.append((stream, rows)))
    today = datetime.now(UTC).date().isoformat()
    assert insights.insights_job.execute_in_process(instance=instance, partition_key=today).success

    [(stream, rows)] = sent
    assert stream == log_ingestion.INSIGHTS_STREAM
    jobs = {r["Job"]: r for r in rows if r["Scope"] == "job"}
    assert (jobs["nightly"]["Runs"], jobs["nightly"]["Failed"]) == (1, 1)
    assert (
        jobs["__ephemeral_asset_job__"]["Runs"],
        jobs["__ephemeral_asset_job__"]["Succeeded"],
    ) == (2, 2)
    [asset] = [r for r in rows if r["Scope"] == "asset" and r["Asset"] == "orders"]
    assert (asset["Materializations"], asset["ChecksFailed"], asset["ChecksPassed"]) == (2, 2, 0)
    assert all(set(r) == INSIGHTS_COLUMNS for r in rows)


def test_schedules_report_settled_days():
    at = datetime(2026, 10, 25, 6, 23, tzinfo=UTC)
    [cost_run] = _requests(cost.cost_schedule, at)
    [insights_run] = _requests(insights.insights_schedule, at)
    assert cost_run.partition_key == "2026-10-23"  # Snowflake attribution lags up to 8 h
    assert insights_run.partition_key == "2026-10-24"


def _requests(schedule: dg.ScheduleDefinition, at: datetime) -> list[dg.RunRequest]:
    with dg.instance_for_test() as instance:
        context = dg.build_schedule_context(instance=instance, scheduled_execution_time=at)
        return [schedule(context)]  # type: ignore[list-item]


def test_nothing_is_sent_without_ingestion_settings(monkeypatch):
    monkeypatch.delenv(log_ingestion.ENDPOINT_ENV, raising=False)
    assert log_ingestion.send(log_ingestion.COST_STREAM, [{"x": 1}]) == 0


class _Statements:
    """The parts of the Databricks SDK's statement API that cost.py uses."""

    def __init__(self, final_state, pages):
        self.final_state, self.pages, self.calls = final_state, pages, []

    def _page(self, index):
        from databricks.sdk.service.sql import ResultData

        rows, nxt = self.pages[index]
        return ResultData(data_array=rows, next_chunk_index=nxt)

    def execute_statement(self, **kwargs):
        from databricks.sdk.service.sql import StatementResponse, StatementState, StatementStatus

        self.calls.append(kwargs)
        return StatementResponse(
            statement_id="s1", status=StatementStatus(state=StatementState.PENDING)
        )

    def get_statement(self, statement_id):
        from databricks.sdk.service.sql import StatementResponse, StatementStatus

        return StatementResponse(
            statement_id=statement_id,
            status=StatementStatus(state=self.final_state),
            result=self._page(0),
        )

    def get_statement_result_chunk_n(self, statement_id, chunk_index):
        return self._page(chunk_index)

    def cancel_execution(self, statement_id):
        pass


def _databricks(statements) -> Any:
    client = type("C", (), {"statement_execution": statements, "get_workspace_id": lambda s: 77})
    return type("R", (), {"client": lambda s: client()})()


def test_databricks_cost_query_reads_every_chunk_for_this_workspace(monkeypatch):
    from databricks.sdk.service.sql import StatementState
    from orchestrator_core.resources import _wait

    monkeypatch.setattr(_wait.time, "sleep", lambda s: None)
    statements = _Statements(
        StatementState.SUCCEEDED,
        [([["111", "2.5", "1.25", "USD"]], 1), ([["222", "1", "0.5", "USD"]], None)],
    )
    costs = cost.fetch_databricks_costs(
        _databricks(statements), "wh", datetime(2026, 10, 23).date()
    )
    assert costs == {
        "111": DatabricksCost(2.5, 1.25, "USD"),
        "222": DatabricksCost(1.0, 0.5, "USD"),
    }
    [call] = statements.calls
    params = {p.name: (p.value, p.type) for p in call["parameters"]}
    assert params == {
        "workspace_id": ("77", None),
        "start_date": ("2026-10-22", "DATE"),
        "end_date": ("2026-10-23", "DATE"),
    }
    assert call["warehouse_id"] == "wh" and "effective_list" in call["statement"]


def test_failed_databricks_cost_query_fails_the_run(monkeypatch):
    from databricks.sdk.service.sql import StatementState

    statements = _Statements(StatementState.FAILED, [([], None)])
    with pytest.raises(RuntimeError, match="s1 failed"):
        cost.fetch_databricks_costs(_databricks(statements), "wh", datetime(2026, 10, 23).date())


def test_snowflake_cost_query_reads_only_this_repos_tags():
    seen = {}

    class _Snowflake:
        def fetch(self, sql, params):
            seen.update(params)
            return [{"QUERY_TAG": '{"mpo_run":"r"}', "CREDITS": 1.5}]

    start = datetime(2026, 10, 23, tzinfo=UTC)
    end = datetime(2026, 10, 24, tzinfo=UTC)
    snowflake: Any = _Snowflake()
    assert cost.fetch_snowflake_credits(snowflake, start, end) == {'{"mpo_run":"r"}': 1.5}
    assert seen["prefix"] == '{"mpo_run":' and seen["start"] == datetime(2026, 10, 22, tzinfo=UTC)
