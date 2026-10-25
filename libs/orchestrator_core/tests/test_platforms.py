from types import SimpleNamespace

import dagster as dg
import pytest
import responses
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from orchestrator_core.resources import _wait, databricks, fabric, snowflake
from orchestrator_core.resources.databricks import DatabricksError, DatabricksResource
from orchestrator_core.resources.fabric import FABRIC_API, POWER_BI_API, FabricError, FabricResource
from orchestrator_core.resources.snowflake import SnowflakeResource

JOBS = f"{FABRIC_API}/workspaces/ws/items/item/jobs/instances"
JOB = f"{JOBS}/job-1"
REFRESH = f"{POWER_BI_API}/groups/ws/datasets/ds/refreshes/req-1"


@pytest.fixture(autouse=True)
def fake_token(monkeypatch):
    for module in (fabric, databricks):
        monkeypatch.setattr(module, "get_token", lambda scope: "token")
    monkeypatch.setattr(_wait.time, "sleep", lambda seconds: None)


@responses.activate
def test_fabric_job_waits_until_completed():
    responses.post(JOBS, status=202, headers={"Location": JOB})
    responses.get(JOB, json={"id": "job-1", "status": "InProgress"})
    responses.get(JOB, json={"id": "job-1", "status": "Completed"})

    job = FabricResource().run_item_job("ws", "item", "Pipeline", {"parameters": {"day": "x"}})

    assert job["status"] == "Completed"
    assert responses.calls[0].request.headers["Authorization"] == "Bearer token"
    assert "jobType=Pipeline" in (responses.calls[0].request.url or "")


@responses.activate
def test_fabric_job_failure_raises():
    responses.post(JOBS, status=202, headers={"Location": JOB})
    responses.get(JOB, json={"id": "job-1", "status": "Failed", "failureReason": {"message": "x"}})

    with pytest.raises(FabricError, match="Failed"):
        FabricResource().run_item_job("ws", "item", "RunNotebook")


@responses.activate
def test_fabric_retries_after_throttling(monkeypatch):
    monkeypatch.setattr(fabric.time, "sleep", lambda seconds: None)
    responses.post(JOBS, status=429, headers={"Retry-After": "1"})
    responses.post(JOBS, status=202, headers={"Location": JOB})
    responses.get(JOB, json={"id": "job-1", "status": "Completed"})

    assert FabricResource().run_item_job("ws", "item", "Pipeline")["status"] == "Completed"


@responses.activate
def test_stopped_dagster_run_cancels_fabric_job():
    responses.post(JOBS, status=202, headers={"Location": JOB})

    def interrupted(request):
        raise KeyboardInterrupt  # what a terminated Dagster run raises inside the step

    responses.add_callback(responses.GET, JOB, callback=interrupted)
    cancel = responses.post(f"{JOB}/cancel", status=202)

    with pytest.raises(KeyboardInterrupt):
        FabricResource().run_item_job("ws", "item", "Pipeline")
    assert cancel.call_count == 1


@responses.activate
def test_semantic_model_refresh_treats_unknown_as_running():
    start = responses.post(
        f"{POWER_BI_API}/groups/ws/datasets/ds/refreshes", status=202, headers={"Location": REFRESH}
    )
    responses.get(REFRESH, json={"requestId": "req-1", "status": "Unknown"})
    responses.get(REFRESH, json={"requestId": "req-1", "status": "Completed"})

    refresh = FabricResource().refresh_semantic_model("ws", "ds", [{"table": "orders"}])

    assert refresh["status"] == "Completed"
    assert "objects" in str(start.calls[0].request.body)


class _FakeJobs:
    def __init__(self, result: RunResultState):
        self.result = result
        self.cancelled = False

    def run_now(self, job_id, job_parameters):
        return SimpleNamespace(run_id=7)

    def get_run(self, run_id):
        state = SimpleNamespace(
            life_cycle_state=RunLifeCycleState.TERMINATED,
            result_state=self.result,
            state_message="m",
        )
        return SimpleNamespace(state=state, run_page_url="https://adb/run/7")

    def cancel_run(self, run_id):
        self.cancelled = True


@pytest.mark.parametrize("result", [RunResultState.SUCCESS, RunResultState.FAILED])
def test_databricks_job_result(monkeypatch, result):
    jobs = _FakeJobs(result)
    monkeypatch.setattr(DatabricksResource, "client", lambda self: SimpleNamespace(jobs=jobs))
    resource = DatabricksResource(host="https://adb")

    if result == RunResultState.SUCCESS:
        assert resource.run_job(1)["run_page_url"] == "https://adb/run/7"
    else:
        with pytest.raises(DatabricksError):
            resource.run_job(1)


@pytest.mark.parametrize(
    ("in_azure", "expected"),
    [
        (True, {"authenticator": "WORKLOAD_IDENTITY", "workload_identity_provider": "AZURE"}),
        (False, {"authenticator": "externalbrowser", "user": "me@example.com"}),
    ],
)
def test_snowflake_uses_workload_identity_in_azure(monkeypatch, in_azure, expected):
    seen = {}
    monkeypatch.delenv("IDENTITY_ENDPOINT", raising=False)
    if in_azure:
        monkeypatch.setenv("IDENTITY_ENDPOINT", "http://localhost")

    class _Connection:
        def execute_string(self, sql):
            return [SimpleNamespace(sfqid="q1"), SimpleNamespace(sfqid="q2")]

        def close(self):
            seen["closed"] = True

    monkeypatch.setattr(
        snowflake.snowflake.connector, "connect", lambda **kw: seen.update(kw) or _Connection()
    )
    resource = SnowflakeResource(account="a", warehouse="w", role="r", user="me@example.com")

    assert resource.execute("select 1; select 2") == ["q1", "q2"]
    assert expected.items() <= seen.items() and "token" not in seen and seen["closed"]


def test_yaml_component_materializes_what_it_writes():
    from pathlib import Path

    import example_sales

    # The defs folder without the code location's real resources bound.
    defs = dg.load_from_defs_folder(path_within_project=Path(example_sales.__file__).parent)

    class _Fabric:
        def run_item_job(self, workspace_id, item_id, job_type, execution_data):
            assert job_type == "Pipeline"
            return {"id": "job-1"}

    key = dg.AssetKey(["fabric", "sales", "lh_raw", "dbo", "orders"])
    [asset] = [a for a in defs.assets or [] if isinstance(a, dg.AssetsDefinition) and key in a.keys]
    result = dg.materialize([asset], resources={"fabric": _Fabric()})

    assert result.success
    [event] = result.get_asset_materialization_events()
    assert event.materialization.metadata["fabric_job_instance_id"].value == "job-1"
