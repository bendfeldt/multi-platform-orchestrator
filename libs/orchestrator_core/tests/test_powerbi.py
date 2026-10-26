"""Power BI reports and dashboards join the graph under canonical keys (Power BI's API is
mocked; dagster-powerbi and the translator are real)."""

import dagster as dg
import pytest
import responses
from dagster.components.testing import create_defs_folder_sandbox
from orchestrator_core import powerbi

API = "https://api.powerbi.com/v1.0/myorg/groups/ws-1"
MODEL = dg.AssetKey(["fabric", "sales", "semantic_model", "sales"])
REPORT = dg.AssetKey(["fabric", "sales", "report", "sales_overview"])
DASHBOARD = dg.AssetKey(["fabric", "sales", "dashboard", "sales_kpis"])


@pytest.fixture
def power_bi(monkeypatch):
    monkeypatch.setattr(powerbi, "get_token", lambda scope: f"token-for-{scope}")
    with responses.RequestsMock() as mock:
        mock.get(
            f"{API}/dashboards",
            json={"value": [{"id": "d1", "displayName": "Sales KPIs.dashboard"}]},
        )
        mock.get(f"{API}/dashboards/d1/tiles", json={"value": [{"id": "t1", "reportId": "r1"}]})
        mock.get(
            f"{API}/reports",
            json={"value": [{"id": "r1", "name": "Sales Overview", "datasetId": "m1"}]},
        )
        mock.get(f"{API}/datasets", json={"value": [{"id": "m1", "name": "Sales"}]})
        mock.get(
            f"{API}/datasets/m1/datasources",
            json={
                "value": [
                    {"datasourceType": "Snowflake", "connectionDetails": {"database": "SALES"}}
                ]
            },
        )
        yield mock


def _load(tmp_attributes: str, instance):
    with create_defs_folder_sandbox() as sandbox:
        path = sandbox.defs_folder_path / "powerbi"
        path.mkdir()
        (path / "defs.yaml").write_text(
            "type: orchestrator_core.powerbi.PowerBIReports\n"
            "attributes:\n"
            "  workspace_id: ws-1\n"
            "  workspace_name: sales\n" + tmp_attributes
        )
        with sandbox.load_component_and_build_defs(defs_path=path) as (_, defs):
            return {s.key: s for s in defs.assets or [] if isinstance(s, dg.AssetSpec)}


def test_reports_and_dashboards_get_canonical_keys_and_lineage(power_bi):
    with dg.instance_for_test() as instance:
        specs = _load("  owners: [team:sales-bi]\n  tags: {layer: bi}\n", instance)
    assert set(specs) == {REPORT, DASHBOARD}  # the refresh component owns the semantic model
    assert {d.asset_key for d in specs[REPORT].deps} == {MODEL}
    assert {d.asset_key for d in specs[DASHBOARD].deps} == {REPORT}
    assert specs[REPORT].owners == ["team:sales-bi"] and specs[REPORT].tags["layer"] == "bi"
    # Our token, for Power BI, from the managed identity: no client secret anywhere.
    auth = power_bi.calls[0].request.headers["Authorization"]
    assert auth == "Bearer token-for-https://analysis.windows.net/powerbi/api/.default"


def test_semantic_models_only_when_asked(power_bi):
    with dg.instance_for_test() as instance:
        specs = _load("  include_semantic_models: true\n", instance)
    assert MODEL in specs and list(specs[MODEL].deps) == []
