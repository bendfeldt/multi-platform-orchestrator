"""Dagster's graph in Purview: the real example domain through a real workspace; Purview's
API is mocked."""

import json

import dagster as dg
import pytest
import responses
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import ModuleTarget
from orchestrator_core import purview

API = "https://contoso.purview.azure.com/datamap/api"
MART = dg.AssetKey(["snowflake", "sales", "mart", "fct_orders"])
SILVER = dg.AssetKey(["databricks", "sales", "silver", "orders"])


def _body(request) -> str | bytes:
    assert isinstance(request.body, str | bytes)
    return request.body


@pytest.fixture(scope="module")
def infos():
    with dg.instance_for_test() as instance:
        target = ModuleTarget(
            module_name="example_sales.definitions",
            attribute=None,
            working_directory=None,
            location_name="example_sales",
        )
        with WorkspaceProcessContext(instance, target) as context:
            return purview.from_asset_graph(context.create_request_context().asset_graph)


def test_the_workspace_graph_becomes_assets_and_processes(infos):
    datasets, processes = purview.entities(infos)
    mart = next(
        d
        for d in datasets
        if d["attributes"]["qualifiedName"] == "dagster://snowflake/sales/mart/fct_orders"
    )
    assert mart["typeName"] == "dagster_asset"
    attrs = mart["attributes"]
    assert (attrs["codeLocation"], attrs["kinds"], attrs["owners"]) == (
        "example_sales",
        "snowflake",
        "team:sales-data",
    )
    assert json.loads(attrs["columnSchema"])[0] == {"name": "order_id", "type": "number"}
    produce = next(
        p
        for p in processes
        if p["attributes"]["outputs"][0]["uniqueAttributes"]["qualifiedName"].endswith("fct_orders")
    )
    assert produce["attributes"]["inputs"] == [
        {
            "typeName": "dagster_asset",
            "uniqueAttributes": {"qualifiedName": "dagster://databricks/sales/silver/orders"},
        }
    ]
    [mapping] = json.loads(produce["attributes"]["columnMapping"])
    assert mapping["DatasetMapping"] == {
        "Source": "dagster://databricks/sales/silver/orders",
        "Sink": "dagster://snowflake/sales/mart/fct_orders",
    }
    assert {"Source": "amount", "Sink": "amount"} in mapping["ColumnMapping"]
    assert len(datasets) == 4  # every asset of the location


@responses.activate
def test_sync_sends_assets_before_processes_with_an_entra_token(infos):
    for name in ("dagster_asset", "dagster_process"):
        responses.get(f"{API}/atlas/v2/types/entitydef/name/{name}", json={"name": name})
    responses.post(f"{API}/atlas/v2/entity/bulk", json={})
    client = purview.Purview("https://contoso.purview.azure.com/", lambda: "tok")

    assets, processes = purview.sync(client, infos)

    posts = [c.request for c in responses.calls if c.request.method == "POST"]
    first, second = (json.loads(_body(p))["entities"] for p in posts)
    assert {e["typeName"] for e in first} == {"dagster_asset"} and len(first) == assets
    assert {e["typeName"] for e in second} == {"dagster_process"} and len(second) == processes
    assert all("api-version=2023-09-01" in (p.url or "") for p in posts)
    assert posts[0].headers["Authorization"] == "Bearer tok"


@responses.activate
def test_sync_refuses_without_the_types_and_the_admin_command_creates_them():
    responses.get(f"{API}/atlas/v2/types/entitydef/name/dagster_asset", status=404)
    responses.get(f"{API}/atlas/v2/types/entitydef/name/dagster_process", json={})
    responses.post(f"{API}/atlas/v2/types/typedefs", json={})
    client = purview.Purview("https://contoso.purview.azure.com", lambda: "tok")
    with pytest.raises(RuntimeError, match="Purview admin"):
        purview.sync(client, [])
    client.create_typedefs()
    created = json.loads(_body(responses.calls[-1].request))
    assert [d["name"] for d in created["entityDefs"]] == ["dagster_asset"]
    assert created["entityDefs"][0]["superTypes"] == ["DataSet"]
