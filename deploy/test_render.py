import json

import pytest
import yaml
from orchestrator_core.contract import load_contract
from render import CODE_LOCATIONS, REPO, WORKSPACE, Preview, app_name, render, workspace_yaml

CONTRACT = load_contract(REPO / "libs/orchestrator_core/tests/contract.dev.json")
IMAGES = {"platform": "acr.io/mpo-platform@sha256:p"} | {
    location: f"acr.io/mpo-{location}@sha256:c" for location in CODE_LOCATIONS
}


def _identity(resource: dict) -> tuple[str, str]:
    """The attached identity and the client ID the container's code will use."""
    [attached] = resource["identity"]["userAssignedIdentities"]
    env = {
        e["name"]: e["value"] for e in resource["properties"]["template"]["containers"][0]["env"]
    }
    return attached, env["AZURE_CLIENT_ID"]


def test_every_container_runs_as_the_intended_identity():
    resources = render(CONTRACT, IMAGES)
    ids = CONTRACT.identities
    expected = {
        "app-dagster-web.yaml": ids.webserver,
        "app-dagster-web.initial.yaml": ids.webserver,
        "app-dagster-daemon.yaml": ids.platform,
        "job-dagster-migrate.yaml": ids.admin,
        "job-dagster-cleanup.yaml": ids.admin,
        "job-dagster-health.yaml": ids.admin,
        "job-dagster-catalog.yaml": ids.domains["mpo_ops"],
    } | {f"app-{app_name(loc)}.yaml": ids.domains[loc] for loc in CODE_LOCATIONS}

    assert set(resources) == set(expected)
    for name, identity in expected.items():
        assert _identity(resources[name]) == (identity.resource_id, identity.client_id), name


def test_runs_inherit_the_domain_identity_not_the_daemons():
    resources = render(CONTRACT, IMAGES)
    for location in CODE_LOCATIONS:
        command = resources[f"app-{app_name(location)}.yaml"]["properties"]["template"][
            "containers"
        ][0]["command"]
        context = json.loads(command[command.index("--container-context") + 1])["aca"]
        domain = CONTRACT.identities.domains[location]
        assert context["identity_id"] == domain.resource_id
        assert f"AZURE_CLIENT_ID={domain.client_id}" in context["env_vars"]
        assert f"DAGSTER_PG_USER={domain.principal_name}" in context["env_vars"]


def test_only_mpo_ops_gets_the_cost_and_insight_settings():
    resources = render(CONTRACT, IMAGES)
    ops = {"DATABRICKS_COST_WAREHOUSE_ID", "SNOWFLAKE_CREDIT_PRICE_USD", "MPO_INGESTION_ENDPOINT",
           "MPO_INGESTION_RULE_ID", "FABRIC_EVENTS_QUERY_URI", "FABRIC_EVENTS_DATABASE",
           "FABRIC_CU_HOUR_PRICE_USD"}  # fmt: skip
    for location in CODE_LOCATIONS:
        container = resources[f"app-{app_name(location)}.yaml"]["properties"]["template"][
            "containers"
        ][0]
        names = {e["name"] for e in container["env"]}
        command = container["command"]
        run_env = json.loads(command[command.index("--container-context") + 1])["aca"]["env_vars"]
        run_names = {item.split("=", 1)[0] for item in run_env}
        if location == "mpo_ops":
            assert ops <= names and ops <= run_names  # the jobs run as their own containers
        else:
            assert not ops & names and not ops & run_names, location


def test_only_the_webserver_is_reachable_from_front_door():
    resources = render(CONTRACT, IMAGES)
    external = {
        name
        for name, r in resources.items()
        if r["properties"]["configuration"].get("ingress", {}).get("external")
    }
    assert external == {"app-dagster-web.yaml"}


def test_webserver_runs_with_entra_roles_and_audit():
    resources = render(CONTRACT, IMAGES)
    for name in ("app-dagster-web.yaml", "app-dagster-web.initial.yaml"):
        command = resources[name]["properties"]["template"]["containers"][0]["command"]
        assert command[:3] == ["python", "-m", "orchestrator_core.webserver"], name


def test_code_location_without_identity_is_refused():
    data = CONTRACT.model_dump()
    data["identities"]["domains"] = {}
    with pytest.raises(ValueError, match="no identity"):
        render(type(CONTRACT).model_validate(data), IMAGES)


def _mounted_workspace(resource: dict) -> str:
    properties = resource["properties"]
    [secret] = properties["configuration"]["secrets"]
    [volume] = properties["template"]["volumes"]
    [mount] = properties["template"]["containers"][0]["volumeMounts"]
    assert volume["storageType"] == "Secret" and volume["secrets"][0]["secretRef"] == secret["name"]
    assert f"{mount['mountPath']}/{volume['secrets'][0]['path']}" == WORKSPACE
    return secret["value"]


def test_workspace_lists_every_code_location_and_reaches_its_readers():
    resources = render(CONTRACT, IMAGES)
    entries = [e["grpc_server"] for e in yaml.safe_load(workspace_yaml())["load_from"]]
    assert sorted(e["location_name"] for e in entries) == CODE_LOCATIONS
    assert all(e["host"] == app_name(e["location_name"]) for e in entries)
    readers = ("app-dagster-web.yaml", "app-dagster-daemon.yaml", "job-dagster-health.yaml",
               "job-dagster-catalog.yaml")  # fmt: skip
    for name in readers:
        assert _mounted_workspace(resources[name]) == workspace_yaml(), name


def _env(resource: dict) -> dict[str, str]:
    return {
        e["name"]: e["value"] for e in resource["properties"]["template"]["containers"][0]["env"]
    }


def test_a_preview_runs_as_its_domain_and_rolls_the_readers():
    preview = Preview("example_sales", 12)
    images = IMAGES | {preview.name: "acr.io/mpo-example-sales@sha256:pr12"}
    before, after = render(CONTRACT, IMAGES), render(CONTRACT, images, (preview,))
    app = after["app-cl-example-sales-pr12.yaml"]
    assert app["tags"] == {"mpo-preview": "example_sales:12"}
    assert "tags" not in after["app-cl-example-sales.yaml"]
    domain = CONTRACT.identities.domains["example_sales"]
    assert _identity(app) == (domain.resource_id, domain.client_id)
    container = app["properties"]["template"]["containers"][0]
    assert container["image"] == "acr.io/mpo-example-sales@sha256:pr12"
    command = container["command"]
    assert command[command.index("-m") + 1] == "orchestrator_core.preview"
    run_env = json.loads(command[command.index("--container-context") + 1])["aca"]["env_vars"]
    assert {"MPO_PREVIEW_MODULE=example_sales.definitions", "MPO_PREVIEW_PR=12"} <= set(run_env)
    hosts = [
        e["grpc_server"]["host"] for e in yaml.safe_load(workspace_yaml((preview,)))["load_from"]
    ]
    assert "cl-example-sales-pr12" in hosts
    for name in ("app-dagster-web.yaml", "app-dagster-daemon.yaml", "job-dagster-health.yaml"):
        assert _mounted_workspace(after[name]) == workspace_yaml((preview,))
        # A new workspace changes the template, so the app gets a new revision that reads it.
        assert (
            _env(after[name])["MPO_WORKSPACE_SHA256"] != _env(before[name])["MPO_WORKSPACE_SHA256"]
        )


@pytest.mark.parametrize("location", ["mpo_ops", "nowhere"])
def test_previews_only_of_domains(location):
    with pytest.raises(ValueError, match="no preview"):
        render(CONTRACT, IMAGES, (Preview(location, 1),))


def test_previews_only_in_dev():
    data = CONTRACT.model_dump() | {"environment": "test"}
    with pytest.raises(ValueError, match="dev only"):
        render(type(CONTRACT).model_validate(data), IMAGES, (Preview("example_sales", 1),))


def test_servers_have_probes():
    resources = render(CONTRACT, IMAGES)

    def probes(name: str) -> dict:
        container = resources[name]["properties"]["template"]["containers"][0]
        return {p["type"]: p for p in container["probes"]}

    web = probes("app-dagster-web.yaml")
    assert web["Liveness"]["httpGet"] == {"path": "/server_info", "port": 3000}
    assert set(web) == {"Startup", "Liveness", "Readiness"}
    for location in CODE_LOCATIONS:
        server = probes(f"app-{app_name(location)}.yaml")
        assert server["Liveness"]["tcpSocket"] == {"port": 4000}


def test_no_catalog_job_without_purview():
    data = CONTRACT.model_dump() | {"catalog": None}
    assert "job-dagster-catalog.yaml" not in render(type(CONTRACT).model_validate(data), IMAGES)
