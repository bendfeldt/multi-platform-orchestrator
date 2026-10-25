import json

import pytest
import yaml
from orchestrator_core.contract import load_contract
from render import CODE_LOCATIONS, REPO, app_name, render

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


def test_azure_workspace_lists_every_code_location():
    workspace = yaml.safe_load((REPO / "deploy/workspace.azure.yaml").read_text())
    entries = [entry["grpc_server"] for entry in workspace["load_from"]]
    assert sorted(e["location_name"] for e in entries) == CODE_LOCATIONS
    assert all(e["host"] == app_name(e["location_name"]) for e in entries)
