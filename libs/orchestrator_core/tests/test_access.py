"""Roles and audit against the real Dagster GraphQL API (no mocks), with two code locations."""

import base64
import json
from pathlib import Path

import dagster as dg
import pytest
from dagster._core.remote_origin import InProcessCodeLocationOrigin
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin
from dagster._core.workspace.load_target import InProcessWorkspaceLoadTarget
from orchestrator_core.access import Access, EntraWorkspaceProcessContext, access_from_headers
from orchestrator_core.run_coordinator import LAUNCHED_BY_TAG
from orchestrator_core.webserver import AuditedWebserver
from starlette.testclient import TestClient

# Each location owns different assets: Dagster checks an asset launch against the location that
# defines the asset, so one table must have exactly one writing domain.
LOCATIONS = {
    "example_sales": ("example_sales.definitions", ["fabric", "sales", "lh_raw", "dbo", "orders"]),
    "finance": ("finance_test_defs", ["snowflake", "finance", "mart", "ledger"]),
}
ROLE_URI = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"


def principal(*roles: str, claim_type: str = ROLE_URI) -> dict[str, str]:
    data = {
        "auth_typ": "aad",
        "role_typ": ROLE_URI,
        "claims": [{"typ": claim_type, "val": r} for r in roles],
    }
    return {
        "x-ms-client-principal": base64.b64encode(json.dumps(data).encode()).decode(),
        "x-ms-client-principal-name": "anna@contoso.com",
    }


@pytest.fixture(scope="module")
def client():
    origins = [
        InProcessCodeLocationOrigin(
            loadable_target_origin=LoadableTargetOrigin(
                module_name=module, attribute="defs", working_directory=str(Path(__file__).parent)
            ),
            location_name=name,
        )
        for name, (module, _) in LOCATIONS.items()
    ]
    with dg.instance_for_test(
        overrides={
            "run_coordinator": {
                "module": "orchestrator_core.run_coordinator",
                "class": "AuditedRunCoordinator",
            }
        }
    ) as instance:
        with EntraWorkspaceProcessContext(
            instance, workspace_load_target=InProcessWorkspaceLoadTarget(origins), version="test"
        ) as context:
            yield TestClient(AuditedWebserver(context).create_asgi_app()), instance


def gql(client, query: str, headers: dict[str, str]) -> dict:
    return client.post("/graphql", json={"query": query}, headers=headers).json()["data"]


def permissions(client, headers) -> dict[str, dict[str, bool]]:
    data = gql(
        client,
        "{ workspaceOrError { ... on Workspace { locationEntries "
        "{ name permissions { permission value } } } } }",
        headers,
    )
    return {
        e["name"]: {p["permission"]: p["value"] for p in e["permissions"]}
        for e in data["workspaceOrError"]["locationEntries"]
    }


def reload(client, location: str, headers) -> str:
    query = 'mutation { reloadRepositoryLocation(repositoryLocationName: "%s") { __typename } }'
    return gql(client, query % location, headers)["reloadRepositoryLocation"]["__typename"]


LAUNCH = """mutation { launchRun(executionParams: { mode: "default", selector: {
  repositoryLocationName: "%s", repositoryName: "__repository__", jobName: "__ASSET_JOB",
  assetSelection: [{ path: %s }] } })
  { __typename ... on LaunchRunSuccess { run { tags { key value } } } } }"""


def launch(location: str) -> str:
    return LAUNCH % (location, json.dumps(LOCATIONS[location][1]))


@pytest.mark.parametrize(
    ("headers", "allowed"),
    [
        ({}, set()),
        (principal("Dagster.Viewer"), set()),
        (principal("Dagster.Editor.finance"), {"finance"}),
        (principal("Dagster.Editor.example_sales"), {"example_sales"}),
        (principal("Dagster.Admin"), set(LOCATIONS)),
    ],
    ids=["anonymous", "viewer", "editor-other", "editor-own", "admin"],
)
def test_write_access_only_where_the_role_allows(client, headers, allowed):
    client, _ = client
    for location in LOCATIONS:
        expected = "LaunchRunSuccess" if location in allowed else "UnauthorizedError"
        assert gql(client, launch(location), headers)["launchRun"]["__typename"] == expected, (
            location
        )


def test_reload_is_refused_without_an_editor_role(client):
    client, _ = client
    for headers in ({}, principal("Dagster.Launcher.example_sales")):
        assert reload(client, "example_sales", headers) == "UnauthorizedError"


def test_launcher_runs_jobs_but_changes_nothing_else(client):
    client, _ = client
    perms = permissions(client, principal("Dagster.Launcher.example_sales"))
    assert perms["example_sales"]["launch_pipeline_execution"]
    assert perms["example_sales"]["terminate_pipeline_execution"]
    assert not perms["example_sales"]["edit_sensor"]
    assert not perms["example_sales"]["wipe_assets"]
    assert not perms["finance"]["launch_pipeline_execution"]


def test_only_admins_change_workspace_wide_settings(client):
    client, _ = client
    query = "{ permissions { permission value } }"
    for role, allowed in (("Dagster.Editor.example_sales", False), ("Dagster.Admin", True)):
        perms = {
            p["permission"]: p["value"] for p in gql(client, query, principal(role))["permissions"]
        }
        assert perms["reload_workspace"] is allowed, role
        assert perms["edit_concurrency_limit"] is allowed, role
    # Executing it for a non-admin is refused (reloading in-process locations is not possible).
    denied = gql(
        client,
        "mutation { reloadWorkspace { __typename } }",
        principal("Dagster.Editor.example_sales"),
    )
    assert denied["reloadWorkspace"]["__typename"] == "UnauthorizedError"


def test_ui_launch_is_tagged_with_the_user_and_audited(client, capsys):
    client, _ = client
    result = gql(client, launch("example_sales"), principal("Dagster.Launcher.example_sales"))
    assert result["launchRun"]["__typename"] == "LaunchRunSuccess"
    tags = {t["key"]: t["value"] for t in result["launchRun"]["run"]["tags"]}
    assert tags[LAUNCHED_BY_TAG] == "anna@contoso.com"

    denied = gql(client, launch("finance"), principal("Dagster.Launcher.example_sales"))
    assert denied["launchRun"]["__typename"] == "UnauthorizedError"

    records = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith('{"mpo_audit"')
    ]
    assert [(r["user"], r["mutations"], r["denied"]) for r in records[-2:]] == [
        ("anna@contoso.com", ["launchRun"], False),
        ("anna@contoso.com", ["launchRun"], True),
    ]


def test_every_request_form_that_runs_a_mutation_is_audited(client, capsys):
    """Dagster also runs GraphQL sent as GET, as raw text and in the query string."""
    client, _ = client
    headers = principal("Dagster.Launcher.example_sales")
    query = launch("example_sales")
    responses = [
        client.post("/graphql", json={"query": query}, headers=headers),
        client.post("/graphql", content=query,
                    headers=headers | {"content-type": "application/graphql"}),
        client.get("/graphql", params={"query": query}, headers=headers),
        client.post("/graphql", params={"query": query}, headers=headers),
    ]  # fmt: skip
    assert [r.json()["data"]["launchRun"]["__typename"] for r in responses] == (
        ["LaunchRunSuccess"] * 4
    )
    records = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith('{"mpo_audit"')
    ]
    assert [r["mutations"] for r in records] == [["launchRun"]] * 4
    assert {r["user"] for r in records} == {"anna@contoso.com"}


@pytest.mark.parametrize(
    "header",
    [
        "not-base64!",
        base64.b64encode(b"[1, 2]").decode(),
        base64.b64encode(b'{"claims": 5}').decode(),
    ],
)
def test_malformed_principal_grants_nothing(header):
    assert access_from_headers({"x-ms-client-principal": header}) == Access()


def test_roles_claim_with_short_type_and_highest_role_wins():
    headers = principal(
        "Dagster.Launcher.example_sales",
        "Dagster.Editor.example_sales",
        "Other.App",
        claim_type="roles",
    )
    assert access_from_headers(headers) == Access(location_roles={"example_sales": "Editor"})


def test_a_preview_has_the_roles_of_its_code_location():
    from orchestrator_core.access import Access

    access = Access(location_roles={"example_sales": "Launcher", "finance": "Editor"})
    assert access.role_for("example_sales_pr12") == "Launcher"
    assert access.role_for("finance_pr3") == "Editor"
    assert access.role_for("example_sales") == "Launcher"
    assert access.role_for("marketing_pr1") is None and access.role_for("example") is None
