"""Prototype: per-code-location permissions from Easy Auth's X-MS-CLIENT-PRINCIPAL header."""

import base64, json, sys, tempfile
import dagster as dg
from dagster._core.workspace.context import WorkspaceProcessContext, WorkspaceRequestContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster_webserver.app import create_app_from_workspace_process_context
from starlette.testclient import TestClient

ROLE_CLAIM = "roles"


def principal_roles(conn) -> set[str]:
    raw = conn.headers.get("x-ms-client-principal") if conn is not None else None
    if not raw:
        return set()
    data = json.loads(base64.b64decode(raw))
    role_typ = data.get("role_typ", ROLE_CLAIM)
    return {c["val"] for c in data.get("claims", []) if c["typ"] in (ROLE_CLAIM, role_typ)}


class EntraProcessContext(WorkspaceProcessContext):
    def create_request_context(self, source=None) -> WorkspaceRequestContext:
        roles = principal_roles(source)
        locations = self.get_current_workspace().code_location_entries.keys()
        admin = "Dagster.Admin" in roles
        return WorkspaceRequestContext(
            instance=self._instance,
            current_workspace=self.get_current_workspace(),
            process_context=self,
            version=self.version,
            source=source,
            read_only=not admin,  # global permissions (reload workspace, concurrency) only for admins
            read_only_locations={
                loc: not (admin or f"Dagster.Operator.{loc}" in roles) for loc in locations
            },
        )


def header(*roles):
    p = {
        "auth_typ": "aad",
        "name_typ": "name",
        "role_typ": "http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
        "claims": [
            {"typ": "http://schemas.microsoft.com/ws/2008/06/identity/claims/role", "val": r}
            for r in roles
        ],
    }
    return {"x-ms-client-principal": base64.b64encode(json.dumps(p).encode()).decode()}


PERMS = "{ workspaceOrError { ... on Workspace { locationEntries { name permissions { permission value } } } } }"
RELOAD = 'mutation { reloadRepositoryLocation(repositoryLocationName: "%s") { __typename } }'
WIPE_ALL = "mutation { reloadWorkspace { __typename } }"

with tempfile.TemporaryDirectory() as home, dg.instance_for_test(temp_dir=home) as instance:
    target = ModuleTarget(
        module_name="example_sales.definitions",
        attribute="defs",
        working_directory=None,
        location_name="example_sales",
    )
    with EntraProcessContext(instance, workspace_load_target=target, version="proto") as ctx:
        client = TestClient(create_app_from_workspace_process_context(ctx))

        def gql(q, h):
            return client.post("/graphql", json={"query": q}, headers=h).json()

        for who, h in {
            "anonymous": {},
            "viewer": header("Dagster.Viewer"),
            "operator(other domain)": header("Dagster.Operator.finance"),
            "operator(example_sales)": header("Dagster.Operator.example_sales"),
            "admin": header("Dagster.Admin"),
        }.items():
            perms = gql(PERMS, h)["data"]["workspaceOrError"]["locationEntries"][0]["permissions"]
            launch = next(
                p["value"] for p in perms if p["permission"] == "launch_pipeline_execution"
            )
            reload_ = gql(RELOAD % "example_sales", h)["data"]["reloadRepositoryLocation"][
                "__typename"
            ]
            reload_ws = gql(WIPE_ALL, h)
            ws = (
                reload_ws.get("data", {}).get("reloadWorkspace", {}).get("__typename")
                if reload_ws.get("data")
                else reload_ws["errors"][0]["message"][:60]
            )
            print(
                f"{who:26} launch_perm={launch!s:5} reloadLocation={reload_:24} reloadWorkspace={ws}"
            )
