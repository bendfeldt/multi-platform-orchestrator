"""Prototype: Launcher / Editor / Admin per code location by overriding permissions_for_location."""

import base64, json, tempfile
import dagster as dg
from dagster._core.workspace.context import WorkspaceProcessContext, WorkspaceRequestContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster._core.workspace.permissions import (
    LOCATION_SCOPED_PERMISSIONS,
    PermissionResult,
    Permissions,
    get_location_scoped_user_permissions,
)
from dagster_webserver.app import create_app_from_workspace_process_context
from starlette.testclient import TestClient

LAUNCHER = {
    Permissions.LAUNCH_PIPELINE_EXECUTION,
    Permissions.LAUNCH_PIPELINE_REEXECUTION,
    Permissions.TERMINATE_PIPELINE_EXECUTION,
    Permissions.LAUNCH_PARTITION_BACKFILL,
    Permissions.CANCEL_PARTITION_BACKFILL,
}


class RoleRequestContext(WorkspaceRequestContext):
    def __init__(self, *args, location_roles: dict[str, str], **kwargs):
        super().__init__(*args, **kwargs)
        self._location_roles = location_roles

    def permissions_for_location(self, *, location_name):
        role = self._location_roles.get(location_name)
        if role == "Editor":
            return get_location_scoped_user_permissions(False)
        allowed = LAUNCHER if role == "Launcher" else set()
        return {
            p: PermissionResult(
                enabled=p in allowed,
                disabled_reason=None if p in allowed else "Your role does not allow this.",
            )
            for p in LOCATION_SCOPED_PERMISSIONS
        }


class Ctx(WorkspaceProcessContext):
    def create_request_context(self, source=None):
        raw = source.headers.get("x-ms-client-principal") if source is not None else None
        roles = {c["val"] for c in json.loads(base64.b64decode(raw))["claims"]} if raw else set()
        loc_roles = {}
        for r in roles:  # e.g. Dagster.Launcher.example_sales
            parts = r.split(".")
            if len(parts) == 3 and parts[0] == "Dagster":
                loc_roles[parts[2]] = parts[1]
        return RoleRequestContext(
            instance=self._instance,
            current_workspace=self.get_current_workspace(),
            process_context=self,
            version=self.version,
            source=source,
            read_only=True,
            location_roles=loc_roles,
        )


def hdr(*roles):
    p = {"auth_typ": "aad", "claims": [{"typ": "roles", "val": r} for r in roles]}
    return {"x-ms-client-principal": base64.b64encode(json.dumps(p).encode()).decode()}


Q = "{ workspaceOrError { ... on Workspace { locationEntries { name permissions { permission value } } } } }"
with tempfile.TemporaryDirectory() as home, dg.instance_for_test(temp_dir=home) as instance:
    t = ModuleTarget(
        module_name="example_sales.definitions",
        attribute="defs",
        working_directory=None,
        location_name="example_sales",
    )
    with Ctx(instance, workspace_load_target=t, version="p") as ctx:
        c = TestClient(create_app_from_workspace_process_context(ctx))
        for who, h in {
            "launcher": hdr("Dagster.Launcher.example_sales"),
            "editor": hdr("Dagster.Editor.example_sales"),
        }.items():
            perms = {
                p["permission"]: p["value"]
                for p in c.post("/graphql", json={"query": Q}, headers=h).json()["data"][
                    "workspaceOrError"
                ]["locationEntries"][0]["permissions"]
            }
            print(
                who,
                "launch",
                perms["launch_pipeline_execution"],
                "terminate",
                perms["terminate_pipeline_execution"],
                "edit_sensor",
                perms["edit_sensor"],
                "start_schedule",
                perms["start_schedule"],
                "wipe",
                perms["wipe_assets"],
                flush=True,
            )
            r = c.post(
                "/graphql",
                json={
                    "query": 'mutation { reloadRepositoryLocation(repositoryLocationName: "example_sales") { __typename } }'
                },
                headers=h,
            ).json()
            print(
                who,
                "reloadLocation",
                r["data"]["reloadRepositoryLocation"]["__typename"],
                flush=True,
            )
