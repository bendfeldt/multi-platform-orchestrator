"""Role-based access per code location for the Dagster UI (docs/PRO_FEATURES_PLAN.md §2).

Container Apps' built-in Entra sign-in passes the user's claims in `X-MS-CLIENT-PRINCIPAL`
(Base64 JSON); outside callers cannot set that header. The app roles on the sign-in app are:

    Dagster.Viewer                read everything (also what any signed-in user gets)
    Dagster.Launcher.<location>   launch, re-execute and terminate runs; launch/cancel backfills
    Dagster.Editor.<location>     everything scoped to that code location
    Dagster.Admin                 everything, including workspace-wide settings

Dagster's GraphQL layer enforces these per code location, also for existing runs.
"""

import base64
import binascii
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dagster._core.workspace.context import WorkspaceProcessContext, WorkspaceRequestContext
from dagster._core.workspace.permissions import (
    LOCATION_SCOPED_PERMISSIONS,
    PermissionResult,
    Permissions,
    get_location_scoped_user_permissions,
)

# Container Apps may map the token's `roles` claim to this URI (it names it in `role_typ`).
_ROLE_CLAIM_TYPES = {"roles", "http://schemas.microsoft.com/ws/2008/06/identity/claims/role"}
ADMIN_ROLE = "Dagster.Admin"
_RANK = {"Launcher": 1, "Editor": 2}

LAUNCHER_PERMISSIONS = frozenset(
    {
        Permissions.LAUNCH_PIPELINE_EXECUTION,
        Permissions.LAUNCH_PIPELINE_REEXECUTION,
        Permissions.TERMINATE_PIPELINE_EXECUTION,
        Permissions.LAUNCH_PARTITION_BACKFILL,
        Permissions.CANCEL_PARTITION_BACKFILL,
    }
)
_DENIED = "Your Dagster role does not allow this in this code location."


@dataclass(frozen=True)
class Access:
    admin: bool = False
    location_roles: Mapping[str, str] = field(default_factory=dict)  # location -> Launcher|Editor


def access_from_headers(headers: Mapping[str, str]) -> Access:
    """Reads the roles from Container Apps' principal header. Anything missing or malformed is
    read-only: never more access than the header proves."""
    raw = headers.get("x-ms-client-principal")
    if not raw:
        return Access()
    try:
        principal = json.loads(base64.b64decode(raw, validate=True))
        claims = principal.get("claims") or []
        claim_types = _ROLE_CLAIM_TYPES | {principal.get("role_typ")}
        roles = {c["val"] for c in claims if isinstance(c, dict) and c.get("typ") in claim_types}
    except (ValueError, binascii.Error, TypeError, KeyError, AttributeError):
        return Access()

    location_roles: dict[str, str] = {}
    for role in roles:
        parts = role.split(".", 2)
        if len(parts) == 3 and parts[0] == "Dagster" and parts[1] in _RANK:
            location, level = parts[2], parts[1]
            if _RANK[level] > _RANK.get(location_roles.get(location, ""), 0):
                location_roles[location] = level
    return Access(admin=ADMIN_ROLE in roles, location_roles=location_roles)


class RoleRequestContext(WorkspaceRequestContext):
    """Workspace-wide settings only for admins; per-location rights from the user's roles."""

    def __init__(self, *, access: Access, **kwargs: Any):
        super().__init__(read_only=not access.admin, **kwargs)
        self._access = access

    def permissions_for_location(self, *, location_name: str) -> Mapping[str, PermissionResult]:
        role = self._access.location_roles.get(location_name)
        if self._access.admin or role == "Editor":
            return get_location_scoped_user_permissions(False)
        allowed = LAUNCHER_PERMISSIONS if role == "Launcher" else frozenset()
        return {
            permission: PermissionResult(
                enabled=permission in allowed,
                disabled_reason=None if permission in allowed else _DENIED,
            )
            for permission in LOCATION_SCOPED_PERMISSIONS
        }


class EntraWorkspaceProcessContext(WorkspaceProcessContext):
    """Builds every UI request's context from the signed-in user's Entra roles."""

    def create_request_context(self, source: object | None = None) -> WorkspaceRequestContext:
        headers = getattr(source, "headers", None) or {}
        return RoleRequestContext(
            access=access_from_headers(headers),
            instance=self.instance,
            current_workspace=self.get_current_workspace(),
            process_context=self,
            version=self.version,
            source=source,
        )
