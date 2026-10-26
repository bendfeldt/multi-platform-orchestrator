"""The Dagster UI with Entra roles and an audit log; replaces `dagster-webserver` in Azure.

    python -m orchestrator_core.webserver -h 0.0.0.0 -p 3000 -w /opt/dagster/workspace.yaml

Mirrors `dagster-webserver`'s own startup (dagster_webserver/cli.py) with our process context
and middleware. Locally, the plain `dagster-webserver` still runs (no sign-in there).
"""

import argparse
from typing import Any

import uvicorn
from dagster import DagsterInstance
from dagster._core.workspace.load_target import WorkspaceFileTarget
from dagster_webserver.version import __version__
from dagster_webserver.webserver import DagsterWebserver
from starlette.requests import Request
from starlette.responses import JSONResponse

from orchestrator_core.access import EntraWorkspaceProcessContext
from orchestrator_core.audit import log_mutations

# dagster-webserver's defaults for its long-running database connections.
_STATEMENT_TIMEOUT_MS = 15000
_POOL_RECYCLE_S = 3600
_POOL_MAX_OVERFLOW = 20


class AuditedWebserver(DagsterWebserver):
    async def execute_graphql_request(
        self,
        request: Request,
        query: str,
        variables: dict[str, Any] | None,
        operation_name: str | None,
    ) -> JSONResponse:
        # Every HTTP GraphQL request reaches this method, after Dagster has read it from any
        # form; parsing the HTTP body ourselves missed GET and non-JSON requests.
        response = None
        try:
            response = await super().execute_graphql_request(
                request, query, variables, operation_name
            )
            return response
        finally:  # also when execution fails: the attempt is audited
            log_mutations(
                request.headers,
                query,
                operation_name,
                variables,
                response.status_code if response else 500,
                bytes(response.body) if response else b"",
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("-h", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=3000)
    parser.add_argument("-w", "--workspace", required=True)
    args = parser.parse_args()

    instance = DagsterInstance.get()
    instance.optimize_for_webserver(_STATEMENT_TIMEOUT_MS, _POOL_RECYCLE_S, _POOL_MAX_OVERFLOW)
    with EntraWorkspaceProcessContext(
        instance,
        workspace_load_target=WorkspaceFileTarget(paths=[args.workspace]),
        version=__version__,
    ) as context:
        uvicorn.run(AuditedWebserver(context).create_asgi_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
