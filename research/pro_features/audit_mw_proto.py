"""Prototype: ASGI middleware that logs every GraphQL mutation with the signed-in user."""

import json, sys, tempfile
import dagster as dg
from graphql import parse, OperationDefinitionNode
from starlette.middleware import Middleware
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster_webserver.webserver import DagsterWebserver
from starlette.testclient import TestClient

AUDIT = []


class GraphQLAuditMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] != "http"
            or scope["method"] != "POST"
            or not scope["path"].endswith("/graphql")
        ):
            return await self.app(scope, receive, send)
        chunks, more = [], True
        while more:
            message = await receive()
            chunks.append(message.get("body", b""))
            more = message.get("more_body", False)
        body = b"".join(chunks)
        try:
            payload = json.loads(body)
            doc = parse(payload["query"])
            for op in doc.definitions:
                if isinstance(op, OperationDefinitionNode) and op.operation.value == "mutation":
                    headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
                    AUDIT.append(
                        {
                            "user": headers.get("x-ms-client-principal-name"),
                            "fields": [s.name.value for s in op.selection_set.selections],
                            "variables": payload.get("variables"),
                        }
                    )
        except Exception as exc:  # never block a request because auditing failed to parse it
            AUDIT.append({"unparsed": str(exc)[:80]})
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        return await self.app(scope, replay, send)


class AuditedWebserver(DagsterWebserver):
    def build_middleware(self):
        return [*super().build_middleware(), Middleware(GraphQLAuditMiddleware)]


with tempfile.TemporaryDirectory() as home, dg.instance_for_test(temp_dir=home) as instance:
    t = ModuleTarget(
        module_name="example_sales.definitions",
        attribute="defs",
        working_directory=None,
        location_name="example_sales",
    )
    with WorkspaceProcessContext(instance, workspace_load_target=t, version="p") as ctx:
        c = TestClient(AuditedWebserver(ctx).create_asgi_app())
        h = {"x-ms-client-principal-name": "anna@contoso.com"}
        q = c.post("/graphql", json={"query": "{ version }"}, headers=h).json()
        m = c.post(
            "/graphql",
            json={
                "query": "mutation Reload($n: String!) { reloadRepositoryLocation(repositoryLocationName: $n) { __typename } }",
                "variables": {"n": "example_sales"},
            },
            headers=h,
        ).json()
        print("QUERY", q.get("data"), flush=True)
        print("MUTATION", m["data"]["reloadRepositoryLocation"]["__typename"], flush=True)
        print("AUDIT", AUDIT, flush=True)
