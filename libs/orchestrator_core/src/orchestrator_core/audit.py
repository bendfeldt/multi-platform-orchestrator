"""Audit log of what people do in the Dagster UI (docs/PRO_FEATURES_PLAN.md §3).

`GraphQLAuditMiddleware` writes one JSON line to stdout per GraphQL mutation (every UI action
that changes something), with the signed-in user. Container Apps sends stdout to Log Analytics
(`ContainerAppConsoleLogs_CL`). Webserver only: it needs the webserver's GraphQL packages.
"""

import json
import sys
from datetime import UTC, datetime
from typing import Any

from graphql import FieldNode, GraphQLError, OperationDefinitionNode, OperationType, parse

_USER_HEADER = "x-ms-client-principal-name"
_USER_ID_HEADER = "x-ms-client-principal-id"
# Enough of a mutation response to see whether Dagster refused it.
_RESPONSE_PEEK = 64 * 1024


def _mutation_fields(body: bytes) -> tuple[str | None, list[str], Any]:
    payload = json.loads(body)
    fields: list[str] = []
    for definition in parse(payload["query"]).definitions:
        if (
            isinstance(definition, OperationDefinitionNode)
            and definition.operation == OperationType.MUTATION
        ):
            fields += [
                s.name.value
                for s in definition.selection_set.selections
                if isinstance(s, FieldNode)
            ]
    return payload.get("operationName"), fields, payload.get("variables")


class GraphQLAuditMiddleware:
    """Pure ASGI middleware: reads the request body, replays it unchanged to the app."""

    def __init__(self, app: Any):
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if (
            scope["type"] != "http"
            or scope["method"] != "POST"
            or not scope["path"].endswith("/graphql")
        ):
            await self.app(scope, receive, send)
            return

        chunks, more = [], True
        while more:
            message = await receive()
            chunks.append(message.get("body", b""))
            more = message.get("more_body", False)
        body = b"".join(chunks)

        try:
            operation, mutations, variables = _mutation_fields(body)
        except (ValueError, KeyError, TypeError, GraphQLError):
            operation, mutations, variables = None, [], None  # Dagster answers malformed requests
        if not mutations:
            await self.app(scope, _replay(body, receive), send)
            return

        status, peek = 0, bytearray()

        async def capture(message: dict) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and len(peek) < _RESPONSE_PEEK:
                peek.extend(message.get("body", b"")[: _RESPONSE_PEEK - len(peek)])
            await send(message)

        try:
            await self.app(scope, _replay(body, receive), capture)
        finally:
            headers = {
                k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]
            }
            record = {
                "mpo_audit": "graphql_mutation",
                "time": datetime.now(UTC).isoformat(),
                "user": headers.get(_USER_HEADER),
                "user_id": headers.get(_USER_ID_HEADER),
                "operation": operation,
                "mutations": mutations,
                "variables": variables,
                "http_status": status,
                "denied": b"UnauthorizedError" in peek,
            }
            print(json.dumps(record, default=str), file=sys.stdout, flush=True)


def _replay(body: bytes, receive: Any) -> Any:
    sent = False

    async def replay() -> dict:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replay
