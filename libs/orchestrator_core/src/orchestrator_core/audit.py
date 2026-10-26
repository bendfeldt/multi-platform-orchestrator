"""Audit log of what people do in the Dagster UI (docs/PRO_FEATURES_PLAN.md §3).

`AuditedWebserver` (webserver.py) calls `log_mutations` for every GraphQL request Dagster
executes over HTTP, whatever form it came in (JSON, `application/graphql`, GET or a query
string). Each mutation request is one JSON line on stdout, with the signed-in user; Container
Apps sends stdout to Log Analytics (`ContainerAppConsoleLogs_CL`). Websockets carry only
subscriptions, which change nothing.
"""

import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from graphql import FieldNode, GraphQLError, OperationDefinitionNode, OperationType, parse

_USER_HEADER = "x-ms-client-principal-name"
_USER_ID_HEADER = "x-ms-client-principal-id"


def mutation_fields(query: str) -> list[str]:
    """The top-level fields of the query's mutations; [] for queries or unparsable text."""
    try:
        definitions = parse(query).definitions
    except GraphQLError:
        return []  # Dagster refuses it too
    return [
        selection.name.value
        for definition in definitions
        if isinstance(definition, OperationDefinitionNode)
        and definition.operation == OperationType.MUTATION
        for selection in definition.selection_set.selections
        if isinstance(selection, FieldNode)
    ]


def log_mutations(
    headers: Mapping[str, str],
    query: str,
    operation: str | None,
    variables: Any,
    status: int,
    body: bytes,
) -> None:
    mutations = mutation_fields(query)
    if not mutations:
        return
    record = {
        "mpo_audit": "graphql_mutation",
        "time": datetime.now(UTC).isoformat(),
        "user": headers.get(_USER_HEADER),
        "user_id": headers.get(_USER_ID_HEADER),
        "operation": operation,
        "mutations": mutations,
        "variables": variables,
        "http_status": status,
        "denied": b"UnauthorizedError" in body,
    }
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)
