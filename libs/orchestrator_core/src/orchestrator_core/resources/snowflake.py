"""Snowflake connections without secrets: workload identity federation in Azure, browser SSO
locally."""

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

import dagster as dg
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection


class SnowflakeResource(dg.ConfigurableResource):
    account: str
    warehouse: str
    role: str
    # Local development only: your own Snowflake login, signed in through the browser.
    user: str | None = None

    def _auth(self) -> dict[str, Any]:
        # Container Apps sets IDENTITY_ENDPOINT. The connector then gets an Entra token for the
        # container's managed identity (AZURE_CLIENT_ID) and Snowflake maps it to the domain's
        # service user, which trusts exactly that identity.
        if "IDENTITY_ENDPOINT" in os.environ:
            return {"authenticator": "WORKLOAD_IDENTITY", "workload_identity_provider": "AZURE"}
        return {"authenticator": "externalbrowser", "user": self.user}

    @contextmanager
    def connect(self, query_tag: str | None = None) -> Iterator[SnowflakeConnection]:
        """`query_tag` marks every query of the session (cost attribution, query_tag.py)."""
        connection = snowflake.connector.connect(
            account=self.account,
            warehouse=self.warehouse,
            role=self.role,
            session_parameters={"QUERY_TAG": query_tag} if query_tag else None,
            **self._auth(),
        )
        try:
            yield connection
        finally:
            connection.close()

    def execute(self, sql: str, query_tag: str | None = None) -> list[str]:
        """Runs one or more `;`-separated statements; returns their Snowflake query IDs."""
        with self.connect(query_tag) as connection:
            return [cursor.sfqid or "" for cursor in connection.execute_string(sql)]

    def fetch(self, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        """One query with `%(name)s` parameters; rows as dicts keyed by upper-case column."""
        with self.connect() as connection, connection.cursor(DictCursor) as cursor:
            cursor.execute(sql, dict(params))
            return list(cursor.fetchall())  # type: ignore[arg-type]
