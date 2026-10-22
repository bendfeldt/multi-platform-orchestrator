"""Snowflake connections authenticated with Entra tokens (Snowflake External OAuth)."""

from collections.abc import Iterator
from contextlib import contextmanager

import dagster as dg
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from orchestrator_core.auth import get_token


class SnowflakeResource(dg.ConfigurableResource):
    account: str
    # Application ID URI of the Entra app the Snowflake External OAuth integration trusts,
    # e.g. "api://snowflake-sales/.default".
    oauth_scope: str
    warehouse: str
    role: str

    @contextmanager
    def connect(self) -> Iterator[SnowflakeConnection]:
        # A fresh token per connection, so long-running code never reuses an expired one.
        connection = snowflake.connector.connect(
            account=self.account,
            authenticator="oauth",
            token=get_token(self.oauth_scope),
            warehouse=self.warehouse,
            role=self.role,
        )
        try:
            yield connection
        finally:
            connection.close()

    def execute(self, sql: str) -> list[str]:
        """Runs one or more `;`-separated statements; returns their Snowflake query IDs."""
        with self.connect() as connection:
            return [cursor.sfqid or "" for cursor in connection.execute_string(sql)]
