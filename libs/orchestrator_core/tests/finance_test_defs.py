"""A second code location for the access tests, with its own asset."""

import dagster as dg


@dg.asset(key=["snowflake", "finance", "mart", "ledger"])
def ledger() -> None: ...


defs = dg.Definitions(assets=[ledger])
