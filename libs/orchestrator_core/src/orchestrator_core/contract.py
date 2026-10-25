"""The environment contract the infra repo publishes (docs/PLAN.md §3.1).

These models are the schema. Unknown fields are ignored so the infra repo can add outputs
without breaking this repo; a breaking change bumps `schema_version`.
"""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel


class Identity(BaseModel):
    resource_id: str
    client_id: str
    principal_name: str


class Identities(BaseModel):
    platform: Identity
    webserver: Identity
    admin: Identity
    domains: dict[str, Identity]


class Postgres(BaseModel):
    host: str
    database: str


class ComputeLogs(BaseModel):
    account: str
    container: str


class Auth(BaseModel):
    tenant_id: str
    ui_app_client_id: str


class Ingress(BaseModel):
    hostname: str


class Databricks(BaseModel):
    host: str
    # mpo_ops's SQL warehouse for the billing system tables (cost.py); None: no Databricks cost.
    cost_warehouse_id: str | None = None


class SnowflakeDomain(BaseModel):
    role: str
    warehouse: str


class Snowflake(BaseModel):
    account: str
    domains: dict[str, SnowflakeDomain]
    # Your contract's price per credit; None: cost is reported in credits only.
    credit_price_usd: float | None = None


class Insights(BaseModel):
    """The data collection rule mpo_ops sends cost and insight rows to (log_ingestion.py)."""

    ingestion_endpoint: str
    rule_id: str


class Platforms(BaseModel):
    databricks: Databricks | None = None
    snowflake: Snowflake | None = None


class Contract(BaseModel):
    # 2: one webserver with Entra roles (no read-only app or hostname).
    schema_version: Literal[2]
    environment: Literal["dev", "test", "prod"]
    location: str
    subscription_id: str
    resource_group: str
    aca_environment_id: str
    acr_login_server: str
    postgres: Postgres
    compute_logs: ComputeLogs
    identities: Identities
    auth: Auth
    ingress: Ingress
    platforms: Platforms
    insights: Insights | None = None


def load_contract(path: str | Path) -> Contract:
    return Contract.model_validate_json(Path(path).read_text())
