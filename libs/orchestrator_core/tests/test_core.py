import json
from pathlib import Path

import pytest
from azure.identity import AzureCliCredential, ManagedIdentityCredential
from orchestrator_core import auth
from orchestrator_core.asset_keys import databricks_key, snowflake_key
from orchestrator_core.contract import load_contract
from pydantic import ValidationError

FIXTURE = Path(__file__).parent / "contract.dev.json"


def test_same_table_written_differently_gets_one_key():
    assert snowflake_key("SALES", "Mart ", "FCT_ORDERS") == snowflake_key(
        "sales", "mart", "fct_orders"
    )
    assert databricks_key("main", "silver", "orders").path == [
        "databricks",
        "main",
        "silver",
        "orders",
    ]


def test_empty_key_part_is_rejected():
    with pytest.raises(ValueError):
        snowflake_key("sales", " ", "orders")


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"IDENTITY_ENDPOINT": "http://localhost"}, ManagedIdentityCredential),
        ({}, AzureCliCredential),
    ],
)
def test_credential_follows_runtime(monkeypatch, env, expected):
    monkeypatch.delenv("IDENTITY_ENDPOINT", raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    auth._credential.cache_clear()
    assert isinstance(auth._credential(), expected)
    auth._credential.cache_clear()


def test_contract_fixture_loads():
    contract = load_contract(FIXTURE)
    assert contract.identities.domains["sales"].client_id


def test_contract_rejects_unknown_schema_version(tmp_path):
    data = json.loads(FIXTURE.read_text()) | {"schema_version": 2}
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValidationError):
        load_contract(path)
