"""Rows for Log Analytics custom tables through the Logs Ingestion API, with the process's
Entra identity (docs/PRO_FEATURES_PLAN.md §5.3). The infra repo creates the tables and the
data collection rule and gives mpo_ops "Monitoring Metrics Publisher" on that rule only.

Streams (one per table, `Custom-<table>`): COST_STREAM and INSIGHTS_STREAM.
"""

import os
from collections.abc import Mapping, Sequence
from typing import Any

from orchestrator_core.auth import credential

ENDPOINT_ENV = "MPO_INGESTION_ENDPOINT"  # the rule's logs ingestion endpoint
RULE_ENV = "MPO_INGESTION_RULE_ID"  # the rule's immutable ID
COST_STREAM = "Custom-MpoCost_CL"
INSIGHTS_STREAM = "Custom-MpoInsights_CL"


def configured() -> bool:
    return bool(os.environ.get(ENDPOINT_ENV) and os.environ.get(RULE_ENV))


def send(stream: str, rows: Sequence[Mapping[str, Any]]) -> int:
    """Uploads the rows (the client splits them into 1 MB calls); returns how many were sent.
    Without the settings (local runs) nothing is sent."""
    if not rows or not configured():
        return 0
    # Imported here: only mpo_ops processes send rows.
    from azure.monitor.ingestion import LogsIngestionClient

    client = LogsIngestionClient(endpoint=os.environ[ENDPOINT_ENV], credential=credential())
    with client:
        client.upload(
            rule_id=os.environ[RULE_ENV], stream_name=stream, logs=[dict(r) for r in rows]
        )
    return len(rows)
