"""Snowflake query tags that tie platform cost to Dagster runs (docs/PRO_FEATURES_PLAN.md §5.2).

    {"mpo_run":"<run id>","mpo_step":"<step key>"}                         SnowflakeSql, dbt
    {"mpo_run":"<run id>","mpo_step":"<step key>","mpo_dbt_node":"<id>"}   dbt models (macro)

dbt adds `mpo_dbt_node` through `dbt_macros/mpo_query_tag.sql`. Snowflake allows 2000
characters; run IDs are 36 and step keys are op names, so tags stay far below that.
"""

import json
from collections.abc import Mapping

import dagster as dg

ENV_VAR = "MPO_QUERY_TAG"  # dbt profiles read it: query_tag: "{{ env_var('MPO_QUERY_TAG', '') }}"
PREFIX = '{"mpo_run":'


def query_tag(run_id: str, step_key: str) -> str:
    return json.dumps({"mpo_run": run_id, "mpo_step": step_key}, separators=(",", ":"))


def query_tag_for(context: dg.AssetExecutionContext) -> str:
    # The step key is what materialization events record as `step_key`, so cost joins on it.
    step = context.op_execution_context.get_step_execution_context().step.key
    return query_tag(context.run.run_id, step)


def parse_query_tag(tag: str | None) -> Mapping[str, str] | None:
    """The tag's fields, or None for tags this repo did not write."""
    if not tag or not tag.startswith(PREFIX):
        return None
    try:
        fields = json.loads(tag)
    except ValueError:
        return None
    if not isinstance(fields, dict) or not all(isinstance(v, str) for v in fields.values()):
        return None
    return fields
