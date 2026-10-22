"""Canonical asset keys: one key per physical data object, whichever component describes it.

Two components that name the same table must build the same key, or the cross-platform
DAG falls apart (docs/PLAN.md §4.1).
"""

import dagster as dg


def _key(platform: str, *parts: str) -> dg.AssetKey:
    # shortcut: lower-casing merges quoted identifiers that differ only by case; add a
    # case-preserving variant if a platform ever needs one.
    cleaned = [part.strip().lower() for part in parts]
    if not all(cleaned):
        raise ValueError(f"empty part in {platform} asset key: {parts!r}")
    return dg.AssetKey([platform, *cleaned])


def databricks_key(catalog: str, schema: str, table: str) -> dg.AssetKey:
    return _key("databricks", catalog, schema, table)


def snowflake_key(database: str, schema: str, table: str) -> dg.AssetKey:
    return _key("snowflake", database, schema, table)


def fabric_table_key(workspace: str, item: str, schema: str, table: str) -> dg.AssetKey:
    return _key("fabric", workspace, item, schema, table)


def fabric_semantic_model_key(workspace: str, name: str) -> dg.AssetKey:
    return _key("fabric", workspace, "semantic_model", name)
