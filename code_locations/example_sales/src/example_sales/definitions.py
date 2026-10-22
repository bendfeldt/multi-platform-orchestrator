"""Example domain: the end-to-end lineage target from docs/PLAN.md §4.3.

Asset specs only, so the cross-platform graph shows in the UI. The platform components
that materialize them arrive in Phase 2.
"""

import dagster as dg
from orchestrator_core.asset_keys import (
    databricks_key,
    fabric_semantic_model_key,
    fabric_table_key,
    snowflake_key,
)

raw_orders = fabric_table_key("sales", "lh_raw", "dbo", "orders")
silver_orders = databricks_key("sales", "silver", "orders")
fct_orders = snowflake_key("SALES", "MART", "fct_orders")
sales_model = fabric_semantic_model_key("sales", "Sales")

defs = dg.Definitions(
    assets=[
        dg.AssetSpec(raw_orders, kinds={"fabric"}, group_name="sales"),
        dg.AssetSpec(silver_orders, deps=[raw_orders], kinds={"databricks"}, group_name="sales"),
        dg.AssetSpec(fct_orders, deps=[silver_orders], kinds={"snowflake"}, group_name="sales"),
        dg.AssetSpec(sales_model, deps=[fct_orders], kinds={"fabric"}, group_name="sales"),
    ]
)
