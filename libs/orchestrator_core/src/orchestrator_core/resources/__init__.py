"""Platform resources. Settings come from environment variables the deployment sets from the
environment contract, so code locations never hard-code hosts or accounts."""

import dagster as dg

from orchestrator_core.resources.databricks import DatabricksResource
from orchestrator_core.resources.fabric import FabricResource
from orchestrator_core.resources.snowflake import SnowflakeResource


def platform_resources() -> dict[str, dg.ConfigurableResource]:
    """Every platform resource; an unset variable only fails a run that uses that platform."""
    return {
        "fabric": FabricResource(),
        "databricks": DatabricksResource(host=dg.EnvVar("DATABRICKS_HOST")),
        "snowflake": SnowflakeResource(
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            oauth_scope=dg.EnvVar("SNOWFLAKE_OAUTH_SCOPE"),
            warehouse=dg.EnvVar("SNOWFLAKE_WAREHOUSE"),
            role=dg.EnvVar("SNOWFLAKE_ROLE"),
        ),
    }


__all__ = ["DatabricksResource", "FabricResource", "SnowflakeResource", "platform_resources"]
