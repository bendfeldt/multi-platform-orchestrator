terraform {
  required_providers {
    azurerm    = { source = "hashicorp/azurerm" }
    databricks = { source = "databricks/databricks" }
    snowflake  = { source = "snowflakedb/snowflake" }
    fabric     = { source = "microsoft/fabric" }
  }
}
