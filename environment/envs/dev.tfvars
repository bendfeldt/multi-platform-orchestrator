# Example values: replace every ID with your own (outputs of bootstrap/ and shared/).
environment         = "dev"
tenant_id           = "00000000-0000-0000-0000-000000000000"
subscription_id     = "00000000-0000-0000-0000-000000000001"
resource_group_name = "rg-mpo-dev"
unique_suffix       = "x7k2"
waf_mode            = "Detection"

frontdoor_profile_id       = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Cdn/profiles/afd-mpo"
app_deployer_principal_id  = "00000000-0000-0000-0000-00000000000a"
contract_container_id      = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Storage/storageAccounts/stmposharedx7k2/blobServices/default/containers/contract-dev"
image_policy_definition_id = "/subscriptions/00000000-0000-0000-0000-000000000001/providers/Microsoft.Authorization/policyDefinitions/mpo-images"
admin_group_ids            = ["00000000-0000-0000-0000-00000000000b"]
viewer_group_ids           = ["00000000-0000-0000-0000-00000000000c"]

# test and prod copy images from dev's registry.
acr_pull_principal_ids = {
  test = "00000000-0000-0000-0000-00000000000d"
  prod = "00000000-0000-0000-0000-00000000000e"
}

databricks_host = "https://adb-0000000000000000.0.azuredatabricks.net"
snowflake = {
  organization = "myorg"
  account      = "sales_dev"
  infra_user   = "MPO_INFRA_DEV"
  infra_role   = "MPO_INFRA_DEV"
}
