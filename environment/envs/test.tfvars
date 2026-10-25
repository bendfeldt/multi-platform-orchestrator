# Example values: replace every ID with your own (outputs of bootstrap/ and shared/).
environment         = "test"
tenant_id           = "00000000-0000-0000-0000-000000000000"
subscription_id     = "00000000-0000-0000-0000-000000000001"
resource_group_name = "rg-mpo-test"
unique_suffix       = "x7k2"
waf_mode            = "Detection"

frontdoor_profile_id       = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Cdn/profiles/afd-mpo"
app_deployer_principal_id  = "00000000-0000-0000-0000-00000000000a"
contract_container_id      = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Storage/storageAccounts/stmposharedx7k2/blobServices/default/containers/contract-test"
image_policy_definition_id = "/subscriptions/00000000-0000-0000-0000-000000000001/providers/Microsoft.Authorization/policyDefinitions/mpo-images"
admin_group_ids            = ["00000000-0000-0000-0000-00000000000b"]
viewer_group_ids           = ["00000000-0000-0000-0000-00000000000c"]

databricks_host = "https://adb-0000000000000000.0.azuredatabricks.net"
snowflake = {
  organization = "myorg"
  account      = "sales_test"
  infra_user   = "MPO_INFRA_TEST"
  infra_role   = "MPO_INFRA_TEST"
  # credit_price_usd = <your contract's USD price per credit>  # for cost in USD
}
