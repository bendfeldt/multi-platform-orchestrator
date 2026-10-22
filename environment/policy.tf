# Container Apps and Jobs in this resource group may only run images from this environment's
# registry (PLAN R4). The definition is created once in bootstrap/.
resource "azurerm_resource_group_policy_assignment" "images" {
  name                 = "mpo-images-${var.environment}"
  resource_group_id    = data.azurerm_resource_group.this.id
  policy_definition_id = var.image_policy_definition_id
  parameters = jsonencode({
    registry = { value = azurerm_container_registry.this.login_server }
  })
}
