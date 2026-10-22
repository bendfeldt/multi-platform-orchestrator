# This environment's part of the shared Front Door Premium profile: own endpoints, origins,
# WAF policy and routes. Both origins reach the internal ACA environment over Private Link.

locals {
  webservers = {
    full     = "dagster-web"
    readonly = "dagster-web-ro"
  }
}

resource "azurerm_cdn_frontdoor_endpoint" "full" {
  name                     = "${local.name}-${var.unique_suffix}"
  cdn_frontdoor_profile_id = var.frontdoor_profile_id
}

resource "azurerm_cdn_frontdoor_endpoint" "readonly" {
  name                     = "${local.name}-ro-${var.unique_suffix}"
  cdn_frontdoor_profile_id = var.frontdoor_profile_id
}

locals {
  endpoints = {
    full     = azurerm_cdn_frontdoor_endpoint.full
    readonly = azurerm_cdn_frontdoor_endpoint.readonly
  }
}

resource "azurerm_cdn_frontdoor_origin_group" "this" {
  for_each                 = local.webservers
  name                     = "${local.name}-${each.key}"
  cdn_frontdoor_profile_id = var.frontdoor_profile_id
  # No health_probe block: probes are off. One origin per group, and probes would hit the
  # Entra sign-in redirect and keep the webservers from scaling to zero (PLAN R7).
  load_balancing {}
}

resource "azurerm_cdn_frontdoor_origin" "this" {
  for_each                       = local.webservers
  name                           = each.value
  cdn_frontdoor_origin_group_id  = azurerm_cdn_frontdoor_origin_group.this[each.key].id
  host_name                      = "${each.value}.${azurerm_container_app_environment.this.default_domain}"
  origin_host_header             = "${each.value}.${azurerm_container_app_environment.this.default_domain}"
  certificate_name_check_enabled = true

  private_link {
    location               = var.location
    private_link_target_id = azurerm_container_app_environment.this.id
    target_type            = "managedEnvironments"
    request_message        = "Front Door ${local.name}"
  }
}

resource "azurerm_cdn_frontdoor_route" "this" {
  for_each                      = local.webservers
  name                          = each.key
  cdn_frontdoor_endpoint_id     = local.endpoints[each.key].id
  cdn_frontdoor_origin_group_id = azurerm_cdn_frontdoor_origin_group.this[each.key].id
  cdn_frontdoor_origin_ids      = [azurerm_cdn_frontdoor_origin.this[each.key].id]
  patterns_to_match             = ["/*"]
  supported_protocols           = ["Http", "Https"]
  https_redirect_enabled        = true
  forwarding_protocol           = "HttpsOnly"
  link_to_default_domain        = true
}

resource "azurerm_cdn_frontdoor_firewall_policy" "this" {
  name                = replace("waf${local.name}", "-", "")
  resource_group_name = var.resource_group_name
  sku_name            = "Premium_AzureFrontDoor"
  mode                = var.waf_mode

  managed_rule {
    type    = "Microsoft_DefaultRuleSet"
    version = "2.1"
    action  = "Block"
  }

  managed_rule {
    type    = "Microsoft_BotManagerRuleSet"
    version = "1.1"
    action  = "Block"
  }

  custom_rule {
    name                           = "ratelimitperip"
    type                           = "RateLimitRule"
    action                         = "Block"
    priority                       = 100
    rate_limit_duration_in_minutes = 1
    # The Dagster UI polls often; this only stops floods.
    rate_limit_threshold = 2000
    match_condition {
      match_variable = "RemoteAddr"
      operator       = "IPMatch"
      match_values   = ["0.0.0.0/0", "::/0"]
    }
  }
}

resource "azurerm_cdn_frontdoor_security_policy" "this" {
  name                     = "waf-${local.name}"
  cdn_frontdoor_profile_id = var.frontdoor_profile_id
  security_policies {
    firewall {
      cdn_frontdoor_firewall_policy_id = azurerm_cdn_frontdoor_firewall_policy.this.id
      association {
        patterns_to_match = ["/*"]
        domain {
          cdn_frontdoor_domain_id = azurerm_cdn_frontdoor_endpoint.full.id
        }
        domain {
          cdn_frontdoor_domain_id = azurerm_cdn_frontdoor_endpoint.readonly.id
        }
      }
    }
  }
}

# Front Door's Private Link connection must be approved on the ACA environment. Terraform has
# no resource for that, so approve whatever is pending once the origins exist (Spike E).
resource "terraform_data" "approve_private_link" {
  triggers_replace = [for origin in azurerm_cdn_frontdoor_origin.this : origin.id]
  provisioner "local-exec" {
    command = <<-EOT
      az network private-endpoint-connection list --id "${azurerm_container_app_environment.this.id}" \
        --query "[?properties.privateLinkServiceConnectionState.status=='Pending'].id" -o tsv |
        xargs -r -n1 az network private-endpoint-connection approve --description "Front Door" --id
    EOT
  }
}
