"""Entra ID tokens for platform resources: a managed identity in Azure, `az login` locally."""

import os
from functools import cache

import dagster as dg
from azure.core.credentials import TokenCredential
from azure.identity import AzureCliCredential, ManagedIdentityCredential


@cache
def _credential() -> TokenCredential:
    # Container Apps sets IDENTITY_ENDPOINT; without it we are on a developer machine.
    if "IDENTITY_ENDPOINT" in os.environ:
        # AZURE_CLIENT_ID picks the container's user-assigned identity (the domain identity).
        return ManagedIdentityCredential(client_id=os.environ.get("AZURE_CLIENT_ID"))
    return AzureCliCredential()


class AzureCredentialResource(dg.ConfigurableResource):
    """Hands out short-lived Entra tokens; azure-identity caches and refreshes them."""

    def get_token(self, scope: str) -> str:
        return _credential().get_token(scope).token
