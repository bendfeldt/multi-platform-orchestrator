"""Power BI reports and dashboards in the asset catalog (docs/PRO_FEATURES_PLAN.md §6.2), through
open-source dagster-powerbi with the code location's own Entra identity (no client secret).

Needs the `powerbi` extra. Keys join the rest of the graph:

    fabric/<workspace>/semantic_model/<name>   what FabricSemanticModelRefresh writes
    fabric/<workspace>/report/<name>           depends on its semantic model
    fabric/<workspace>/dashboard/<name>        depends on the reports of its tiles

Semantic models are left out by default, because the refresh component already defines them
(one key, one definition); `include_semantic_models: true` adds the others as plain specs.
Uses the workspace APIs, not the admin scan, so the identity only needs to be a member
(Viewer) of the workspace and allowed to use Power BI APIs.
"""

import re

import dagster as dg
from dagster_powerbi import DagsterPowerBITranslator, PowerBIWorkspace, load_powerbi_asset_specs
from dagster_powerbi.resource import PowerBICredentials
from dagster_powerbi.translator import PowerBITranslatorData

from orchestrator_core.asset_keys import fabric_semantic_model_key
from orchestrator_core.auth import get_token
from orchestrator_core.resources.fabric import POWER_BI_SCOPE


class EntraPowerBICredentials(PowerBICredentials):
    """dagster-powerbi's credentials, from the managed identity (or `az login` locally)."""

    @property
    def api_token(self) -> str:
        return get_token(POWER_BI_SCOPE)


def _name(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", value.strip().lower()).strip("_")
    if not cleaned:
        raise ValueError(f"Power BI name without letters or digits: {value!r}")
    return cleaned


class CanonicalPowerBITranslator(DagsterPowerBITranslator):
    def __init__(self, workspace: str, owners: list[str], tags: dict[str, str]):
        self.workspace, self.owners, self.tags = _name(workspace), owners, tags

    def _key(self, kind: str, name: str) -> dg.AssetKey:
        return dg.AssetKey(["fabric", self.workspace, kind, _name(name)])

    def get_semantic_model_spec(self, data: PowerBITranslatorData) -> dg.AssetSpec:
        spec = super().get_semantic_model_spec(data)
        # Upstream tables are declared on the refresh component (`reads`), in canonical keys.
        return spec.replace_attributes(
            key=fabric_semantic_model_key(self.workspace, _name(data.properties["name"])),
            deps=[],
        )

    def get_report_spec(self, data: PowerBITranslatorData) -> dg.AssetSpec:
        spec = super().get_report_spec(data)  # deps: its semantic model, via the method above
        return self._ours(spec.replace_attributes(key=self._key("report", data.properties["name"])))

    def get_dashboard_spec(self, data: PowerBITranslatorData) -> dg.AssetSpec:
        spec = super().get_dashboard_spec(data)  # deps: its tiles' reports, via the method above
        name = re.sub(r"\.[^.]+$", "", data.properties["displayName"])
        return self._ours(spec.replace_attributes(key=self._key("dashboard", name)))

    def _ours(self, spec: dg.AssetSpec) -> dg.AssetSpec:
        return spec.merge_attributes(owners=self.owners, tags=self.tags)


class PowerBIReports(dg.Component, dg.Model, dg.Resolvable):
    """The reports and dashboards of one Power BI workspace, as external assets."""

    workspace_id: str
    workspace_name: str  # the key part, as in fabric/<workspace>/semantic_model/<name>
    group_name: str | None = None
    owners: list[str] = []
    tags: dict[str, str] = {}
    include_semantic_models: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        workspace = PowerBIWorkspace(
            credentials=EntraPowerBICredentials(), workspace_id=self.workspace_id
        )
        translator = CanonicalPowerBITranslator(self.workspace_name, self.owners, self.tags)
        kinds = {"report", "dashboard"} | (
            {"semantic_model"} if self.include_semantic_models else set()
        )
        specs = [
            spec.replace_attributes(group_name=self.group_name) if self.group_name else spec
            for spec in load_powerbi_asset_specs(workspace, translator, use_workspace_scan=False)
            if spec.key.path[2] in kinds
        ]
        return dg.Definitions(assets=specs)


__all__ = ["EntraPowerBICredentials", "PowerBIReports"]
