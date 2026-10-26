"""Dagster's catalog and lineage in Microsoft Purview (docs/PRO_FEATURES_PLAN.md §6.2, phase H).

Uses the tenant's existing Purview account (an input, nothing is created by Terraform) through
the Data Map REST API (Atlas v2, api-version 2023-09-01) with an Entra token, no key:

    python -m orchestrator_core.purview typedefs          once, by a Purview admin (az login)
    python -m orchestrator_core.purview sync <workspace>  the daily dagster-catalog job (mpo_ops)

Every asset of every code location becomes a `dagster_asset` (qualified name
`dagster://<asset key>`) with its code location, kinds, owners, tags and columns; every asset
with upstreams gets a `dagster_process` from its upstreams to it, with the column lineage as
Purview's `columnMapping`. Entities are upserted; assets removed from Dagster stay in Purview
until deleted there.
"""

import json
import os
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
import requests

API_VERSION = "2023-09-01"
SCOPE = "https://purview.azure.net/.default"
ENDPOINT_ENV = "PURVIEW_ENDPOINT"  # e.g. https://<account>.purview.azure.com
ASSET_TYPE = "dagster_asset"
PROCESS_TYPE = "dagster_process"
_KIND = "dagster/kind/"
_CHUNK = 100


def _attribute(name: str) -> dict[str, Any]:
    return {"name": name, "typeName": "string", "isOptional": True, "cardinality": "SINGLE",
            "isUnique": False, "isIndexable": False}  # fmt: skip


TYPEDEFS = {
    "entityDefs": [
        {
            "name": ASSET_TYPE,
            "superTypes": ["DataSet"],
            "description": "An asset defined in Dagster",
            "attributeDefs": [
                _attribute(a) for a in ("codeLocation", "kinds", "owners", "tags", "columnSchema")
            ],
        },  # fmt: skip
        {
            "name": PROCESS_TYPE,
            "superTypes": ["Process"],
            "description": "How Dagster produces an asset from its upstream assets",
            "attributeDefs": [_attribute("columnMapping")],
        },
    ]
}


@dataclass(frozen=True)
class AssetInfo:
    key: dg.AssetKey
    parents: tuple[dg.AssetKey, ...] = ()
    location: str | None = None
    kinds: tuple[str, ...] = ()
    owners: tuple[str, ...] = ()
    tags: Mapping[str, str] = field(default_factory=dict)
    description: str | None = None
    columns: tuple[tuple[str, str], ...] = ()  # (name, type)
    # column -> the upstream (asset, column) pairs it is derived from
    column_lineage: Mapping[str, tuple[tuple[dg.AssetKey, str], ...]] = field(default_factory=dict)


def qualified_name(key: dg.AssetKey) -> str:
    return f"dagster://{key.to_user_string()}"


def _ref(key: dg.AssetKey) -> dict[str, Any]:
    return {"typeName": ASSET_TYPE, "uniqueAttributes": {"qualifiedName": qualified_name(key)}}


def from_asset_graph(graph: Any) -> list[AssetInfo]:
    """The workspace's asset graph (every code location) as AssetInfos."""
    infos = []
    for key in sorted(graph.get_all_asset_keys(), key=lambda k: k.to_user_string()):
        node = graph.get(key)
        try:
            location = node.resolve_to_singular_repo_scoped_node().repository_handle.location_name
        except Exception:  # an upstream no code location defines
            location = None
        schema = node.metadata.get("dagster/column_schema")
        lineage = node.metadata.get("dagster/column_lineage")
        columns = getattr(getattr(schema, "value", None), "columns", None) or []
        deps = getattr(getattr(lineage, "value", None), "deps_by_column", None) or {}
        infos.append(
            AssetInfo(
                key=key,
                parents=tuple(sorted(node.parent_keys, key=lambda k: k.to_user_string())),
                location=location,
                kinds=tuple(sorted(t[len(_KIND) :] for t in node.tags if t.startswith(_KIND))),
                owners=tuple(node.owners),
                tags={k: v for k, v in sorted(node.tags.items()) if not k.startswith(_KIND)},
                description=node.description,
                columns=tuple((c.name, c.type) for c in columns),
                column_lineage={
                    column: tuple((d.asset_key, d.column_name) for d in sources)
                    for column, sources in deps.items()
                },
            )
        )
    return infos


def entities(infos: Iterable[AssetInfo]) -> tuple[list[dict], list[dict]]:
    """(datasets, processes) as Atlas entities. Processes refer to datasets by qualified name,
    so they are sent after the datasets exist."""
    datasets, processes = [], []
    for info in infos:
        datasets.append(
            {
                "typeName": ASSET_TYPE,
                "attributes": {
                    "qualifiedName": qualified_name(info.key),
                    "name": info.key.path[-1],
                    "description": info.description,
                    "codeLocation": info.location,
                    "kinds": ",".join(info.kinds),
                    "owners": ",".join(info.owners),
                    "tags": json.dumps(dict(info.tags)),
                    "columnSchema": json.dumps([{"name": n, "type": t} for n, t in info.columns]),
                },
            }
        )
        if not info.parents:
            continue
        by_parent: dict[dg.AssetKey, list[dict[str, str]]] = {}
        for column, sources in info.column_lineage.items():
            for parent, parent_column in sources:
                by_parent.setdefault(parent, []).append({"Source": parent_column, "Sink": column})
        attributes: dict[str, Any] = {
            "qualifiedName": f"{qualified_name(info.key)}#produce",
            "name": f"produce {info.key.to_user_string()}",
            "inputs": [_ref(parent) for parent in info.parents],
            "outputs": [_ref(info.key)],
        }
        if by_parent:
            attributes["columnMapping"] = json.dumps(
                [
                    {
                        "DatasetMapping": {
                            "Source": qualified_name(p),
                            "Sink": qualified_name(info.key),
                        },
                        "ColumnMapping": mapping,
                    }
                    for p, mapping in sorted(by_parent.items(), key=lambda i: i[0].to_user_string())
                ]
            )
        processes.append({"typeName": PROCESS_TYPE, "attributes": attributes})
    return datasets, processes


@dataclass
class Purview:
    endpoint: str
    token: Callable[[], str]
    session: Any = requests

    def _call(self, method: str, path: str, body: Any = None) -> requests.Response:
        response = self.session.request(
            method,
            f"{self.endpoint.rstrip('/')}/datamap/api{path}",
            params={"api-version": API_VERSION},
            headers={"Authorization": f"Bearer {self.token()}"},
            json=body,
            timeout=60,
        )
        return response

    def missing_typedefs(self) -> list[str]:
        missing = []
        for name in (ASSET_TYPE, PROCESS_TYPE):
            response = self._call("GET", f"/atlas/v2/types/entitydef/name/{name}")
            if response.status_code == 404:
                missing.append(name)
            else:
                response.raise_for_status()
        return missing

    def create_typedefs(self) -> None:
        missing = set(self.missing_typedefs())
        if missing:
            body = {"entityDefs": [d for d in TYPEDEFS["entityDefs"] if d["name"] in missing]}
            self._call("POST", "/atlas/v2/types/typedefs", body).raise_for_status()

    def upsert(self, items: Sequence[dict]) -> None:
        for start in range(0, len(items), _CHUNK):
            body = {"entities": list(items[start : start + _CHUNK])}
            self._call("POST", "/atlas/v2/entity/bulk", body).raise_for_status()


def sync(purview: Purview, infos: Sequence[AssetInfo]) -> tuple[int, int]:
    """Upserts every asset, then every process; returns their counts."""
    missing = purview.missing_typedefs()
    if missing:
        raise RuntimeError(
            f"Purview has no type {missing}: a Purview admin runs "
            "`python -m orchestrator_core.purview typedefs` once"
        )
    datasets, processes = entities(infos)
    purview.upsert(datasets)
    purview.upsert(processes)
    return len(datasets), len(processes)


def main(argv: Sequence[str]) -> None:
    from orchestrator_core.auth import get_token

    purview = Purview(os.environ[ENDPOINT_ENV], lambda: get_token(SCOPE))
    if argv[:1] == ["typedefs"]:
        purview.create_typedefs()
        print("Purview types ready:", ASSET_TYPE, PROCESS_TYPE)
        return
    if argv[:1] != ["sync"] or len(argv) != 2:
        sys.exit("usage: python -m orchestrator_core.purview typedefs | sync <workspace.yaml>")
    from dagster._core.workspace.context import WorkspaceProcessContext
    from dagster._core.workspace.load_target import WorkspaceFileTarget

    with (
        dg.DagsterInstance.get() as instance,
        WorkspaceProcessContext(
            instance, workspace_load_target=WorkspaceFileTarget(paths=[argv[1]])
        ) as context,
    ):
        infos = from_asset_graph(context.create_request_context().asset_graph)
    assets, processes = sync(purview, infos)
    print(json.dumps({"purview_assets": assets, "purview_processes": processes}))


if __name__ == "__main__":
    main(sys.argv[1:])
