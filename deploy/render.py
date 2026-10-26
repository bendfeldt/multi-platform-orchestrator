"""Renders the Container Apps and Jobs of one environment from its contract.

    python deploy/render.py --contract contract.json --images images.json --out build/aca

`images.json` maps "platform" and every code location to an image pinned by digest. Writes
one `az containerapp [job] create|update --yaml` file per app or job. This file decides which
container gets which identity (docs/PLAN.md §5.1).
"""

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from orchestrator_core.contract import Contract, Identity, load_contract

REPO = Path(__file__).resolve().parent.parent
CODE_LOCATIONS = sorted(path.name for path in (REPO / "code_locations").iterdir() if path.is_dir())
# The workspace file is a Container Apps secret mounted here, so adding a code location or a
# preview needs no image rebuild (docs/PRO_FEATURES_PLAN.md §7.2).
WORKSPACE_DIR = "/opt/dagster/workspace"
WORKSPACE = f"{WORKSPACE_DIR}/workspace.yaml"
OPS_LOCATION = "mpo_ops"
PREVIEW_TAG = "mpo-preview"


@dataclass(frozen=True)
class Preview:
    """A pull request's version of a code location, deployed to dev (orchestrator_core.preview)."""

    location: str
    pr: int

    @property
    def name(self) -> str:
        return f"{self.location}_pr{self.pr}"

    @classmethod
    def parse(cls, value: str) -> "Preview":  # "<location>:<pr>"
        location, _, pr = value.partition(":")
        return cls(location, int(pr))


def workspace_yaml(previews: tuple[Preview, ...] = ()) -> str:
    """One gRPC entry per code location and preview; container app `cl-<name>` serves it."""
    names = [*CODE_LOCATIONS, *(p.name for p in previews)]
    entries = [
        {"grpc_server": {"host": app_name(name), "port": 80, "location_name": name}}
        for name in names
    ]
    return yaml.safe_dump({"load_from": entries}, sort_keys=False)


def _probes(port: int, http_path: str | None = None) -> list[dict[str, Any]]:
    """Liveness (restart when stuck) and readiness, after a startup allowance of 5 minutes:
    loading a code location or the workspace can take a while."""
    check: dict[str, Any] = (
        {"httpGet": {"path": http_path, "port": port}}
        if http_path
        else {"tcpSocket": {"port": port}}
    )
    return [
        {
            "type": "Startup",
            "tcpSocket": {"port": port},
            "periodSeconds": 5,
            "failureThreshold": 60,
        },
        {
            "type": "Liveness",
            **check,
            "periodSeconds": 30,
            "failureThreshold": 3,
            "timeoutSeconds": 5,
        },
        {
            "type": "Readiness",
            **check,
            "periodSeconds": 10,
            "failureThreshold": 3,
            "timeoutSeconds": 5,
        },
    ]


def _with_workspace(resource: dict[str, Any], workspace: str) -> dict[str, Any]:
    properties = resource["properties"]
    properties["configuration"]["secrets"] = [{"name": "workspace", "value": workspace}]
    template = properties["template"]
    template["volumes"] = [
        {
            "name": "workspace",
            "storageType": "Secret",
            "secrets": [{"secretRef": "workspace", "path": "workspace.yaml"}],
        }
    ]
    container = template["containers"][0]
    container["volumeMounts"] = [{"volumeName": "workspace", "mountPath": WORKSPACE_DIR}]
    # A changed secret alone starts no new revision; a changed variable does, so every
    # workspace change reaches the running containers.
    digest = hashlib.sha256(workspace.encode()).hexdigest()
    container["env"].append({"name": "MPO_WORKSPACE_SHA256", "value": digest})
    return resource


def app_name(code_location: str) -> str:
    return f"cl-{code_location.replace('_', '-')}"


def _env(values: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": name, "value": value} for name, value in values.items()]


def _common_env(contract: Contract, identity: Identity) -> dict[str, str]:
    return {
        "DAGSTER_PG_HOST": contract.postgres.host,
        "DAGSTER_PG_DB": contract.postgres.database,
        "DAGSTER_PG_USER": identity.principal_name,
        "AZURE_CLIENT_ID": identity.client_id,
        "AZURE_SUBSCRIPTION_ID": contract.subscription_id,
        "AZURE_RESOURCE_GROUP": contract.resource_group,
        "AZURE_LOCATION": contract.location,
        "ACA_ENVIRONMENT_ID": contract.aca_environment_id,
        "COMPUTE_LOG_ACCOUNT": contract.compute_logs.account,
        "COMPUTE_LOG_CONTAINER": contract.compute_logs.container,
    }


def _platform_env(contract: Contract, domain: str) -> dict[str, str]:
    env = {}
    platforms = contract.platforms
    if platforms.databricks:
        env["DATABRICKS_HOST"] = platforms.databricks.host
    if platforms.snowflake and domain in platforms.snowflake.domains:
        snowflake = platforms.snowflake.domains[domain]
        env |= {
            "SNOWFLAKE_ACCOUNT": platforms.snowflake.account,
            "SNOWFLAKE_ROLE": snowflake.role,
            "SNOWFLAKE_WAREHOUSE": snowflake.warehouse,
        }
    return env


def _ops_env(contract: Contract, location: str) -> dict[str, str]:
    """mpo_ops alone reports cost and insights (orchestrator_core.cost / .insights)."""
    if location != OPS_LOCATION:
        return {}
    env = {}
    platforms = contract.platforms
    if platforms.databricks and platforms.databricks.cost_warehouse_id:
        env["DATABRICKS_COST_WAREHOUSE_ID"] = platforms.databricks.cost_warehouse_id
    if platforms.snowflake and platforms.snowflake.credit_price_usd is not None:
        env["SNOWFLAKE_CREDIT_PRICE_USD"] = str(platforms.snowflake.credit_price_usd)
    events = platforms.fabric.capacity_events if platforms.fabric else None
    if events:
        env["FABRIC_EVENTS_QUERY_URI"] = events.query_uri
        env["FABRIC_EVENTS_DATABASE"] = events.database
        if events.cu_hour_price_usd is not None:
            env["FABRIC_CU_HOUR_PRICE_USD"] = str(events.cu_hour_price_usd)
    if contract.insights:
        env["MPO_INGESTION_ENDPOINT"] = contract.insights.ingestion_endpoint
        env["MPO_INGESTION_RULE_ID"] = contract.insights.rule_id
    return env


def _resource(
    contract: Contract,
    identity: Identity,
    image: str,
    command: list[str],
    env: dict[str, str],
    cpu: float,
    memory: str,
    configuration: dict[str, Any],
    scale: dict[str, int] | None = None,
) -> dict[str, Any]:
    template: dict[str, Any] = {
        "containers": [
            {
                "name": "main",
                "image": image,
                "command": command,
                "env": _env(env),
                "resources": {"cpu": cpu, "memory": memory},
            }
        ]
    }
    if scale:
        template["scale"] = scale
    return {
        "location": contract.location,
        "identity": {
            "type": "UserAssigned",
            "userAssignedIdentities": {identity.resource_id: {}},
        },
        "properties": {
            "environmentId": contract.aca_environment_id,
            "configuration": {
                **configuration,
                "registries": [
                    {"server": contract.acr_login_server, "identity": identity.resource_id}
                ],
            },
            "template": template,
        },
    }


def _app(
    contract: Contract,
    identity: Identity,
    image: str,
    command: list[str],
    env: dict[str, str],
    ingress: dict[str, Any] | None,
    min_replicas: int,
    cpu: float = 0.5,
    memory: str = "1Gi",
) -> dict[str, Any]:
    configuration: dict[str, Any] = {"activeRevisionsMode": "Single"}
    if ingress:
        configuration["ingress"] = ingress
    scale = {"minReplicas": min_replicas, "maxReplicas": 1}
    return _resource(contract, identity, image, command, env, cpu, memory, configuration, scale)


def _job(
    contract: Contract,
    identity: Identity,
    image: str,
    command: list[str],
    env: dict[str, str],
    schedule: str | None,
) -> dict[str, Any]:
    run_once = {"parallelism": 1, "replicaCompletionCount": 1}
    configuration: dict[str, Any] = {"replicaTimeout": 1800, "replicaRetryLimit": 0}
    if schedule:
        configuration |= {
            "triggerType": "Schedule",
            "scheduleTriggerConfig": {"cronExpression": schedule, **run_once},
        }
    else:
        configuration |= {"triggerType": "Manual", "manualTriggerConfig": run_once}
    return _resource(contract, identity, image, command, env, 0.5, "1Gi", configuration)


def render(
    contract: Contract, images: dict[str, str], previews: tuple[Preview, ...] = ()
) -> dict[str, dict[str, Any]]:
    """Returns `{file name: resource}` for every app and job of the environment."""
    missing = [loc for loc in CODE_LOCATIONS if loc not in contract.identities.domains]
    if missing:
        raise ValueError(f"contract has no identity for code locations {missing}")
    for preview in previews:
        if preview.location not in CODE_LOCATIONS or preview.location == OPS_LOCATION:
            raise ValueError(f"no preview for {preview.location!r}: not a domain code location")
        if contract.environment != "dev":
            raise ValueError("previews run in dev only")
    workspace = workspace_yaml(previews)
    ids = contract.identities
    platform_image = images["platform"]
    ui_url = f"https://{contract.ingress.hostname}"  # links in alerts
    web_ingress = {
        "external": True,
        "targetPort": 3000,
        "transport": "http",
        "allowInsecure": False,
    }
    # Our webserver: Dagster's UI with Entra roles per code location and the audit log.
    webserver = ["python", "-m", "orchestrator_core.webserver", "-h", "0.0.0.0", "-p", "3000"]
    webserver += ["-w", WORKSPACE]

    def web(ingress: dict[str, Any]) -> dict[str, Any]:
        app = _app(
            contract,
            ids.webserver,
            platform_image,
            webserver,
            _common_env(contract, ids.webserver),
            ingress,
            min_replicas=0,
        )
        # /server_info answers without touching the database or code servers.
        app["properties"]["template"]["containers"][0]["probes"] = _probes(3000, "/server_info")
        return _with_workspace(app, workspace)

    resources = {
        "app-dagster-web.yaml": web(web_ingress),
        # First deploy only: reachable inside the environment until Entra sign-in is on.
        "app-dagster-web.initial.yaml": web(web_ingress | {"external": False}),
        "app-dagster-daemon.yaml": _with_workspace(
            _app(
                contract,
                ids.platform,
                platform_image,
                ["dagster-daemon", "run", "-w", WORKSPACE],
                _common_env(contract, ids.platform),
                None,
                min_replicas=1,
            ),
            workspace,
        ),
        "job-dagster-migrate.yaml": _job(
            contract,
            ids.admin,
            platform_image,
            ["python", "-m", "orchestrator_core.admin", "migrate"],
            _common_env(contract, ids.admin)
            | {
                "DAGSTER_HOME": "/opt/dagster/admin",
                "DAGSTER_PG_PRINCIPALS": ",".join(
                    identity.principal_name
                    for identity in [ids.platform, ids.webserver, *ids.domains.values()]
                ),
            },
            schedule=None,
        ),
        "job-dagster-cleanup.yaml": _job(
            contract,
            ids.admin,
            platform_image,
            ["python", "-m", "orchestrator_core.admin", "cleanup"],
            _common_env(contract, ids.admin),
            schedule="17 3 * * *",
        ),
        # Alerts on what sensors cannot see: daemon down, failed ticks, broken code locations.
        "job-dagster-health.yaml": _with_workspace(
            _job(
                contract,
                ids.admin,
                platform_image,
                ["python", "-m", "orchestrator_core.health", WORKSPACE],
                _common_env(contract, ids.admin) | {"MPO_UI_URL": ui_url},
                schedule="*/5 * * * *",
            ),
            workspace,
        ),
    }
    if contract.catalog:
        # Daily: the whole workspace's assets and lineage to Purview, as mpo_ops.
        ops = ids.domains[OPS_LOCATION]
        resources["job-dagster-catalog.yaml"] = _with_workspace(
            _job(
                contract,
                ops,
                platform_image,
                ["python", "-m", "orchestrator_core.purview", "sync", WORKSPACE],
                _common_env(contract, ops)
                | {"PURVIEW_ENDPOINT": contract.catalog.purview_endpoint},
                schedule="53 2 * * *",
            ),
            workspace,
        )
    servers = [(location, location, f"{location}.definitions", {}) for location in CODE_LOCATIONS]
    servers += [
        # A preview: the pull request's image, as the domain (same identity and settings).
        (p.name, p.location, "orchestrator_core.preview",
         {"MPO_PREVIEW_MODULE": f"{p.location}.definitions", "MPO_PREVIEW_PR": str(p.pr)})
        for p in previews
    ]  # fmt: skip
    for name, location, module, extra_env in servers:
        identity = ids.domains[location]
        # Runs of this code location get the same identity and settings as its code server.
        run_env = {
            "DAGSTER_PG_USER": identity.principal_name,
            "AZURE_CLIENT_ID": identity.client_id,
            **_platform_env(contract, location),
            **_ops_env(contract, location),
            **extra_env,
        }
        container_context = {
            "aca": {
                "identity_id": identity.resource_id,
                "env_vars": [f"{key}={value}" for key, value in run_env.items()],
            }
        }
        command = [
            "dagster",
            "code-server",
            "start",
            "-h",
            "0.0.0.0",
            "-p",
            "4000",
            "-m",
            module,
            "--container-image",
            images[name],
            "--container-context",
            json.dumps(container_context),
        ]
        app = resources[f"app-{app_name(name)}.yaml"] = _app(
            contract,
            identity,
            images[name],
            command,
            _common_env(contract, identity) | run_env | {"MPO_UI_URL": ui_url},
            # Plain HTTP/2 inside the environment only; gRPC from the webserver and daemon.
            {"external": False, "targetPort": 4000, "transport": "http2", "allowInsecure": True},
            min_replicas=1,
            cpu=0.25,
            memory="0.5Gi",
        )
        app["properties"]["template"]["containers"][0]["probes"] = _probes(4000)  # gRPC: TCP
        if extra_env:  # how deploy scripts find running previews (deploy/scripts/lib.sh)
            app["tags"] = {PREVIEW_TAG: f"{location}:{extra_env['MPO_PREVIEW_PR']}"}
    return resources


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--images", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--previews", default="", help="dev only: '<location>:<pr>,...' (deploy/scripts/preview.sh)"
    )
    args = parser.parse_args()
    previews = tuple(Preview.parse(v) for v in args.previews.split(",") if v)

    images = json.loads(Path(args.images).read_text())
    resources = render(load_contract(args.contract), images, previews)
    args.out.mkdir(parents=True, exist_ok=True)
    for name, resource in resources.items():
        (args.out / name).write_text(yaml.safe_dump(resource, sort_keys=False))


if __name__ == "__main__":
    main()
