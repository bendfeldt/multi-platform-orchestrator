"""Renders the Container Apps and Jobs of one environment from its contract.

    python deploy/render.py --contract contract.json --images images.json --out build/aca

`images.json` maps "platform" and every code location to an image pinned by digest. Writes
one `az containerapp [job] create|update --yaml` file per app or job. This file decides which
container gets which identity (docs/PLAN.md §5.1).
"""

import argparse
import json
from pathlib import Path
from typing import Any

import yaml
from orchestrator_core.contract import Contract, Identity, load_contract

REPO = Path(__file__).resolve().parent.parent
CODE_LOCATIONS = sorted(path.name for path in (REPO / "code_locations").iterdir() if path.is_dir())
WORKSPACE = "/opt/dagster/workspace.yaml"


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


def render(contract: Contract, images: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Returns `{file name: resource}` for every app and job of the environment."""
    missing = [loc for loc in CODE_LOCATIONS if loc not in contract.identities.domains]
    if missing:
        raise ValueError(f"contract has no identity for code locations {missing}")
    ids = contract.identities
    platform_image = images["platform"]
    web_ingress = {
        "external": True,
        "targetPort": 3000,
        "transport": "http",
        "allowInsecure": False,
    }
    webserver = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", WORKSPACE]

    resources = {
        "app-dagster-web.yaml": _app(
            contract,
            ids.webserver,
            platform_image,
            webserver,
            _common_env(contract, ids.webserver),
            web_ingress,
            min_replicas=0,
        ),
        "app-dagster-web-ro.yaml": _app(
            contract,
            ids.webserver,
            platform_image,
            [*webserver, "--read-only"],
            _common_env(contract, ids.webserver),
            web_ingress,
            min_replicas=0,
        ),
        "app-dagster-daemon.yaml": _app(
            contract,
            ids.platform,
            platform_image,
            ["dagster-daemon", "run", "-w", WORKSPACE],
            _common_env(contract, ids.platform),
            None,
            min_replicas=1,
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
    }
    for location in CODE_LOCATIONS:
        identity = ids.domains[location]
        # Runs of this code location get the same identity and settings as its code server.
        run_env = {
            "DAGSTER_PG_USER": identity.principal_name,
            "AZURE_CLIENT_ID": identity.client_id,
            **_platform_env(contract, location),
        }
        container_context = {
            "aca": {
                "identity_id": identity.resource_id,
                "env_vars": [f"{name}={value}" for name, value in run_env.items()],
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
            f"{location}.definitions",
            "--container-image",
            images[location],
            "--container-context",
            json.dumps(container_context),
        ]
        resources[f"app-{app_name(location)}.yaml"] = _app(
            contract,
            identity,
            images[location],
            command,
            _common_env(contract, identity) | run_env,
            # Plain HTTP/2 inside the environment only; gRPC from the webserver and daemon.
            {"external": False, "targetPort": 4000, "transport": "http2", "allowInsecure": True},
            min_replicas=1,
            cpu=0.25,
            memory="0.5Gi",
        )
    return resources


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--images", required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    resources = render(load_contract(args.contract), json.loads(Path(args.images).read_text()))
    args.out.mkdir(parents=True, exist_ok=True)
    for name, resource in resources.items():
        (args.out / name).write_text(yaml.safe_dump(resource, sort_keys=False))


if __name__ == "__main__":
    main()
