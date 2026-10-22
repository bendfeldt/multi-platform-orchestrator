"""Admin jobs that run inside the VNet as `id-dagster-admin` (docs/PLAN.md §2.2).

python -m orchestrator_core.admin migrate   # before every deploy
python -m orchestrator_core.admin cleanup   # daily
"""

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import psycopg2
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from psycopg2 import sql

from orchestrator_core.auth import get_token

POSTGRES_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"
# Tag the ACA run launcher puts on every Job it creates; nothing else carries it.
RUN_TAG = "dagster-run-id"


def migrate() -> None:
    """Creates a database login for every Dagster identity, then migrates Dagster's schema.

    Logins get `dagster_rw` (read and write rows, no DDL); this admin identity owns the tables.
    """
    principals = [name for name in os.environ["DAGSTER_PG_PRINCIPALS"].split(",") if name]
    connection = psycopg2.connect(
        host=os.environ["DAGSTER_PG_HOST"],
        dbname=os.environ["DAGSTER_PG_DB"],
        user=os.environ["DAGSTER_PG_USER"],
        password=get_token(POSTGRES_SCOPE),
        sslmode="require",
    )
    connection.autocommit = True
    grants = [
        "GRANT USAGE ON SCHEMA public TO dagster_rw",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO dagster_rw",
        "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO dagster_rw",
    ]
    try:
        cursor = connection.cursor()
        cursor.execute(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dagster_rw') "
            "THEN CREATE ROLE dagster_rw NOLOGIN; END IF; END $$"
        )
        for name in principals:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,))
            if cursor.fetchone() is None:
                # Azure's function for an Entra principal; the name is the identity's name.
                cursor.execute(
                    "SELECT * FROM pgaadauth_create_principal(%s, false, false)", (name,)
                )
            cursor.execute(sql.SQL("GRANT dagster_rw TO {}").format(sql.Identifier(name)))
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dagster_rw"
        )
        cursor.execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO dagster_rw"
        )
        subprocess.run(["dagster", "instance", "migrate"], check=True)
        # Tables created before the default privileges existed need the grants too.
        for grant in grants:
            cursor.execute(grant)
    finally:
        connection.close()


def cleanup(keep_days: int = 7) -> list[str]:
    """Deletes run Jobs whose last execution ended more than `keep_days` ago.

    The ACA run launcher leaves one Job per finished run in place. Run logs stay in
    Postgres and Blob storage. Returns the deleted Job names.
    """
    resource_group = os.environ["AZURE_RESOURCE_GROUP"]
    client = ContainerAppsAPIClient(DefaultAzureCredential(), os.environ["AZURE_SUBSCRIPTION_ID"])
    cutoff = datetime.now(UTC) - timedelta(days=keep_days)
    deleted = []
    for job in client.jobs.list_by_resource_group(resource_group):
        if not job.name or RUN_TAG not in (job.tags or {}):
            continue
        executions = list(client.jobs_executions.list(resource_group, job.name))
        # A Job still running, or never started, has no execution with an end time yet.
        end_times = [execution.end_time for execution in executions]
        if not end_times or any(end is None for end in end_times):
            continue
        if max(end for end in end_times if end is not None) > cutoff:
            continue
        client.jobs.begin_delete(resource_group, job.name)
        deleted.append(job.name)
    return deleted


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "migrate":
        migrate()
    elif command == "cleanup":
        print(f"deleted {len(cleanup())} finished run Jobs")
    else:
        sys.exit("usage: python -m orchestrator_core.admin migrate|cleanup")
