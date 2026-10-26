# Runbook

What to do when an alert fires, and how to restore the metadata database. Steps that change
Azure resources run as a person with the environment's Owner role, signed in with `az login`;
nothing here uses a password or key.

## Alerts (infra repo, `alerts/<env>.yaml`)

| Alert | Meaning | First steps |
|---|---|---|
| Dagster error/warning alerts (`run_failure`, `check_failed`, `freshness`, `tick_failure`, `code_location_error`) | A run, check, schedule/sensor tick or code location failed; the alert names the code location and job or asset | Open the run or asset from the alert's search results (`alert_url`); a code location error means its code server cannot load: check the code location's last deploy |
| `daemon_unhealthy` | A required daemon (scheduler, sensors, run queue, ...) stopped heartbeating | `az containerapp logs show -n dagster-daemon -g <rg>`; restart with `az containerapp revision restart` |
| Health check missing (15 min) | The health job itself, or the whole environment, stopped | `az containerapp job execution list -n dagster-health -g <rg>`; then Postgres and the environment's status |
| Platform errors | Container Apps reported errors: failed revision, crash loop, failing probe | `ContainerAppSystemLogs_CL` for the app (docs/SLO.md); roll back with the previous image digest via `deploy.sh` |

## Restore the metadata database (point in time)

Postgres keeps 7 days of point-in-time backups (infra repo, `environment/platform.tf`). A
restore creates a **new** server; Terraform and the environment contract keep pointing at the
original, so the data is copied back into the original server instead of switching servers.
Practise this every quarter in dev; the steps below have not yet been run against Azure.

1. **Stop writers**: scale the daemon and webserver to zero and stop the schedules' runs:
   `az containerapp update -n dagster-daemon -g <rg> --min-replicas 0 --max-replicas 0`
   (same for `dagster-web`).
2. **Restore** to a new server at the chosen time (UTC):
   `az postgres flexible-server restore -g <rg> --name <server>-restore --source-server <server> --restore-time 2026-10-20T08:00:00Z`.
   If the CLI asks for networking, give the same delegated subnet and private DNS zone as the
   original (`environment/network.tf`). The restored server keeps Entra-only login and the
   admin identity `id-dagster-admin-<env>` as its Entra administrator.
3. **Copy back** from inside the VNet with a one-off Container Apps Job that runs as the admin
   identity and a PostgreSQL 16 client image imported into the environment's registry
   (`az acr import`; only that registry is allowed). The job gets a token for
   `https://ossrdbms-aad.database.windows.net` as the password and runs
   `pg_dump --format=custom --dbname=dagster` against the restored server and
   `pg_restore --clean --if-exists --no-owner --dbname=dagster` against the original.
4. **Start again**: run `deploy.sh` with the current images (it reapplies the migrate job,
   code servers, daemon and webserver), then check the UI and the next health line.
5. **Delete** the restored server: `az postgres flexible-server delete -g <rg> --name <server>-restore`.

Run history is what is lost between the restore time and now; the platforms' data is not
touched.

## Roll back a deploy

Deploys are image digests. Run the deploy pipeline again from the earlier commit on `main`,
or `deploy/scripts/deploy.sh` with that commit's `images.json`.
