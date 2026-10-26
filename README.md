# multi-platform-orchestrator

Dagster orchestration of data assets across Databricks, Microsoft Fabric and Snowflake, deployed
to Azure Container Apps with Entra ID everywhere and least-privilege identities.

- Plan: [docs/PLAN.md](docs/PLAN.md) · Costs: [docs/COSTS.md](docs/COSTS.md) · Review: [docs/PLAN_REVIEW.md](docs/PLAN_REVIEW.md)
- Operations: [docs/RUNBOOK.md](docs/RUNBOOK.md) · [docs/SLO.md](docs/SLO.md) · [docs/SECURITY.md](docs/SECURITY.md) · Pro features: [docs/PRO_FEATURES_PLAN.md](docs/PRO_FEATURES_PLAN.md)
- Infrastructure (Terraform) lives in a separate repo, `multi-platform-orchestrator-iac`.

## Layout

| Path | What |
|---|---|
| `libs/orchestrator_core` | Shared library: Entra credentials, canonical asset keys, environment contract |
| `code_locations/<domain>` | One Dagster code location per domain (own image and identity in Azure); `mpo_ops` holds the operations sensors |
| `containers/Containerfile` | One Containerfile for all images; `SYNC_ARGS` picks the package |
| `deploy/` | Dagster instance/workspace config and the scripts CI calls |
| `local/compose.yaml` | Local stack: Postgres, webserver, daemon, code locations |
| `.github/workflows/`, `.azuredevops/pipelines/` | Thin CI wrappers around `deploy/scripts` |

## Develop

Needs [uv](https://docs.astral.sh/uv/) and [Podman](https://podman.io/) with a compose provider
(`podman-compose` or `docker-compose`).

```bash
uv sync --all-packages                            # one venv with every package
deploy/scripts/ci.sh                              # lint, types, tests, definitions check
podman compose -f local/compose.yaml up --build   # UI at http://127.0.0.1:3000
```

For work that calls real dev platforms, run Dagster on your machine so it can use your
`az login`: `uv run dagster dev -m example_sales.definitions`.

## Deploy

Pushing to `main` runs CI, builds the images once, deploys to dev, then promotes the same image
digests to test and prod after approval. Either CI system works:

- **GitHub Actions** (`.github/workflows/deploy.yml`): create environments `dev`, `test`, `prod`
  with the variables `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`,
  `ACR_LOGIN_SERVER`, `CONTRACT_URL`, and required reviewers on test and prod.
- **Azure DevOps** (`.azuredevops/pipelines/deploy.yml`): service connections `mpo-app-dev`,
  `mpo-app-test`, `mpo-app-prod` (workload identity federation; the names the app deployers'
  federated credentials trust), environments `mpo-<env>` with approvals, and variable groups
  `mpo-<env>` with `ACR_LOGIN_SERVER`, `CONTRACT_URL`.

No secrets in either: Azure login is federated, and the variables are IDs and URLs. The infra
repo creates the identities and publishes each environment's contract at `CONTRACT_URL`.

Pull requests get a change report (what changes in assets, checks, schedules, sensors). Label
one `preview` and approve the `preview` environment to run its changed code locations in dev as
`<domain>_pr<N>` (own asset keys, no automation); closed pull requests' previews are removed
hourly.

`deploy/scripts/deploy.sh` renders the apps from the contract (`deploy/render.py`), runs the
in-VNet migrate job, then updates code servers, daemon, cleanup job and the webserver.

In Azure the UI runs `orchestrator_core.webserver`: Dagster's webserver with Entra app roles
per code location (`Dagster.Viewer`, `Dagster.Launcher.<domain>`, `Dagster.Editor.<domain>`,
`Dagster.Admin`) and an audit log of every UI action. Runs launched from the UI carry the
user in the `mpo/launched_by` tag. Each table has exactly one writing domain.

Alerts: the `mpo_ops` code location's sensors (failed runs, failed checks, freshness) and the
5-minute `dagster-health` job (daemons, failed ticks, broken code locations) print one JSON
line per alert. Azure Monitor rules from the infra repo's `alerts/<env>.yaml` deliver them;
see [docs/PRO_FEATURES_PLAN.md](docs/PRO_FEATURES_PLAN.md) §4.

Cost and insights: `mpo_ops` also runs `mpo_cost` (platform cost per asset, charted on each
asset page under Plots) and `mpo_insights` (runs, failures, durations, checks) daily, and sends
both to Log Analytics for the workbook. dbt projects on Snowflake set
`query_tag: "{{ env_var('MPO_QUERY_TAG', '') }}"` in their profile and copy
`libs/orchestrator_core/dbt_macros/mpo_query_tag.sql` into `macros/` for cost per model.

Purview (optional): with the contract's `catalog.purview_endpoint`, the daily `dagster-catalog`
job puts every asset, its columns and its lineage into the tenant's Purview account. Once, a
Purview admin runs `PURVIEW_ENDPOINT=... uv run python -m orchestrator_core.purview typedefs`
and gives mpo_ops's identity Data Curator on a collection.

## Add a domain

1. Copy `code_locations/example_sales` to `code_locations/<domain>` and rename the package.
2. Describe its jobs as YAML in `src/<domain>/defs/<job>/defs.yaml`. Components:

   | `type: orchestrator_core.components.…` | Runs |
   |---|---|
   | `FabricItemJob` | any Fabric item: `job_type: Pipeline`, `RunNotebook`, `sparkjob`, … |
   | `FabricSemanticModelRefresh` | a semantic model refresh (optionally only some tables) |
   | `DatabricksJob` | an existing Databricks job |
   | `SnowflakeSql` | SQL statements in Snowflake |
   | `orchestrator_core.powerbi.PowerBIReports` | nothing: puts a Power BI workspace's reports and dashboards in the catalog (extra `powerbi`) |

   Each lists the assets it `writes` and the upstream assets it `reads` as
   `platform/...` keys, e.g. `snowflake/sales/mart/fct_orders`. That is the lineage.
   Optional `owners`, `tags` and `columns` (with `derived_from` for column lineage) fill the
   catalog; see `defs/mart_orders/defs.yaml`.
3. dbt: depend on `orchestrator-core[dbt]` plus the adapter, and use
   `type: orchestrator_core.dbt.CanonicalDbtProject` with `platform:` (and `fabric_workspace:`
   for Fabric). Databricks and Fabric profiles read a token from `DATABRICKS_TOKEN` or
   `FABRIC_TOKEN`, always with a default: `{{ env_var('DATABRICKS_TOKEN', 'unset') }}`. Snowflake
   profiles use `authenticator: workload_identity`. Sources on another platform set
   `meta: {dagster: {asset_key: [...]}}`.
4. Add it to `deploy/workspace.local.yaml` and `local/compose.yaml`.
5. In the infra repo, add `domains/<domain>.yaml` for its identity and grants.

Platform settings come from environment variables the deployment sets: `DATABRICKS_HOST`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`. Locally, also set
`SNOWFLAKE_USER` to sign in to Snowflake through your browser.
