# Multi-Platform Orchestrator — Implementation Plan

Status: **v8**. Implemented: Phases 1, 2 and 4 in this repo, and Phase 3 (Terraform) on the
`infra` branch, which becomes the infra repo once it exists. Everything is tested against
mocked platform APIs and providers and a local Postgres; nothing has run against Azure yet
(Phase 0 spikes, Phases 5–6). All findings of
[PLAN_REVIEW.md](PLAN_REVIEW.md) are applied (marked *R1*–*R16* below).

## 1. Goal

One Dagster deployment in Azure that orchestrates data assets on Databricks, Microsoft Fabric,
and Snowflake (later Azure SQL, ADLS, ADF and others), with dbt for transformations, and with
**lineage across all of them** forming a single asset graph (DAG).

Hard requirements:

- **Entra ID everywhere**: humans sign in with Entra (OAuth/OIDC); workloads use managed
  identities / workload identity federation. **No stored secrets.**
- **Least privilege**: each identity can reach only what it needs. Data access is held only by
  domain identities. Known limit (*R4*): whoever can deploy or control the Dagster control plane
  in an environment can start a run with any domain identity of that environment (§5.2).
- **Separation of concerns**: infrastructure and solution are separate, independently versioned
  and deployed (§3).
- **Code on GitHub; deployable from GitHub Actions or Azure DevOps Pipelines**, both secretless (§9).
- **Cost-conscious**: ≈ $660–680/month for dev + test + prod incl. Front Door Premium (§6.3, [COSTS.md](COSTS.md)).
- **Runs locally (Podman) and in Azure** from the same container images.

Design principle: **Dagster orchestrates, the platforms compute.**

### 1.1 Decisions

| # | Decision |
|---|---|
| 1 | Podman for building and running containers (OCI images, `Containerfile`). |
| 2 | Terraform for infrastructure. |
| 3 | On-prem out of scope for now. |
| 3a | **Azure SQL out of scope for now**; can be added later as its own resource, component and access module. |
| 4 | Fabric: any item with a job type; first-class pipelines, notebooks, semantic models. |
| 5 | dbt in scope. |
| 6 | Environments dev, test, prod. |
| 7 | UI reachable from the internet behind Entra ID OAuth. |
| 8 | Hosting: **Azure Container Apps** (confirmed). |
| 9 | Subscription layout: **both** supported — one subscription per environment, or one shared subscription with a resource group per environment. |
| 10 | Azure region: **setup input**, optional, default `westeurope`. |
| 11 | Snowflake: Business Critical (PrivateLink available if ever required); one account per environment assumed. |
| 12 | Databricks and Fabric workspaces **already exist** — Terraform only references them and manages identities/grants. |
| 13 | Ingress: **one Front Door Premium profile** shared by dev/test/prod, Private Link to each internal ACA environment, plus Container Apps built-in Entra auth (§6.2). |
| 14 | Both repositories live on **GitHub**. Deployment pipelines for **GitHub Actions and Azure DevOps**, same scripts underneath. |

## 2. Architecture

```
  Users (internet) ──► Front Door Premium (WAF managed rules + bot protection)
                                │ Private Link (no public endpoint on the environment)
                 ┌──────────────▼────────────────────────────────────────────────┐
  Azure VNet     │  Azure Container Apps environment (internal, VNet-integrated)  │
  (per env)      │  dagster-webserver (full)       ← Entra OAuth, Operators        │
                 │  dagster-webserver --read-only  ← Entra OAuth, Viewers          │
                 │  dagster-daemon (schedules, sensors, run queue)                │
                 │  code-location servers: one per domain, domain identity        │
                 │  run workers: ACA Jobs, one per run, domain identity           │
                 │  admin jobs: migrate (on deploy), cleanup (daily)              │
                 └───┬──────────────┬──────────────┬─────────────────────────────┘
                     │ Entra token  │ service endpoints   │ public APIs, Entra auth
          PostgreSQL Flexible     Storage (compute logs)     Databricks · Fabric ·
          (VNet-integrated,                                  Snowflake
           Entra-only auth)
```

Verified in the current Dagster release (`dagster-azure` / `dagster-postgres` 0.29.26):
`AcaRunLauncher` (marked beta) runs each run as its own ACA Job with a **per-code-location
managed identity**; `dagster-postgres` supports `auth_provider: azure_wif` (Entra token per
connection, password auth disabled); blob compute logs support `DefaultAzureCredential`.
Phase 0 spike, AKS fallback.

### 2.1 Run workers, sensors and the metadata database (*R1*, *R2*)

- **Every process that loads user code or executes runs uses the domain identity**: the domain's
  code-location server (it evaluates schedules and sensors, and calls platforms from sensors)
  and every run Job of that domain.
- All Dagster processes (daemon, webservers, code-location servers, runs) read and write
  Dagster's metadata database. They log in with their own identity and get the Postgres role
  **`dagster_rw`** (SELECT/INSERT/UPDATE/DELETE on the Dagster schema, no DDL). Only the admin
  identity (§2.2) owns the schema.
- Consequence, stated plainly: any domain identity can read and change Dagster run records of
  other domains. The metadata database holds run status, logs and config — never business data,
  and no secrets.

### 2.2 Admin jobs (*R3*, *R6*)

Two ACA Jobs, same image as the daemon, identity `id-dagster-admin-<env>`, running **inside the
VNet** so CI runners never need network access to private resources:

| Job | Trigger | Does |
|---|---|---|
| `job-dagster-migrate` | Manual: started by the deploy pipeline (`az containerapp job start`, wait for result) **before** apps are updated | Creates Entra database principals and grants `dagster_rw` (`pgaadauth_create_principal`), then `dagster instance migrate` |
| `job-dagster-cleanup` | Schedule (daily) | Deletes run Jobs whose last execution finished more than 7 days ago. The launcher leaves one Job per finished run in place; at 200 runs/day that would be ~6,000 resources a month. Logs remain in Postgres and Blob storage |

A failed migration stops the deploy before any app changes.

### 2.3 Run concurrency (*R8*)

Limits in `dagster.yaml`:

- `concurrency.runs.max_concurrent_runs: 10` in every environment. Dagster reads it as a fixed
  number, and the same image runs everywhere; 10 stays well inside Postgres B1ms's ~50
  connections.
- One concurrency **pool per platform** (`pool=<platform>` on every component, default limit 10),
  so a backfill on one platform cannot take every slot.
- Known limit: the ACA launcher blocks the daemon for 30–50 s per launch while Azure creates
  the Job, so launches are serial — a 100-partition backfill needs ~1 h just to start. Spike A
  measures it; AKS is the fallback if that is too slow.

### 2.4 Local runtime (*R11*)

- `podman compose` stack: Postgres, Azurite, webserver, daemon, code-location containers — same
  images as Azure.
- **`DefaultRunLauncher`** locally: each run is a process inside the code-location container.
  No container-runtime socket is mounted into any container, and `dagster-docker` is not used.
- Local auth: `AzureCliCredential` (developer's own `az login`) against **dev** platforms only.
  The containers have no Azure CLI, so platform work runs Dagster on the host
  (`uv run dagster dev`); the Podman stack checks the full deployment shape.

## 3. Separation of concerns: two repositories

Infrastructure and solution change at different speeds, are owned by different roles and need
different permissions. They are split into **two repositories with a versioned contract** between
them. Neither imports code from the other.

| | `multi-platform-orchestrator-infra` (new repo) | `multi-platform-orchestrator` (this repo) |
|---|---|---|
| Contains | Terraform only | Python, Dagster definitions, Containerfiles, app deployment, local Podman stack |
| Owns | Network, ACA environment, Postgres, ACR, storage, Front Door, monitoring, Entra groups & app registrations, managed identities, federated credentials, Azure Policy, **platform grants** (Unity Catalog, Snowflake roles, Fabric workspace roles) | Core library, components, code locations, dbt projects, images, container app + job definitions (incl. admin jobs), Dagster instance config |
| Deploy identity | `infra-deployer-<env>`: rights to create resources and role assignments (powerful, rarely used, approval-gated) | `app-deployer-<env>`: push to ACR, update container apps/jobs in one RG, start the migrate job. Cannot change networking, identities or grants |
| Changes when | New environment, new domain (identity + grants), platform change | Code, assets, schedules, dbt models |
| Release cadence | Slow, reviewed by platform/security | Fast, reviewed by data engineers |

### 3.1 The contract (*R15*)

The infra pipeline publishes its **non-secret Terraform outputs** per environment as
`contract/<env>.json` to the bootstrap storage account (Entra-only, shared keys disabled; read
via RBAC by the app deployer):

```json
{
  "schema_version": 1,
  "environment": "dev",
  "location": "westeurope",
  "subscription_id": "…",
  "resource_group": "rg-mpo-dev",
  "aca_environment_id": "…",
  "acr_login_server": "acrmpodev.azurecr.io",
  "postgres": { "host": "…", "database": "dagster" },
  "compute_logs": { "account": "…", "container": "compute-logs" },
  "identities": {
    "platform": { "resource_id": "…", "client_id": "…", "principal_name": "…" },
    "webserver": { "resource_id": "…", "client_id": "…", "principal_name": "…" },
    "admin": { "resource_id": "…", "client_id": "…", "principal_name": "…" },
    "domains": { "sales": { "resource_id": "…", "client_id": "…", "principal_name": "…" } }
  },
  "auth": { "tenant_id": "…", "ui_app_client_id": "…", "ui_ro_app_client_id": "…" },
  "ingress": { "full_hostname": "…", "readonly_hostname": "…" },
  "platforms": {
    "databricks": { "host": "…" },
    "snowflake": { "account": "…", "domains": { "sales": { "role": "…", "warehouse": "…" } } }
  }
}
```

- The schema is the pydantic model in `orchestrator_core/contract.py` in the **solution** repo,
  which validates the contract when it reads it. The infra repo only publishes outputs and has no dependency on the solution repo.
  Breaking changes bump `schema_version`.
- The solution never reads Terraform state and never calls Terraform.

### 3.2 Modularity inside each repo

- **Infra**: `bootstrap/` (run once by a person), one `environment/` root applied per
  environment with `envs/<env>.tfvars`, and one module, `domain_access`, that turns
  `domains/<env>/<name>.yaml` (platforms, schemas/workspaces, read vs write) into the domain's
  identity and every grant. Adding a domain = one YAML file per environment.
- **Solution**: `orchestrator_core` (auth, resources, asset keys) is a library with no knowledge
  of specific domains. Platform integrations are **Dagster Components** (YAML-configured,
  reusable). Each domain is an independent code location with its own image and identity;
  domains can later move to their own repos without changing the core.

## 4. Lineage across platforms

The cross-platform DAG only exists if Dagster knows which asset reads which. Dagster's asset
graph **is** the DAG: scheduling, automation conditions, backfills, freshness and impact
analysis all run off asset dependencies.

### 4.1 Canonical asset keys = physical data

Every asset key identifies the **data object**, not the job that produces it:

| Platform | Key |
|---|---|
| Databricks (Unity Catalog) | `databricks/<catalog>/<schema>/<table>` |
| Snowflake | `snowflake/<database>/<schema>/<table>` |
| Fabric Lakehouse / Warehouse | `fabric/<workspace>/<item>/<schema>/<table>` |
| Fabric semantic model | `fabric/<workspace>/semantic_model/<name>` |

One function in `orchestrator_core` builds these keys; every component uses it, so two
components describing the same table produce the same key and connect automatically.

### 4.2 Where dependencies come from (*R12*)

| Source | How |
|---|---|
| **dbt** (all adapters) | `manifest.json` gives model → model deps automatically. dbt `sources` point to upstream assets on other platforms with dagster-dbt's native `meta.dagster.asset_key` on the source — this is the main glue between platforms, no extra code |
| **Fabric items, Databricks jobs, Snowflake SQL** | Declared in the component YAML: `reads:` / `writes:` with canonical keys. When the same data is visible on two platforms (OneLake shortcut, external table), the reader declares the writer's key directly |
| Tables loaded by systems outside Dagster | External assets (observed, not materialized) with observation sensors |

Declared lineage drives scheduling, reviewed in pull requests like any other code.

### 4.3 Example graph (end-to-end target)

```
fabric/sales/lh_raw/dbo/orders          (Fabric pipeline: ingest)
        │
databricks/sales/silver/orders          (Databricks job via Pipes)
        │
snowflake/SALES/MART/fct_orders         (dbt-snowflake model; source meta → databricks key)
        │
fabric/sales/semantic_model/Sales       (semantic model refresh)
```

### 4.4 Later, only when needed

Not built in the first version; added per platform only when declared lineage proves wrong in
practice:

- Lineage discovery (Unity Catalog lineage tables, Snowflake `ACCESS_HISTORY`, Fabric item
  definitions) and a declared-vs-discovered drift check — using only domain-scoped rights.
- Column-level lineage as asset metadata.
- Export to Microsoft Purview or OpenLineage.

## 5. Identity and least privilege

### 5.1 Identities (one set per environment, all created in the infra repo)

| Identity | Used by | Gets access to | Explicitly NOT |
|---|---|---|---|
| `id-dagster-platform-<env>` | daemon | Postgres `dagster_rw`; create/start/stop ACA Jobs in its RG (custom role); `Managed Identity Operator` on the domain identities (needed to attach them to run Jobs); ACR pull | Any data platform; schema changes |
| `id-dagster-web-<env>` | both webservers | Postgres `dagster_rw`, ACR pull, read the compute-log container (logs in the UI) | Data platforms, ACA Jobs |
| `id-dagster-admin-<env>` | admin jobs (§2.2) | Postgres Entra admin (schema owner, creates principals); delete ACA Jobs in its RG; ACR pull | Data platforms |
| `id-cl-<domain>-<env>` | that domain's code-location server **and** its run Jobs (*R1*, *R2*) | That domain's objects only (from `domains/<name>.yaml`); Postgres `dagster_rw`; ACR pull; compute-log container | Other domains' data; schema changes |
| `infra-deployer-<env>` | infra pipeline | Terraform apply for that env | Other environments |
| `app-deployer-<env>` | solution pipeline | ACR push; container app/job updates and Easy Auth settings in that env's RG; start `job-dagster-migrate`; read contract; test/prod: `AcrPull` on the dev ACR to copy images | Network, identities, grants, writing to other envs |

Deployer identities are Entra apps with federated credentials (GitHub OIDC and/or ADO workload
identity). Platform permissions are granted to Entra security groups containing the UAMIs.
Dev/test identities never have prod access.

### 5.2 Known limits, stated plainly (*R4*, *R10*)

- **The control plane can act as any domain in its environment.** To attach domain identities
  to run Jobs, the daemon identity holds `Managed Identity Operator` on them, and the app
  deployer can change what the daemon and code-location servers run. Anyone who controls either
  can start a Job with the `finance` identity. Mitigations:
  - **Azure Policy** on the environment's resource group: Container Apps and Jobs may only use
    images from the environment's own ACR.
  - Only `app-deployer-<env>` has `AcrPush`; deployments to test/prod need approval.
  - Daemon identity has no rights beyond the table above.
- **Domain separation does not cover people.** In open-source Dagster any Operator can launch
  any domain's job, which then runs with that domain's identity. If people must be separated per
  domain, the options are Dagster+ (role-based access) or one deployment per domain. No custom
  access layer is built. *Update:* open-source Dagster turned out to support per-code-location
  permissions through its request context; see [PRO_FEATURES_PLAN.md](PRO_FEATURES_PLAN.md) §2.

### 5.3 Subscription layout (both supported)

Each environment root module takes `subscription_id` and `resource_group_name` inputs:

- **Subscription per environment** (recommended for prod isolation).
- **Shared subscription**: same `subscription_id`, different resource groups; all role
  assignments are scoped to the resource group, never the subscription.

### 5.4 Setup inputs

`bootstrap/bootstrap.tfvars` holds the one-time inputs: tenant, subscription per environment
(or the same one three times), **location** (optional, default `westeurope`), naming prefix and
the CI systems (GitHub repo names and/or the ADO organization and project). Bootstrap creates
the state and contract storage (Entra auth only, public endpoint so hosted CI runners can
reach it), the shared Front Door profile, each environment's resource group and image policy,
and the federated deployers. It is the only step run by a person with elevated rights; its
outputs go into `environment/envs/<env>.tfvars` with the platform settings (Databricks URL,
Snowflake account, Entra groups).

Platform admins do a few things once (infra README): Snowflake `bootstrap.sql`, Databricks
workspace admin for the infra deployer, Fabric workspace admin. **Databricks job permissions
are not managed by Terraform**: `databricks_permissions` replaces a job's whole ACL, so job
owners grant `CAN_MANAGE_RUN` to the domain's service principal.

## 6. Networking, ingress and cost

### 6.1 Private where it matters, without paying for it (*R14*)

- VNet-integrated ACA environment (workload profiles, Consumption profile).
- **Internal environment, public network access disabled.** The only way in from the internet
  is Front Door over Private Link, and only to the two webserver apps. Daemon and code-location
  servers have internal-only ingress; jobs have no ingress.
- PostgreSQL: **VNet integration** (delegated subnet, private access only). Only the admin and
  Dagster processes inside the VNet reach it (§2.2).
- Compute-log storage: **service endpoint + firewall** allowing only the ACA subnet.
- ACR **Basic**: admin user off, anonymous pull off, Entra RBAC only.
- Data platforms are reached over their public endpoints with Entra / workload-identity auth.
- No Key Vault (no secrets exist), no private endpoints, no NAT Gateway in the first version.
  Add a NAT Gateway or a private endpoint only when a platform requires a fixed egress IP or
  private connectivity.

### 6.2 Internet ingress

**Azure Front Door Premium → Private Link → internal ACA environment**, then Container Apps
built-in authentication (Easy Auth) with Entra ID.

- **One Premium profile shared by dev, test and prod** ($330/month once, not per env), managed
  in its own Terraform root (`envs/shared`, separate state, prod-level approval). Each
  environment gets its own endpoint, custom domain, WAF policy, origin group and Private Link
  origin.
- **WAF (Prevention mode)**: Microsoft managed Default Rule Set + Bot Manager rule set, plus
  custom rules for rate limiting and optional geo/IP filtering. Dev/test policies run in
  Detection mode first to tune false positives before prod.
- **Health probes disabled** on every origin group (*R7*). Each group has one origin, where
  Microsoft recommends no probes; probes would hit the Entra login redirect, could mark the
  origin unhealthy, and would keep the webservers from scaling to zero.
- Private Link connection approved by Terraform (cross-subscription when environments are in
  separate subscriptions).
- **One Entra app registration per environment** (`mpo-ui-<env>`) with app roles
  `Dagster.Viewer`, `Dagster.Admin`, `Dagster.Launcher.<domain>`, `Dagster.Editor.<domain>`;
  user assignment required. One webserver enforces the roles per code location
  ([PRO_FEATURES_PLAN.md](PRO_FEATURES_PLAN.md) §2).
- **Conditional Access**: MFA, compliant device, block risky sign-ins.
- HTTPS only, managed certificates on Front Door, Easy Auth configured for the forwarded host
  (`X-Forwarded-Host`) so redirects use the Front Door domain.
- Webservers scale to zero when unused.

### 6.3 Cost

Full breakdown, unit prices and assumptions: **[COSTS.md](COSTS.md)**. Summary (list prices,
pay-as-you-go, excluding platform compute and Entra licences):

| dev | test | prod | Front Door Premium (shared) | **all** |
|---|---|---|---|---|
| ≈ $65–70 | ≈ $65–70 | ≈ $195–205 | ≈ $335–340 | **≈ $660–680** |

Biggest drivers: Front Door Premium base fee ($330), always-on daemon and code-location servers
(≈ $20 per 0.25 vCPU container), Postgres SKU (B1ms $15 vs B2s $58).

## 7. Platform connectors

All connectors are `ConfigurableResource`s in `orchestrator_core`, getting tokens from one
`AzureCredentialResource` (`ManagedIdentityCredential` in Azure, `AzureCliCredential` locally),
fetched per connection/request, never once per run.

| Platform | Integration | Auth | Grants (in infra repo) |
|---|---|---|---|
| Databricks | `databricks-sdk`: run existing jobs, wait, cancel (Pipes later, when jobs should stream logs to Dagster) | UAMI as Databricks SP, Entra token per request | Workspace user; `CAN_MANAGE_RUN` on specific jobs; UC grants on specific schemas |
| Snowflake | `snowflake-connector-python` | **Workload identity federation**: the connector gets an Entra token for the container's managed identity, and each domain's Snowflake service user trusts exactly that identity. No Entra app or OAuth integration needed. Locally: browser SSO with your own login | Role per domain; one warehouse; schema-level grants |
| Fabric | Custom (§7.1) | Entra tokens for Fabric API and Power BI API | SP tenant setting limited to a group; workspace role per workspace |

### 7.1 Fabric (*R13*)

Two components:

- **`FabricItemJob`** — any item with a job type, via the Run On Demand Item Job API: start,
  poll, cancel when the Dagster run is cancelled. `job_type` selects the item kind:
  `Pipeline` (data pipelines), `RunNotebook` (notebooks, with parameters and
  Lakehouse/environment config), `sparkjob`, and others.
- **`FabricSemanticModelRefresh`** — Power BI enhanced refresh API (tables/partitions, poll,
  cancel).

### 7.2 dbt

- `dagster-dbt`; one dbt project per domain/platform (`dbt-snowflake`, `dbt-databricks`,
  `dbt-fabric`); manifest built at image build time with `dbt parse`.
- Short-lived tokens from the run's MI passed to dbt via env vars in `profiles.yml`. Every
  `env_var()` has a placeholder default (`env_var('SNOWFLAKE_TOKEN', 'unset')`) so `dbt parse`
  works at build time when no token exists (*R16*).
- dbt sources point to upstream keys via `meta.dagster.asset_key` (§4.2); dbt tests become
  asset checks.

## 8. Solution repository layout (this repo)

```
multi-platform-orchestrator/
├── pyproject.toml                # uv workspace, ruff, pyright, pytest
├── libs/orchestrator_core/
│   └── src/orchestrator_core/
│       ├── auth.py               # AzureCredentialResource
│       ├── contract.py           # environment contract model (the schema) + loader
│       ├── asset_keys.py         # canonical keys
│       ├── resources/            # databricks, fabric, snowflake, adls
│       ├── components/           # FabricItemJob, FabricSemanticModelRefresh, DatabricksJob,
│       │                         # SnowflakeSql
│       ├── admin/                # migrate (principals + dagster instance migrate), cleanup
│       └── dbt/                  # translator, token → env helpers
├── code_locations/
│   └── example_sales/            # one domain = one code location = one identity = one image
│       ├── defs/                 # component YAML (assets, reads/writes, schedules)
│       └── dbt/
├── containers/Containerfile      # one file for all images (non-root, slim); SYNC_ARGS picks package
├── deploy/
│   ├── dagster.azure.yaml        # Postgres azure_wif, AcaRunLauncher, queue limits, blob logs
│   ├── dagster.local.yaml        # local Postgres, DefaultRunLauncher
│   ├── workspace.local.yaml
│   ├── aca/                      # container app / job templates rendered from the contract
│   └── scripts/                  # build.sh, push.sh, deploy.sh — called by both CI systems
├── local/compose.yaml            # podman compose stack
├── .github/workflows/            # thin wrappers around deploy/scripts
├── .azuredevops/pipelines/       # thin wrappers around deploy/scripts (+ templates)
└── docs/
```

The infra repo mirrors this: `modules/`, `envs/{shared,dev,test,prod}/`, `domains/*.yaml`,
`tests/` (`terraform test`), `scripts/` (setup, plan, apply, publish-contract),
`.github/workflows/`, `.azuredevops/pipelines/`.

## 9. CI/CD: GitHub Actions and Azure DevOps

The code lives on GitHub. Deployment can run from **GitHub Actions or Azure DevOps Pipelines**
(ADO pipelines use the GitHub repository as source via a GitHub service connection).

- **All logic lives in scripts** (`deploy/scripts/*.sh` in the solution repo, `scripts/*.sh` in
  infra), runnable locally. GitHub workflows and ADO pipelines are thin wrappers that install
  tools, log in to Azure and call the same scripts — no logic duplicated in YAML.
- **Secretless on both**:
  - GitHub Actions → Entra federated credential, subject `repo:<org>/<repo>:environment:<env>`.
  - Azure DevOps → Azure Resource Manager service connection with **workload identity
    federation**, issuer `https://vstoken.dev.azure.com/<org-id>`, subject
    `sc://<org>/<project>/<connection>`.
  - Federated credentials are bound to **environments**, so only approved deployment jobs can
    log in.
- **Pull-request CI has no cloud login at all** (*R5*): no `id-token: write`, no service
  connection. Code from a pull request never gets a token for any environment.
- **Approvals**: GitHub environments with required reviewers ↔ ADO environments with
  approvals and checks. Dev deploys on merge to `main`; test and prod need approval.
- **Promotion**: build once with Podman, scan (Trivy), push to the dev ACR, promote the **same
  digest** to test/prod (`az acr import`). Deployments pin images by digest.
- **Private resources** are never contacted from CI runners: database setup and migrations run
  in the in-VNet `job-dagster-migrate` (§2.2). CI only calls Azure Resource Manager, ACR and the
  bootstrap storage account.
- **Hardening**: GitHub actions pinned by SHA, Dependabot, CODEOWNERS on `deploy/`, `.github/`,
  `.azuredevops/`; ADO pipelines restricted to their service connections and environments.
- Pipelines per repo (identical stages in both systems):
  - infra: on PR `validate` (fmt, validate, tflint, checkov/trivy, **`terraform test`** for
    the access modules — *R9*) → after merge, per env with approval: `plan` → `apply` →
    `publish-contract`.
  - solution: on PR `ci` (ruff, pyright, pytest, `dg check`) → after merge: `build` → per env
    with approval: start `job-dagster-migrate` and wait → update apps and jobs → opt-in
    integration tests against dev (dev only, inside the dev environment's approval).

### 9.1 Tests of permission logic (*R9*)

`terraform test` (plan-only, mocked providers) on the access modules, with fixture domains:

- a domain marked `read` gets no `MODIFY` / `INSERT` / `OWNERSHIP` / Contributor grant;
- a domain never gets a grant on another domain's catalog, schema or workspace;
- domain identities never get `Managed Identity Operator`, `AcrPush` or Postgres admin.

## 10. Delivery phases

**Phase 0 — Spikes (dev)**
- A: `AcaRunLauncher` + per-code-location UAMI, incl. code-location server identity, `dagster_rw`
  Postgres login (verified locally: runs work without DDL rights, DDL is refused), the cleanup
  job, gRPC from webserver/daemon to `cl-<domain>` on port 80 (HTTP/2), and launch time per run
  (fallback AKS).
- B: Snowflake workload identity federation from an ACA Job's managed identity (the connector
  supports Container Apps' `IDENTITY_ENDPOINT`).
- C: Fabric pipeline/notebook run + semantic model refresh with an MI.
- D: dbt adapters with MI tokens.
- E: Front Door Premium → Private Link → internal ACA environment, Easy Auth with two Entra
  apps behind Front Door (forwarded host, redirects, probes off). Confirm the
  `az containerapp auth` flags in `deploy/scripts/deploy.sh` and sign-in without a client
  secret.

**Phase 1 — Solution skeleton (this repo)** — *done*
- uv workspace, lint/type/test, contract model + loader, `AzureCredentialResource`, canonical
  asset keys, example code location (lineage specs), Containerfile, Podman stack, CI for GitHub
  Actions and Azure DevOps.

**Phase 2 — Connectors, components and lineage** — *done*
- Resources: `FabricResource` (item jobs, semantic model refresh, 429 retry), `DatabricksResource`
  (run job), `SnowflakeResource` (External OAuth). All poll and cancel the remote job when the
  Dagster run stops.
- Components: `FabricItemJob`, `FabricSemanticModelRefresh`, `DatabricksJob`, `SnowflakeSql`,
  and `CanonicalDbtProject` (dbt with canonical keys and an Entra token for the profile).
- Tests use mocked APIs and a real dbt manifest (DuckDB stand-in). Opt-in integration tests
  against dev come with the Phase 0 spikes, which need Azure access.
- Not built: ADLS resource (nothing needs it yet), Databricks Pipes.

**Phase 3 — Infra repo** — *done on the `infra` branch, not yet applied*
- `bootstrap/`, `environment/`, `modules/domain_access`, domain YAML, Snowflake bootstrap SQL,
  GitHub and ADO pipelines over shared scripts.
- `terraform test` with mocked providers (12 runs): read access never writes, grants match the
  domain file, no admin rights for domain identities, nothing public except through Front
  Door, no passwords or keys, narrow control-plane roles, viewers cannot sign in to the full
  webserver, federated credentials bound to one repo and environment. Trivy config scan in CI.
- Validated with azurerm 5.10, azuread 3.10, databricks 1.139, snowflake 2.22, fabric 1.14.

**Phase 4 — App deployment** — *done, not yet run against Azure*
- `deploy/render.py` turns the contract + image digests into Container Apps and Jobs; tests
  check every container's identity and that only the webservers are reachable from Front Door.
- `dagster.azure.yaml` (Entra login to Postgres, `AcaRunLauncher`, run and pool limits, Blob
  compute logs) and `dagster.admin.yaml` (the only config that may create tables), both
  validated against Dagster's config schema.
- Admin jobs `migrate` and `cleanup` (`orchestrator_core.admin`); migrate verified on Postgres.
- `deploy/scripts/{push,promote,deploy}.sh`; GitHub `deploy.yml` + `promote.yml`; ADO
  `deploy.yml` + templates. Build once in dev, promote the same digests.

**Phase 5 — End-to-end example** (§4.3) in dev, with checks and alerts.

**Phase 6 — Hardening and docs**
- Security review, access review, runbooks, "onboard a new domain / platform" guides.

Phases 1–2 need no Azure access and can start immediately.

## 11. Risks

| Risk | Mitigation |
|---|---|
| ACA run launcher is new (beta) | Spike A; AKS fallback with same images |
| Control plane can act as any domain in its env | Stated limit (§5.2); ACR-only image policy, approvals, minimal daemon rights |
| OSS Dagster has no RBAC | Read-only vs full webserver behind Entra assignment; Dagster+ or one deployment per domain if people must be separated |
| UI on the internet | Front Door Premium WAF (managed + bot rules), Private Link only, Easy Auth, assignment required, Conditional Access |
| Shared Front Door profile couples environments | Separate endpoint, WAF policy and origin per env; own Terraform root with prod-level approval |
| Wrong grants reach prod | `terraform test` on access modules; plan reviewed before apply |
| Upgrade breaks the metadata schema | `job-dagster-migrate` runs before apps update; deploy stops on failure |
| Backfills overload Postgres or Azure APIs | Queue limits and per-platform tag limits |
| Two CI systems diverge | All logic in shared scripts; YAML wrappers only |
| Declared lineage misses an edge | Lineage reviewed in PRs; discovery added per platform only if this happens (§4.4) |
| Contract drift between repos | Solution validates with its contract model; `schema_version` |
| Fabric / Power BI API changes | Thin resource, integration tests against dev |
| dbt adapter auth differs | Spike D; token-via-env fallback |

## 12. Open items

1. Create the GitHub repo `bendfeldt/multi-platform-orchestrator-infra` (this session's GitHub
   integration may not create repositories), then move the `infra` branch there:
   `git push <infra-repo-url> origin/infra:main` and delete the branch here.
