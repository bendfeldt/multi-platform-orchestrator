# Multi-Platform Orchestrator — Implementation Plan

Status: **DRAFT v6 for review**. Nothing is implemented yet.

## 1. Goal

One Dagster deployment in Azure that orchestrates data assets on Databricks, Microsoft Fabric,
Snowflake and Azure SQL (later ADLS, ADF and others), with dbt for transformations, and with
**lineage across all of them** forming a single asset graph (DAG).

Hard requirements:

- **Entra ID everywhere**: humans sign in with Entra (OAuth/OIDC); workloads use managed
  identities / workload identity federation. **No stored secrets.**
- **Least privilege**: each identity can reach only what it needs. The orchestrator control
  plane cannot read business data.
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
| 4 | Fabric: any item with a job type; first-class pipelines, notebooks, semantic models. |
| 5 | dbt in scope. |
| 6 | Environments dev, test, prod. |
| 7 | UI reachable from the internet behind Entra ID OAuth. |
| 8 | Hosting: **Azure Container Apps** (confirmed). |
| 9 | Subscription layout: **both** supported — one subscription per environment, or one shared subscription with a resource group per environment. |
| 10 | Azure region: **setup input**, optional, default `westeurope`. |
| 11 | Snowflake: Business Critical (PrivateLink available); one account per environment assumed. |
| 12 | Databricks and Fabric workspaces **already exist** — Terraform only references them and manages identities/grants. |
| 13 | Ingress: **one Front Door Premium profile** shared by dev/test/prod, Private Link to each internal ACA environment, plus Container Apps built-in Entra auth (§6.2). |
| 14 | Both repositories live on **GitHub**. Deployment pipelines for **GitHub Actions and Azure DevOps**, same scripts underneath. |

## 2. Architecture

```
  Users (internet) ──► Front Door Premium (WAF managed rules + bot protection)
                                │ Private Link (no public endpoint on the environment)
                 ┌──────────────▼────────────────────────────────────────────────┐
  Azure VNet     │  Azure Container Apps environment (internal, VNet-integrated)  │
  (per env)      │  dagster-webserver (full)       ← Entra OAuth, group Operators  │
                 │  dagster-webserver --read-only  ← Entra OAuth, group Viewers    │
                 │  dagster-daemon (schedules, sensors, run queue)                │
                 │  code-location servers: one container app per domain           │
                 │  run workers: ACA Jobs, one per run (AcaRunLauncher)           │
                 └───┬──────────────┬──────────────┬─────────────────────────────┘
                     │ Entra token  │ private endpoints / PrivateLink
          PostgreSQL Flexible     Storage (compute logs)     Databricks · Fabric ·
          (Entra-only auth)                                  Snowflake · Azure SQL
```

Verified in the current Dagster release (`dagster-azure` / `dagster-postgres` 0.29.26):
`AcaRunLauncher` runs each run as its own ACA Job with a **per-code-location managed identity**;
`dagster-postgres` supports `auth_provider: azure_wif` (Entra token per connection, password
auth disabled); blob compute logs support `DefaultAzureCredential`. The ACA launcher is new →
Phase 0 spike, AKS fallback.

## 3. Separation of concerns: two repositories

Infrastructure and solution change at different speeds, are owned by different roles and need
different permissions. They are split into **two repositories with a versioned contract** between
them. Neither imports code from the other.

| | `multi-platform-orchestrator-infra` (new repo) | `multi-platform-orchestrator` (this repo) |
|---|---|---|
| Contains | Terraform only | Python, Dagster definitions, Containerfiles, app deployment, local Podman stack |
| Owns | Network, ACA environment, Postgres, ACR, storage, Front Door, Entra auth, monitoring, Entra groups & app registrations, managed identities, federated credentials, **platform grants** (Unity Catalog, Snowflake roles, Fabric workspace roles) | Core library, components, code locations, dbt projects, images, container app + job definitions, Dagster instance config |
| Deploy identity | `infra-deployer-<env>`: rights to create resources and role assignments (powerful, rarely used, approval-gated) | `app-deployer-<env>`: push to ACR + update container apps in one RG. Cannot change networking, identities or grants |
| Changes when | New environment, new domain (identity + grants), platform change | Code, assets, schedules, dbt models |
| Release cadence | Slow, reviewed by platform/security | Fast, reviewed by data engineers |

### 3.1 The contract

The infra pipeline publishes a **non-secret environment contract** per environment, e.g.
`contract/<env>.json`, to a storage container (read access via Entra RBAC for the app deployer):

```json
{
  "schema_version": 1,
  "environment": "dev",
  "location": "westeurope",
  "subscription_id": "…",
  "resource_group": "rg-mpo-dev",
  "aca_environment_id": "…",
  "acr_login_server": "acrmpodev.azurecr.io",
  "postgres": { "host": "…", "database": "dagster", "user": "id-dagster-platform-dev" },
  "compute_logs": { "account": "…", "container": "compute-logs" },
  "identities": {
    "platform": { "resource_id": "…", "client_id": "…" },
    "webserver": { "resource_id": "…", "client_id": "…" },
    "domains": { "sales": { "resource_id": "…", "client_id": "…" } }
  },
  "auth": { "tenant_id": "…", "ui_app_client_id": "…" },
  "ingress": { "full_hostname": "…", "readonly_hostname": "…" },
  "platforms": {
    "databricks": { "host": "…" },
    "snowflake": { "account": "…" },
    "fabric": { "workspaces": { "sales": "…" } }
  }
}
```

- The JSON Schema lives in the **solution** repo (it is the consumer) and is validated by both
  pipelines. Breaking changes bump `schema_version`.
- The solution never reads Terraform state and never calls Terraform.
- Locally, a hand-written `contract/local.json` points at the Podman stack and dev platforms.

### 3.2 Modularity inside each repo

- **Infra**: small Terraform modules (`network`, `aca_env`, `frontdoor`, `postgres`, `acr`,
  `storage`, `monitoring`, `entra`, `identity`, `access_databricks`, `access_snowflake`,
  `access_fabric`, `access_azure_sql`, `contract`). Each domain is declared once in a
  `domains/<name>.yaml` file (which platforms, catalogs/schemas/workspaces, read vs write);
  Terraform derives the identity and every grant from it. Adding a domain = one YAML file.
- **Solution**: `orchestrator_core` (auth, resources, lineage) is a library with no knowledge of
  specific domains. Platform integrations are **Dagster Components** (YAML-configured, reusable).
  Each domain is an independent code location with its own image and identity; domains can later
  move to their own repos without changing the core.

## 4. Lineage across platforms

Yes — the cross-platform DAG only exists if Dagster knows which asset reads which. Dagster's
asset graph **is** the DAG: scheduling, automation conditions, backfills, freshness and impact
analysis all run off asset dependencies. So lineage is a core design concern, not a reporting
add-on.

### 4.1 Canonical asset keys = physical data

Every asset key identifies the **data object**, not the job that produces it, using one
convention across all platforms:

| Platform | Key |
|---|---|
| Databricks (Unity Catalog) | `databricks/<catalog>/<schema>/<table>` |
| Snowflake | `snowflake/<database>/<schema>/<table>` |
| Fabric Lakehouse / Warehouse | `fabric/<workspace>/<item>/<schema>/<table>` |
| Fabric semantic model | `fabric/<workspace>/semantic_model/<name>` |
| Azure SQL | `azuresql/<server>/<database>/<schema>/<table>` |

A function in `orchestrator_core.lineage` builds and parses these keys; every component uses it,
so two components describing the same table always produce the same key and connect
automatically. When the same data is visible on two platforms (OneLake shortcut, Snowflake
external table on ADLS, Databricks reading OneLake) an **alias map** links the keys.

### 4.2 Where dependencies come from

| Source | How lineage is obtained | Mode |
|---|---|---|
| **dbt** (all adapters) | `manifest.json`: model → model deps automatically; dbt `sources` translated to canonical keys of upstream assets (custom `DagsterDbtTranslator`) — the main glue between platforms | Automatic |
| **Fabric pipelines / notebooks** | Declared in component YAML (`reads:` / `writes:` tables). Optional discovery: item definition API (pipeline JSON source/sink, notebook Lakehouse bindings) | Declared + discovered |
| **Fabric semantic models** | Item definition (TMDL) → source tables of each partition | Discovered, declared override |
| **Databricks jobs** | Declared in component YAML. Optional discovery: Unity Catalog lineage system tables (`system.access.table_lineage`) | Declared + discovered |
| **Snowflake SQL / tasks** | Declared. Optional discovery: `ACCOUNT_USAGE.ACCESS_HISTORY` / `OBJECT_DEPENDENCIES` | Declared + discovered |
| Tables loaded by systems outside Dagster | External assets (observed, not materialized) with observation sensors | Declared |

Rule: **declared lineage drives scheduling; discovered lineage validates and enriches.**
Auto-generating the scheduling graph purely from platform lineage would make the DAG change
whenever someone runs an ad-hoc query, and discovery often needs broad read rights (e.g. Fabric
admin scanner APIs) that conflict with least privilege. Instead:

- A **lineage drift check** (CI job + scheduled Dagster asset check) compares declared vs
  discovered lineage per domain and reports missing or unexpected edges.
- Discovery uses only each domain's own identity and own grants — no tenant-admin APIs.
- Column-level lineage from dbt and Unity Catalog is attached as asset metadata where available.

### 4.3 Example graph (end-to-end target)

```
fabric/sales/lh_raw/dbo/orders          (Fabric pipeline: ingest)
        │
databricks/sales/silver/orders          (Databricks job via Pipes)
        │
snowflake/SALES/MART/fct_orders         (dbt-snowflake model; source = databricks key via alias/external table)
        │
fabric/sales/semantic_model/Sales       (semantic model refresh)
```

### 4.4 Lineage outside Dagster (later)

Optional export of the Dagster asset graph to **Microsoft Purview** or an **OpenLineage**
endpoint so governance tools see the same lineage. Not required for the DAG itself.

## 5. Identity and least privilege

### 5.1 Identities (one set per environment, all created in the infra repo)

| Identity | Type | Gets access to | Explicitly NOT |
|---|---|---|---|
| `id-dagster-platform-<env>` | UAMI | Metadata Postgres, create/start ACA Jobs in its RG (custom role), ACR pull, compute-log container | Any data platform |
| `id-dagster-web-<env>` | UAMI | Metadata Postgres, ACR pull | Data platforms, ACA Jobs |
| `id-cl-<domain>-<env>` | UAMI | Only that domain's objects (from `domains/<name>.yaml`) | Other domains |
| `infra-deployer-<env>` | Entra app, Federated credential (GitHub OIDC and/or ADO workload identity) | Terraform apply for that env | Other environments |
| `app-deployer-<env>` | Entra app, Federated credential (GitHub OIDC and/or ADO workload identity) | ACR push, container app/job updates in that env's RG, read contract | Network, identities, grants, other envs |

- Run workers inherit their code location's identity; `sales` runs cannot touch `finance`.
- Platform permissions are granted to Entra security groups containing the UAMIs.
- Dev/test identities never have prod access.

### 5.2 Subscription layout (both supported)

Each environment root module takes `subscription_id` and `resource_group_name` inputs:

- **Subscription per environment** (recommended for prod isolation): different `subscription_id`
  per env.
- **Shared subscription**: same `subscription_id`, different resource groups; all role
  assignments are scoped to the resource group, never the subscription.

### 5.3 Setup inputs

A `setup` script in the infra repo asks for (or reads from a `setup.yaml`) and writes
`envs/<env>/terraform.tfvars`:

- tenant ID, subscription ID per env (or one shared), resource-group names
- **location** — optional, default `westeurope`
- naming prefix; CI provider(s) (`github`, `ado` or both) with GitHub repo names and/or ADO
  organization/project/service-connection names (for the federated credentials)
- custom domain names per environment, `private_endpoints` list, SKUs (§6.3)
- existing Databricks workspace URLs, Snowflake account per env, Fabric workspace IDs
- Entra group object IDs for Viewers / Operators (or create them)

It then bootstraps Terraform state storage (Entra auth, no keys) and the deployer identities with
their federated credentials. Bootstrap is the only step run by a human with elevated rights.

## 6. Networking, ingress and cost

### 6.1 Private by default, without paying for it

- VNet-integrated ACA environment (workload profiles, Consumption profile).
- **Internal environment, public network access disabled.** The only way in from the internet
  is Front Door over Private Link, and only to the two webserver apps. Daemon and code-location
  servers have internal-only ingress; run jobs have no ingress.
- PostgreSQL: **VNet integration** (delegated subnet, private access only) — no private
  endpoint needed.
- Storage, Key Vault, Azure SQL: **service endpoints + resource firewall** allowing only the ACA
  subnet; public access otherwise denied. Free, unlike private endpoints.
- ACR **Basic**: admin user off, anonymous pull off, Entra RBAC only (private endpoint would
  require Premium at ≈ $50/month).
- Private endpoints remain a per-service toggle (`private_endpoints`) for stricter policies,
  e.g. Databricks / Snowflake PrivateLink (≈ $7.30/month each).
- Egress via the environment's default outbound; NAT Gateway optional, only if a platform must
  allowlist a fixed egress IP.

### 6.2 Internet ingress

**Azure Front Door Premium → Private Link → internal ACA environment**, then Container Apps
built-in authentication (Easy Auth) with Entra ID.

- **One Premium profile shared by dev, test and prod** ($330/month once, not per env), managed
  in its own Terraform root (`envs/shared`, separate state, prod-level approval). Each
  environment gets its own endpoint, custom domain, WAF policy, origin group and Private Link
  origin, so routes and rules are separated per environment.
- **WAF (Prevention mode)**: Microsoft managed Default Rule Set + Bot Manager rule set, plus
  custom rules for rate limiting and optional geo/IP filtering. Dev/test policies can run in
  Detection mode first to tune false positives before prod.
- Private Link origin: the ACA environment has no public endpoint at all; the Private Link
  connection is approved by Terraform (cross-subscription when environments are in separate
  subscriptions).
- **Two Entra app registrations per environment**: `mpo-ui-<env>` (full webserver) and
  `mpo-ui-ro-<env>` (read-only). User assignment required: only `Dagster-Operators` on the full
  app; `Dagster-Viewers` + `Dagster-Operators` on the read-only app. A viewer cannot sign in to
  the full webserver.
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
| Databricks | `dagster-databricks` (Jobs + Pipes) | UAMI as Databricks SP, Entra token | Workspace user; `CAN_MANAGE_RUN` on specific jobs; UC grants on specific schemas |
| Snowflake | `dagster-snowflake` | Workload Identity Federation (`WORKLOAD_IDENTITY`, AZURE); fallback External OAuth | Role per domain; one warehouse; schema-level grants |
| Azure SQL | Custom (pyodbc, ODBC 18) | Entra access token; Entra-only server | Contained user, schema-level roles |
| Fabric | Custom (§7.1) | Entra tokens for Fabric API and Power BI API | SP tenant setting limited to a group; workspace role per workspace |
| ADLS / Blob | `dagster-azure` | MI | Data role on a specific container |

### 7.1 Fabric

- Generic item job runner (Run On Demand Item Job API, poll, cancel) for any item with a job type.
- Components: `FabricPipeline`, `FabricNotebook` (parameters, Lakehouse/environment),
  `FabricSemanticModelRefresh` (Power BI enhanced refresh API, tables/partitions).
- Lineage per §4.2.

### 7.2 dbt

- `dagster-dbt`; one dbt project per domain/platform (`dbt-snowflake`, `dbt-databricks`,
  `dbt-fabric`); manifest built at image build time.
- Short-lived tokens from the run's MI passed to dbt via env vars in `profiles.yml`.
- dbt sources map to canonical keys (§4.1); dbt tests become asset checks.

## 8. Solution repository layout (this repo)

```
multi-platform-orchestrator/
├── pyproject.toml                # uv workspace, ruff, pyright, pytest
├── libs/orchestrator_core/
│   └── src/orchestrator_core/
│       ├── auth.py               # AzureCredentialResource
│       ├── contract.py           # load + validate environment contract
│       ├── lineage/              # canonical keys, alias map, discovery, drift check
│       ├── resources/            # databricks, fabric, snowflake, azure_sql, adls
│       ├── components/           # FabricPipeline, FabricNotebook, FabricSemanticModelRefresh,
│       │                         # DatabricksJob, SnowflakeSql, AzureSqlProcedure, ...
│       └── dbt/                  # translator, token → env helpers
├── code_locations/
│   └── example_sales/            # one domain = one code location = one identity = one image
│       ├── defs/                 # component YAML (assets, reads/writes, schedules)
│       └── dbt/
├── contract/
│   ├── schema.json               # contract JSON Schema (source of truth)
│   └── local.json
├── containers/                   # Containerfiles (non-root, slim, pinned)
├── deploy/
│   ├── dagster.azure.yaml        # Postgres azure_wif, AcaRunLauncher, blob compute logs
│   ├── dagster.local.yaml        # local Postgres, DockerRunLauncher on Podman socket
│   ├── aca/                      # container app / job templates rendered from the contract
│   └── scripts/                  # build.sh, push.sh, deploy.sh — called by both CI systems
├── local/compose.yaml            # podman compose stack
├── .github/workflows/            # thin wrappers around deploy/scripts
├── .azuredevops/pipelines/       # thin wrappers around deploy/scripts (+ templates)
└── docs/
```

The infra repo mirrors this: `modules/`, `envs/{dev,test,prod}/`, `domains/*.yaml`,
`scripts/` (setup, plan, apply, publish-contract), `.github/workflows/`, `.azuredevops/pipelines/`.

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
  - The infra setup creates federated credentials for whichever providers are selected; one
    deployer identity per repo per environment either way.
- **Approvals**: GitHub environments with required reviewers ↔ ADO environments with
  approvals and checks. Dev deploys on merge to `main`; test and prod need approval.
- **Promotion**: build once with Podman, scan (Trivy), push to the dev ACR, promote the **same
  digest** to test/prod (`az acr import`). Deployments pin images by digest.
- **Hardening**: GitHub actions pinned by SHA, minimal `permissions:` (`id-token: write` only
  where Azure login is needed), Dependabot, CODEOWNERS on `deploy/`, `.github/`,
  `.azuredevops/`; ADO pipelines restricted to their service connections and environments.
- Pipelines per repo (identical stages in both systems):
  - infra: `validate` (fmt, validate, tflint, checkov/trivy) and `plan` on PR →
    `apply` + `publish-contract` per env with approval.
  - solution: `ci` (ruff, pyright, pytest, `dg check`, lineage drift vs dev) on PR →
    `build` → `deploy` per env with approval.

## 10. Delivery phases

**Phase 0 — Spikes (dev)**
- A: `AcaRunLauncher` + per-code-location UAMI (fallback AKS).
- B: Snowflake WIF from an ACA Job MI.
- C: Fabric pipeline/notebook run + semantic model refresh with an MI.
- D: dbt adapters with MI tokens.
- E: Front Door Premium → Private Link → internal ACA environment, Easy Auth with two Entra
  apps behind Front Door (forwarded host, redirects).
- F: Lineage discovery feasibility per platform with domain-scoped (not admin) rights.

**Phase 1 — Solution skeleton (this repo)**
- uv workspace, lint/type/test, contract schema + loader, `AzureCredentialResource`,
  canonical asset-key module, example code location, Containerfiles, Podman stack,
  CI for GitHub Actions and Azure DevOps.

**Phase 2 — Connectors and components**
- Azure SQL → Snowflake → Databricks (Pipes) → Fabric (pipeline, notebook, semantic model,
  generic) → dbt → ADLS. Unit tests with mocked APIs; opt-in integration tests against dev.

**Phase 3 — Lineage**
- dbt translator to canonical keys, alias map, discovery adapters, drift check.

**Phase 4 — Infra repo**
- Setup/bootstrap, modules, domain YAML → identities + grants, contract publishing,
  GitHub Actions and ADO pipelines. Dev first, then test and prod.

**Phase 5 — App deployment**
- Contract-driven ACA templates, deploy scripts, image promotion, GitHub Actions and ADO pipelines.

**Phase 6 — End-to-end example** (§4.3) in dev, with checks and alerts.

**Phase 7 — Hardening and docs**
- Security review, access review, runbooks, "onboard a new domain / platform" guides.

Phases 1–3 need no Azure access and can start immediately.

## 11. Risks

| Risk | Mitigation |
|---|---|
| ACA run launcher is new | Spike A; AKS fallback with same images |
| OSS Dagster has no RBAC | Read-only vs full webserver behind Entra groups; Dagster+ if finer roles needed |
| UI on the internet | Front Door Premium WAF (managed + bot rules), Private Link only, Easy Auth, assignment required, Conditional Access |
| Shared Front Door profile couples environments | Separate endpoint, WAF policy and origin per env; own Terraform root with prod-level approval |
| Two CI systems diverge | All logic in shared scripts; YAML wrappers only; both run in CI of this repo |
| Lineage discovery needs broad rights | Declared lineage drives the DAG; discovery is domain-scoped and only validates |
| Declared lineage drifts from reality | Drift check in CI and as scheduled asset check |
| Contract drift between repos | JSON Schema validated in both pipelines, `schema_version` |
| Fabric / Power BI API changes | Thin resource, integration tests against dev |
| dbt adapter auth differs | Spike D; token-via-env fallback |

## 12. Open items

1. Create the GitHub repo `bendfeldt/multi-platform-orchestrator-infra`.
