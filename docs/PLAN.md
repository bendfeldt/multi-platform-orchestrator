# Multi-Platform Orchestrator — Implementation Plan

Status: **DRAFT v2 for review**. Nothing is implemented yet. Decisions taken are in §1.1.

## 1. Goal

One Dagster deployment in Azure that orchestrates data assets on Databricks, Microsoft Fabric,
Snowflake and Azure SQL (later ADLS, ADF and others), with dbt for transformations.

Hard requirements:

- **Entra ID everywhere**: humans sign in with Entra (OAuth/OIDC); workloads use managed
  identities / workload identity federation. No stored secrets.
- **Least privilege**: each workload identity can reach only the systems and objects it needs.
  The orchestrator control plane cannot read business data.
- **Runs locally and in Azure** from the same container images.

Design principle: **Dagster orchestrates, the platforms compute.** Dagster triggers Databricks
jobs, Fabric items, Snowflake SQL and dbt, and records lineage, metadata and checks. It does not
pull large datasets through its own containers.

### 1.1 Decisions

| # | Decision |
|---|---|
| 1 | **Podman** for building and running containers. Images are standard OCI (`Containerfile`), so the same images run locally under Podman and in Azure Container Apps. |
| 2 | **Terraform** for all infrastructure — Azure *and* platform-side grants (Databricks, Snowflake, Fabric). |
| 3 | **On-prem is out of scope** for now. The connector design leaves room to add it later. |
| 4 | **Fabric**: support any item with a job type, with first-class support for **pipelines, notebooks and semantic models**. |
| 5 | **dbt is in scope** (`dagster-dbt`). |
| 6 | Three environments: **dev, test, prod**. |
| 7 | UI reachable **from the internet behind Entra ID OAuth**. |

Assumed (confirm): Azure hosting on **Azure Container Apps** (see §2.1).

## 2. Architecture

```
  Users (internet) ──► Azure Front Door Premium + WAF ──► Private Link
                                                             │
                 ┌───────────────────────────────────────────▼───────────────────┐
  Azure VNet     │  Azure Container Apps environment (internal, VNet-integrated)  │
  (per env)      │                                                                │
                 │  dagster-webserver (full)       ← Entra OAuth, group Operators  │
                 │  dagster-webserver --read-only  ← Entra OAuth, group Viewers    │
                 │  dagster-daemon (schedules, sensors, run queue)                │
                 │  code-location servers: one container app per domain           │
                 │  run workers: ACA Jobs, one per run (AcaRunLauncher)           │
                 └───┬──────────────┬──────────────┬─────────────────────────────┘
                     │ Entra token  │ private endpoints / PrivateLink
          PostgreSQL Flexible     Storage (compute logs,      Databricks · Fabric ·
          (Dagster metadata,      dbt artifacts)              Snowflake · Azure SQL
          Entra-only auth)
```

### 2.1 Hosting: Azure Container Apps

Verified in the current Dagster release (`dagster-azure` / `dagster-postgres` 0.29.26):

- `dagster_azure.container_apps.AcaRunLauncher` runs **each Dagster run as its own ACA Job** and
  accepts a **user-assigned managed identity per code location** (`container_context.aca.identity_id`).
- `dagster_postgres` supports `auth_provider: azure_wif` — a fresh Entra token is injected as the
  password on each connection, so the metadata DB runs with **password auth disabled**.
- `dagster_azure` blob compute log manager supports `DefaultAzureCredential`.

The ACA launcher is new, so Phase 0 includes a spike. Fallback: AKS + the official Dagster Helm
chart (same images, same code).

### 2.2 Internet access behind Entra

- **Azure Front Door Premium** with WAF (managed OWASP rules, rate limiting, geo filtering if
  wanted) is the only public entry point. It reaches the **internal** ACA environment over
  Private Link, so no Container App has a public IP.
- **Entra OAuth/OIDC sign-in** on the webserver apps via Container Apps built-in authentication
  (Easy Auth). The enterprise app requires user assignment; only the `Dagster-Viewers` and
  `Dagster-Operators` groups are assigned.
- Viewers → read-only webserver (`dagster-webserver --read-only`), Operators → full webserver,
  on separate hostnames. Open-source Dagster has no RBAC, so this split is the access control.
- Conditional Access (MFA, compliant device, sign-in risk) applies automatically.
- Simpler, cheaper alternative if Front Door is not wanted: external ACA ingress + Easy Auth only
  (still Entra-protected, but no WAF and the environment has a public endpoint).

### 2.3 Local runtime (Podman)

- `podman compose` stack: Postgres, Azurite (blob), dagster-webserver, dagster-daemon, code
  location containers — the same images as Azure.
- Local run launcher: `dagster-docker`'s `DockerRunLauncher` pointed at the Podman socket
  (Docker-compatible API), so each run gets its own container, like ACA Jobs in Azure.
- Local auth: `AzureCliCredential` (developer's own Entra login via `az login`) against **dev**
  platforms only. No Entra sign-in in front of the local UI.
- `make` / `just` targets: `up`, `down`, `build`, `test`, `lint`.

## 3. Identity and least-privilege model

### 3.1 Identities (one set per environment)

| Identity | Type | Gets access to | Explicitly NOT |
|---|---|---|---|
| `id-dagster-platform-<env>` | UAMI | Metadata Postgres (Dagster schema), create/start ACA Jobs in its own RG (custom role, not Contributor), ACR pull, compute-log container | Any data platform |
| `id-dagster-web-<env>` | UAMI | Metadata Postgres, ACR pull | Any data platform, ACA Jobs |
| `id-cl-<domain>-<env>` (one per code location) | UAMI | Only that domain's objects on each platform (§4), its own storage paths | Other domains, metadata DB DDL |
| `gh-deploy-<env>` | Entra app + GitHub OIDC federated credential (subject `repo:bendfeldt/multi-platform-orchestrator:environment:<env>`) | Push to ACR, update container apps in that env's RG | Data platforms, other environments |
| `gh-tf-<env>` | Entra app + GitHub OIDC | Terraform apply for that env (scoped RG roles + platform admin rights needed to grant) | Other environments |

- Run workers inherit the code location's identity, so a `sales` run cannot touch `finance`.
- Role assignments are scoped to the narrowest resource (container, database, workspace).
- Platform-side permissions are granted to **Entra security groups** containing the UAMIs.
- Dev/test identities never get prod access. Separate subscriptions per environment
  are recommended; separate resource groups are the minimum.

### 3.2 Secrets

Goal: **zero stored secrets**. Every in-scope platform supports Entra/workload identity. A Key
Vault (RBAC mode, private endpoint) is still provisioned for exceptions, with per-secret grants.

## 4. Platform connectors

All connectors live in `orchestrator_core` as Dagster `ConfigurableResource`s and get tokens from
one `AzureCredentialResource` (`ManagedIdentityCredential` in Azure, `AzureCliCredential` locally).
Tokens are fetched per connection/request via azure-identity's cache, never once per run.

| Platform | Dagster integration | Auth (no secrets) | Least-privilege grants (in Terraform) |
|---|---|---|---|
| **Databricks (Azure)** | `dagster-databricks` (Jobs + **Pipes**) | UAMI registered as Databricks service principal; Entra token as bearer | Workspace user (not admin); `CAN_MANAGE_RUN` on specific jobs; Unity Catalog grants on specific catalogs/schemas; cluster policies |
| **Snowflake** | `dagster-snowflake` | Snowflake **Workload Identity Federation** (`authenticator=WORKLOAD_IDENTITY`, provider AZURE); fallback External OAuth with Entra | Service user + role per domain; one warehouse; grants on specific DB/schemas; no `SYSADMIN`/`ACCOUNTADMIN` |
| **Azure SQL** | Custom resource (pyodbc, ODBC Driver 18) | Entra token via `SQL_COPT_SS_ACCESS_TOKEN`; server Entra-only | Contained user `FROM EXTERNAL PROVIDER`; schema-level roles; no `db_owner` |
| **Microsoft Fabric** | **Custom resource** (no official integration) — see §4.1 | Entra tokens: Fabric API `https://api.fabric.microsoft.com/.default`, Power BI API `https://analysis.windows.net/powerbi/api/.default` | Tenant setting "Service principals can use Fabric APIs" limited to one security group; workspace role per workspace (Contributor where items run, Viewer where only read) |
| **ADLS Gen2 / Blob** | `dagster-azure` | MI | `Storage Blob Data Contributor` on a specific container |

### 4.1 Fabric

- **Generic item job runner**: Fabric REST "Run On Demand Item Job"
  (`POST /v1/workspaces/{ws}/items/{item}/jobs/instances?jobType=...`) → poll the job instance
  until done → surface status, duration and failure reason as Dagster metadata. Works for any
  item type that has a job type (Spark job definitions, copy jobs, etc.).
- **First-class asset factories**:
  - `fabric_pipeline_asset` — Data Pipeline (`jobType=Pipeline`), with optional parameters.
  - `fabric_notebook_asset` — Notebook (`jobType=RunNotebook`), with parameters and Lakehouse/
    environment config.
  - `fabric_semantic_model_asset` — semantic model refresh via the Power BI **enhanced refresh**
    API (`POST /groups/{ws}/datasets/{id}/refreshes`) with tables/partitions and polling.
- Cancellation: terminating a Dagster run cancels the Fabric job instance / refresh.
- Optional: Fabric Warehouse / Lakehouse SQL endpoint via ODBC (same code as Azure SQL) for
  asset checks.

### 4.2 dbt

- `dagster-dbt` with each dbt model as a Dagster asset, so dbt lineage joins the cross-platform
  graph (e.g. Fabric pipeline → Snowflake dbt models → Fabric semantic model refresh).
- One dbt project per domain/platform: `dbt-snowflake`, `dbt-databricks`, `dbt-fabric`.
- Auth without secrets: the Dagster resource mints a short-lived Entra/WIF token for the run's
  managed identity and passes it to dbt via environment variables in `profiles.yml`
  (Snowflake OAuth token / WIF, Databricks Entra token as `token`, Fabric `ServicePrincipal`→
  token or `auto`). Exact per-adapter method confirmed in Spike D.
- The dbt manifest is built at image build time (`dbt parse`) so code-location startup is fast.
- dbt tests become Dagster asset checks.

### 4.3 Cross-platform asset model

- Asset keys prefixed by platform and domain: `["snowflake", "sales", "orders"]`,
  `["databricks", "sales", "orders_enriched"]`, `["fabric", "sales", "orders_model"]`.
- Assets owned by other tools are declared as external assets and observed via sensors.
- Asset checks (row counts, freshness, schema) and declarative automation
  (`AutomationCondition`) instead of fixed cron where possible.

## 5. Repository layout

```
multi-platform-orchestrator/
├── pyproject.toml               # uv workspace root, ruff, pyright, pytest
├── libs/orchestrator_core/
│   └── src/orchestrator_core/
│       ├── auth.py              # AzureCredentialResource, token helpers
│       ├── resources/           # databricks.py, fabric.py, snowflake.py, azure_sql.py, adls.py
│       ├── assets/              # factories: fabric_*_asset, sql_asset, ...
│       ├── dbt/                 # token → dbt env helpers
│       └── checks/
├── code_locations/
│   └── example_sales/           # one code location = one domain = one identity = one image
│       └── dbt/                 # dbt project(s) for this domain
├── containers/
│   ├── Containerfile.dagster    # webserver + daemon (non-root, slim, pinned)
│   └── Containerfile.codeloc    # base for code locations
├── deploy/
│   ├── dagster.azure.yaml       # Postgres azure_wif, AcaRunLauncher, blob compute logs
│   ├── dagster.local.yaml       # Postgres (local), DockerRunLauncher on Podman socket
│   └── workspace.yaml
├── local/compose.yaml           # podman compose stack
├── infra/                       # Terraform
│   ├── modules/                 # network, aca, frontdoor, postgres, identities, acr, storage,
│   │                            # monitoring, entra (apps/groups), databricks, snowflake, fabric
│   └── envs/{dev,test,prod}/    # root modules, remote state in Azure Storage (Entra auth)
├── .github/workflows/           # ci.yml, build.yml (podman build), deploy.yml, terraform.yml
└── docs/                        # PLAN.md, ARCHITECTURE.md, onboarding guides, runbooks
```

### 5.1 Terraform

- Providers: `azurerm`, `azuread`, `databricks`, `Snowflake-Labs/snowflake`, `microsoft/fabric`.
- State: Azure Storage per environment, `use_azuread_auth = true` (no storage keys), versioning
  and soft delete on.
- Platform grants (Unity Catalog, Snowflake roles, Fabric workspace roles) are in Terraform too,
  so every permission is reviewable in a PR.
- CI: `terraform fmt/validate`, `tflint`, `checkov`/`trivy` on every PR; `plan` on PR,
  `apply` on merge for dev, with GitHub environment approvals for test and prod.

### 5.2 CI/CD

- `ci.yml`: ruff, pyright, pytest (platforms mocked), `dagster definitions validate`.
- `build.yml`: `podman build` images, vulnerability scan, push to ACR (via OIDC).
- `deploy.yml`: promote the **same image digest** dev → test → prod, with approvals.

## 6. Hardening

- ACA environment internal; Front Door + WAF is the only public ingress.
- Private endpoints + private DNS: Postgres, ACR, Storage, Key Vault, Azure SQL, Databricks
  (Private Link), Snowflake (PrivateLink, Business Critical edition required — otherwise network
  policy allowlisting the NAT egress IP). Fabric: workspace-level private links where available,
  otherwise Entra Conditional Access for workload identities.
- Egress through NAT Gateway / Azure Firewall with an allowlist.
- Images non-root, dependencies pinned, scanned in CI; ACR admin user disabled.
- Azure Policy: deny public network access on data services; require Entra-only auth.
- Logs: stdout → Log Analytics; compute logs → Blob; run-failure alerts → Teams/email.

## 7. Delivery phases

**Phase 0 — Spikes (in a dev subscription)**
- A: `AcaRunLauncher` with per-code-location UAMI end to end (fallback AKS).
- B: Snowflake WIF from an ACA Job managed identity.
- C: Fabric pipeline/notebook run + semantic model refresh with a managed identity.
- D: dbt adapters (Snowflake, Databricks, Fabric) with tokens from the managed identity.
- E: Front Door → Private Link → internal ACA with Easy Auth.

**Phase 1 — Skeleton and local dev**
- uv workspace, ruff, pyright, pytest, pre-commit, CI.
- `orchestrator_core` with `AzureCredentialResource` and the example code location.
- Containerfiles and `podman compose` stack running Dagster locally.

**Phase 2 — Connectors** (one PR each, unit tests + opt-in integration tests against dev)
- Azure SQL → Snowflake → Databricks (Pipes) → Fabric (pipelines, notebooks, semantic models,
  generic items) → dbt integration → ADLS.

**Phase 3 — Terraform**
- Bootstrap (state storage, GitHub OIDC apps), then network, ACA, Front Door, Postgres, ACR,
  storage, monitoring, identities, Entra apps/groups, and platform grants. Dev first.

**Phase 4 — Deployment**
- `dagster.azure.yaml`, deploy pipeline with image promotion dev → test → prod.

**Phase 5 — End-to-end example**
- Fabric pipeline → Databricks job → Snowflake dbt models → Fabric semantic model refresh,
  with asset checks and alerts, deployed to dev.

**Phase 6 — Hardening and docs**
- Security review, access review of every grant, runbooks, "onboard a new domain/platform" guide.

## 8. Risks

| Risk | Mitigation |
|---|---|
| ACA run launcher is new | Spike A first; AKS fallback with same images |
| OSS Dagster has no RBAC | Read-only vs full webserver behind Entra groups; Dagster+ if finer roles are needed |
| UI exposed to the internet | Front Door WAF + Private Link, Entra with Conditional Access, user assignment required |
| Fabric / Power BI APIs and SP support change often | Thin custom resource, integration tests against a real dev workspace |
| dbt adapter auth with managed identity differs per adapter | Spike D; token-via-env-var fallback |
| Snowflake PrivateLink needs Business Critical | Network policy on NAT egress IP as fallback |
| Token expiry in long runs | Fetch tokens per connection/request from azure-identity cache |

## 9. Remaining open questions

1. Confirm **Azure Container Apps** for hosting (vs AKS / Dagster+ Hybrid).
2. Separate **Azure subscriptions** per environment, or one subscription with resource groups?
3. Which Azure region(s)?
4. Snowflake edition (affects PrivateLink) and whether the Snowflake account(s) are per
   environment.
5. Are Databricks and Fabric workspaces already existing (Terraform imports/grants only), or
   should Terraform create them?
6. Front Door + WAF (recommended) or the simpler external-ingress option for the UI?
