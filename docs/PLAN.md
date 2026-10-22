# Multi-Platform Orchestrator — Implementation Plan

Status: **DRAFT for review**. Nothing is implemented yet. Open decisions are in §9.

## 1. Goal

One Dagster deployment in Azure that orchestrates data assets on Databricks, Microsoft Fabric,
Snowflake, Azure SQL, on-prem SQL Server, on-prem PostgreSQL (and later ADLS, ADF, dbt, Power BI).

Hard requirements:

- **Entra ID everywhere it is technically possible**: humans sign in with Entra; workloads use
  managed identities / workload identity federation. No stored secrets where Entra auth exists.
- **Least privilege**: each workload identity can reach only the systems and objects it needs.
  The orchestrator control plane cannot read business data.

Design principle: **Dagster orchestrates, the platforms compute.** Dagster triggers Databricks
jobs, Fabric items, Snowflake SQL/dbt, etc., and records lineage, metadata and checks. It does not
pull large datasets through its own containers.

## 2. Architecture

```
                 Entra ID (users + groups)            GitHub Actions (OIDC, no secrets)
                         │                                     │
                 ┌───────▼──────────────────────────────────────▼───────────────┐
  Azure VNet     │  Azure Container Apps environment (internal, private)         │
  (private only) │                                                               │
                 │  dagster-webserver (full)      ← Easy Auth, group: Operators   │
                 │  dagster-webserver --read-only ← Easy Auth, group: Viewers     │
                 │  dagster-daemon (schedules, sensors, run queue)               │
                 │  code-location servers: one container app per domain          │
                 │  run workers: ACA Jobs, one per run (AcaRunLauncher)          │
                 └───┬─────────────┬─────────────┬──────────────┬────────────────┘
                     │ Entra token │ Entra token │ private ep.  │ VPN / ExpressRoute
          PostgreSQL Flexible    Key Vault    Databricks / Fabric /     on-prem SQL Server,
          (Dagster metadata,     (only for    Snowflake / Azure SQL     on-prem PostgreSQL
          Entra-only auth)       non-Entra    / ADLS (logs)
                                 systems)
```

### 2.1 Hosting: Azure Container Apps (recommended) vs AKS

I checked the current Dagster release (`dagster-azure` / `dagster-postgres` 0.29.26):

- `dagster_azure.container_apps.AcaRunLauncher` runs **each Dagster run as its own ACA Job**, and
  accepts a **user-assigned managed identity per code location** (`container_context.aca.identity_id`).
  That is exactly what per-domain least privilege needs.
- `dagster_postgres` supports `auth_provider: azure_wif` — it injects a fresh Entra token as the
  password on every connection (scope `https://ossrdbms-aad.database.windows.net/.default`).
  So Dagster's own metadata DB can run with **password auth disabled**.
- `dagster_azure` compute log manager supports `DefaultAzureCredential` for Blob storage.

| | Container Apps (recommended) | AKS |
|---|---|---|
| Ops burden | Low (serverless, no cluster upgrades) | High |
| Entra sign-in for UI | Built-in Easy Auth | oauth2-proxy / App Gateway needed |
| Per-run isolation | ACA Job per run, own UAMI | Pod per run (K8sRunLauncher), own SA + workload identity |
| Maturity in Dagster | ACA launcher is new — needs a spike | Official Helm chart, battle-tested |
| On-prem execution | Needs network line of sight | Can add Arc-enabled cluster on-prem |

Recommendation: **ACA**, with an early spike (Phase 0) to validate the ACA launcher. If it falls
short, switch to AKS + official Helm chart; the code-location code is identical either way.

Alternative worth a look: **Dagster+ Hybrid** (agent runs in our Azure, control plane is SaaS).
It adds real RBAC and Entra SAML SSO out of the box; OSS has no RBAC (only the
read-only/full webserver split described above).

## 3. Identity and least-privilege model

### 3.1 Identities (per environment: dev / test / prod)

| Identity | Type | Gets access to | Explicitly NOT |
|---|---|---|---|
| `id-dagster-platform` | UAMI | Metadata Postgres (Dagster schema owner), ACA Jobs create/start in its own RG (custom role, not Contributor), ACR pull, log storage container | Any data platform |
| `id-dagster-webserver` | UAMI | Metadata Postgres (read/write as Dagster requires), ACR pull | Any data platform, ACA Jobs |
| `id-cl-<domain>` (one per code location, e.g. `id-cl-sales`) | UAMI | Only that domain's objects on each platform (see §4), its own Key Vault secrets, its own ADLS paths | Other domains, metadata DB DDL |
| `id-gh-deploy-<env>` | Entra app + GitHub OIDC federated credential | Push to ACR, update container apps in one RG | Data platforms, Key Vault secret values |

- Run workers inherit the code location's identity, so a run for `sales` cannot touch `finance`.
- All role assignments are scoped to the narrowest resource (container, secret, database,
  workspace) — never subscription-wide.
- Platform-side permissions are granted to **Entra groups** that contain the UAMI, so access
  reviews happen in one place.

### 3.2 Humans

- Container Apps Easy Auth with an Entra app registration; `Dagster-Viewers` group → read-only
  webserver, `Dagster-Operators` group → full webserver. Assignment required on the enterprise app.
- Conditional Access (MFA, compliant device) applies automatically.
- No personal credentials are ever used by runs; runs always use the workload identity.

### 3.3 Secrets

Key Vault (RBAC mode, private endpoint, purge protection) holds **only** credentials for systems
that cannot use Entra (see §4 on-prem). Each secret is granted individually
(`Key Vault Secrets User` on the secret, not the vault) to the one code-location identity that needs it.

## 4. Platform connectors

All connectors live in a shared library as Dagster `ConfigurableResource`s and get credentials
from a single `AzureCredentialResource` (wraps `ManagedIdentityCredential` in Azure,
`AzureCliCredential` locally).

| Platform | Dagster integration | Auth (no secrets) | Least-privilege grants |
|---|---|---|---|
| **Databricks (Azure)** | `dagster-databricks` (Jobs + Pipes) | UAMI added as Databricks service principal; SDK `azure` auth with the MI client id | Workspace user (not admin); `CAN_MANAGE_RUN` on specific jobs; Unity Catalog `USE`/`SELECT`/`MODIFY` on specific schemas; cluster policy restricted |
| **Microsoft Fabric** | No official integration → **custom resource** over Fabric REST API (run item job on demand, poll status) + ODBC to Warehouse/SQL endpoint | Token for `https://api.fabric.microsoft.com/.default` | Tenant setting "Service principals can use Fabric APIs" limited to a security group; workspace role Contributor only on needed workspaces (Viewer where read-only) |
| **Snowflake** | `dagster-snowflake` (+ `dagster-dbt` if used) | Snowflake **Workload Identity Federation** (`authenticator=WORKLOAD_IDENTITY`, provider AZURE) — fallback: External OAuth security integration with Entra | Dedicated service user + role per domain; usage on one warehouse; grants on specific DB/schemas only; no `ACCOUNTADMIN`/`SYSADMIN` |
| **Azure SQL** | Custom resource (pyodbc, ODBC Driver 18) | Entra access token via `SQL_COPT_SS_ACCESS_TOKEN`; server set to Entra-only auth | Contained user `FROM EXTERNAL PROVIDER`; custom DB roles / schema-level `SELECT`/`EXECUTE`; no `db_owner` |
| **On-prem SQL Server** | Same resource as Azure SQL | **SQL Server 2022 + Azure Arc** → Entra auth works like Azure SQL. Older versions → Kerberos (AD account synced via Entra Connect, keytab in Key Vault) | Same as Azure SQL |
| **On-prem PostgreSQL** | Custom resource (psycopg) | No native Entra before PG 18. PG 18 → OAuth via validator module (Entra). Otherwise Kerberos/GSSAPI if AD exists, else client cert or password **from Key Vault, fetched at runtime by the MI** | Dedicated role per domain, schema-level grants, `CONNECTION LIMIT`, `pg_hba` restricted to orchestrator subnet |
| ADLS Gen2 / Blob | `dagster-azure` | MI | `Storage Blob Data Contributor` on a specific container |
| ADF / Power BI (later) | Custom resources | MI / Entra token | Data Factory Contributor on one factory; Power BI workspace Contributor |

Honest caveat: "full Entra" for on-prem depends on the on-prem versions. The plan isolates any
non-Entra credential to Key Vault, a single identity, and a single code location, so it is
auditable and easy to remove once the source supports Entra.

### 4.1 Cross-platform asset model

- Asset keys prefixed by platform and domain: `["snowflake", "sales", "orders"]`,
  `["databricks", "sales", "orders_enriched"]`, `["fabric", "sales", "orders_report"]`.
- Lineage across platforms comes from asset dependencies (e.g. on-prem MSSQL extract →
  Databricks bronze → Snowflake mart → Fabric semantic model refresh).
- Assets owned by other tools are declared as external assets and observed via sensors.
- **Dagster Pipes** for Databricks so remote logs and metadata stream back into Dagster.
- Asset checks (row counts, freshness, schema) and freshness policies per asset.
- Declarative automation (`AutomationCondition`) instead of hard-coded cron where possible.

## 5. Repository layout

```
multi-platform-orchestrator/
├── pyproject.toml               # uv workspace root, ruff, pyright, pytest config
├── libs/
│   └── orchestrator_core/       # shared resources, auth, conventions, tests
│       └── src/orchestrator_core/
│           ├── auth.py          # AzureCredentialResource, token cache
│           ├── resources/       # databricks.py, fabric.py, snowflake.py, mssql.py, postgres.py, adls.py
│           ├── assets/          # factories (e.g. sql_asset, fabric_item_asset)
│           └── checks/
├── code_locations/
│   ├── example_sales/           # one Dagster code location = one domain = one identity = one image
│   └── ...
├── deploy/
│   ├── dagster.yaml             # instance config: Postgres w/ azure_wif, AcaRunLauncher, blob logs
│   ├── workspace.yaml
│   └── docker/                  # Dockerfiles (non-root, distroless/slim, pinned)
├── infra/                       # IaC (Bicep or Terraform — see §9)
│   ├── modules/                 # network, aca, postgres, keyvault, identities, acr, monitoring
│   └── envs/{dev,test,prod}
├── local/                       # docker-compose: Postgres, Azurite, dagster dev
├── .github/workflows/           # ci.yml (lint/type/test), build.yml, deploy.yml (OIDC)
└── docs/                        # PLAN.md, ARCHITECTURE.md, onboarding a new platform/domain, runbooks
```

## 6. Network and platform hardening

- ACA environment internal-only in a VNet; no public ingress. Users reach the UI through private
  network / VPN (or Front Door + Private Link if external access is required).
- Private endpoints + private DNS for Postgres, Key Vault, ACR, Storage, Databricks, Snowflake
  (PrivateLink), Azure SQL. Fabric via workspace private links / trusted access where available.
- Egress via Azure Firewall / NAT with an allowlist; on-prem via existing ExpressRoute/VPN.
- Container images: non-root, pinned dependencies, vulnerability scanning in CI, ACR with
  admin user disabled.
- Azure Policy: deny public network access, require Entra-only auth on SQL/Postgres.
- Logs: container stdout → Log Analytics; Dagster compute logs → Blob (MI); alerts on run
  failures via Dagster sensors → Teams/email.

## 7. Delivery phases

**Phase 0 — Decisions and spikes (before writing real code)**
- Answer §9.
- Spike A: `AcaRunLauncher` with per-code-location UAMI end to end (fallback: AKS).
- Spike B: Snowflake WIF from an ACA Job's managed identity.
- Spike C: Fabric REST "run on demand" with a service principal / MI in your tenant.
- Spike D: on-prem connectivity and auth options (versions, Arc, AD).

**Phase 1 — Skeleton and local dev**
- uv workspace, ruff, pyright, pytest, pre-commit; CI on GitHub Actions.
- `orchestrator_core` with `AzureCredentialResource` and an example code location.
- docker-compose: Postgres + Azurite + `dagster dev`. Tests run with mocked platforms.

**Phase 2 — Connectors** (one PR each, with unit tests + opt-in integration tests)
- Azure SQL / on-prem MSSQL → Snowflake → Databricks (Pipes) → Fabric → on-prem Postgres → ADLS.

**Phase 3 — Infrastructure as code**
- VNet, ACA env, Postgres Flexible (Entra-only), Key Vault, ACR, Storage, Log Analytics,
  UAMIs + role assignments, Easy Auth app registration, GitHub OIDC federated credentials.

**Phase 4 — Deployment**
- `dagster.yaml` (Postgres `azure_wif`, `AcaRunLauncher`, blob compute logs), images,
  deploy pipeline per environment with approvals for prod.

**Phase 5 — End-to-end example**
- A cross-platform asset graph (on-prem MSSQL → Databricks → Snowflake → Fabric) with checks,
  freshness, and alerts, deployed to dev.

**Phase 6 — Hardening and docs**
- Security review, access review of every grant, runbooks, "how to onboard a new domain /
  platform" guide.

## 8. Risks

| Risk | Mitigation |
|---|---|
| ACA run launcher is new | Spike A first; AKS fallback with same code |
| OSS Dagster has no RBAC | Read-only vs full webserver split behind Entra groups; or Dagster+ |
| On-prem systems may not support Entra | Isolate non-Entra creds in Key Vault per identity; plan upgrade path (SQL 2022+Arc, PG 18 OAuth) |
| Fabric API / SP limits change often | Thin custom resource, integration test against a real workspace |
| Token expiry in long runs | Always fetch tokens per connection via azure-identity cache, never once per run |

## 9. Open decisions (need your input)

1. **Hosting**: Azure Container Apps (recommended) or AKS? Or Dagster+ Hybrid?
2. **IaC**: Bicep or Terraform?
3. **On-prem**: SQL Server versions (2022? Arc-enabled?), PostgreSQL versions, is there AD /
   Entra Connect, and is there ExpressRoute/VPN to Azure?
4. **Fabric scope**: which item types (pipelines, notebooks, Spark jobs, semantic model refresh,
   Warehouse SQL)?
5. **Transformations**: is dbt in scope (on Snowflake / Databricks / Fabric)?
6. **Domains / environments**: how many code locations to start with, and dev/test/prod?
7. **UI access**: internal network only, or must it be reachable from the internet (behind Entra)?
