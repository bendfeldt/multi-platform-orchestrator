# multi-platform-orchestrator-infra

Terraform for the [multi-platform orchestrator](https://github.com/bendfeldt/multi-platform-orchestrator):
Azure infrastructure, identities and platform grants. The solution repo deploys onto it and
reads only the environment contract this repo publishes (no shared code, no shared state).

Entra ID everywhere, no secrets: every pipeline and workload logs in with a federated
credential or a managed identity.

## Layout

| Path | What | Run by |
|---|---|---|
| `bootstrap/` | Shared state and contract storage, the shared Front Door Premium profile, per environment: resource group, image policy, and the two federated deployers | A person, once |
| `environment/` | One environment (dev, test or prod): network, Container Apps environment, Postgres, ACR, logs, identities, sign-in apps, Front Door endpoints and WAF, image policy, domains, contract | The pipelines |
| `modules/domain_access/` | One domain: its managed identity and its grants on Databricks, Fabric and Snowflake | via `environment/` |
| `domains/<env>/<name>.yaml` | What each domain may read and write. File name = the code location's name | People, by pull request |
| `snowflake/bootstrap.sql` | The infra pipeline's Snowflake login (workload identity) | A Snowflake admin, once per account |
| `scripts/` | `ci.sh`, `apply.sh`, `install-tools.sh`, called by both CI systems | |

## Set up (once)

1. **Bootstrap** (Owner on the subscriptions; Entra rights to create apps and grant the Graph
   app role `Application.ReadWrite.OwnedBy`):
   ```bash
   cd bootstrap
   cp bootstrap.tfvars.example bootstrap.tfvars   # tenant, subscriptions, GitHub and/or ADO
   terraform init && terraform apply -var-file=bootstrap.tfvars
   # then switch versions.tf to `backend "azurerm"` and:
   terraform init -migrate-state -backend-config=backend.hcl
   ```
   One subscription per environment or one shared subscription both work: give the same ID
   three times. `location` is optional (default `westeurope`).
2. **Platforms**, once per environment, by their admins:
   - Snowflake: run `snowflake/bootstrap.sql` with the infra deployer's object ID.
   - Databricks: add the infra deployer's service principal to the workspace as admin; catalog
     owners give it `MANAGE` on the catalogs domains use. Job owners give each domain's service
     principal `CAN_MANAGE_RUN` on its jobs (Terraform doesn't, because
     `databricks_permissions` would replace each job's whole permission list).
   - Fabric: allow service principals to use Fabric APIs for a security group containing the
     deployers and domain identities; make the infra deployer Admin of the domains' workspaces.
3. **Environment values**: copy the bootstrap outputs into `environment/envs/<env>.tfvars`
   (and the dev file's `acr_pull_principal_ids` with the test and prod app deployers).
4. **CI**: environments `dev`, `test`, `prod` (GitHub) or `mpo-infra-<env>` (ADO) with approvals
   on test and prod; GitHub variables `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
   `AZURE_SUBSCRIPTION_ID`; ADO service connections `mpo-infra-<env>` with workload identity
   federation.
5. The solution repo needs each environment's contract URL (`bootstrap` output
   `shared.contract_urls`) and app deployer client ID.

## Day to day

- Pull requests run `scripts/ci.sh`: format, validate, `terraform test` (mocked providers,
  including the grant tests in `modules/domain_access/tests`) and a Trivy scan. No cloud login.
- Merging to `main` applies dev, then test and prod after approval.
- **Add a domain**: add `domains/<env>/<name>.yaml` for each environment. Terraform creates
  its identity and exactly the listed grants, adds it to the contract, and creates the
  domain's Dagster roles (`Dagster.Launcher.<name>`, `Dagster.Editor.<name>`) for the groups
  in `launchers:` / `editors:`.
- **Who may do what in Dagster**: groups in `viewer_group_ids` / `admin_group_ids`
  (`envs/<env>.tfvars`) and the domain files. Group-to-role assignment needs Entra ID P1 or
  P2.

## Not yet verified against Azure (Phase 0 spikes)

- Front Door Private Link approval on the Container Apps environment (`frontdoor.tf`).
- The Azure Policy aliases for Container Apps images (`bootstrap/environment/main.tf`).
- The Snowflake provider's workload identity login from a CI runner.
- Container Apps sign-in without a client secret (implicit ID tokens in `entra.tf`).
