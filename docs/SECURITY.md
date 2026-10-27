# Security

For security reviews and vendor questionnaires. Every statement points at the code or test
that makes it true; the infra repo is `multi-platform-orchestrator-iac`. Items not yet
verified against Azure are listed in docs/PRO_FEATURES_PLAN.md §10 and the infra README.

## Architecture and data flows

- Dagster's webserver, daemon, code servers and run jobs run in one Azure Container Apps
  environment per environment (dev, test, prod) with an internal load balancer and public
  network access disabled (infra: `environment/platform.tf`, test
  `nothing_is_reachable_except_through_front_door`).
- The only way in is Front Door Premium over Private Link to the webserver, behind a WAF
  (Microsoft default rule set 2.1, bot manager 1.1, per-IP rate limit; Prevention in prod,
  Detection in dev/test while tuning) (infra: `environment/frontdoor.tf`, test
  `one_public_entry_point`).
- Users sign in with Entra ID through Container Apps' built-in sign-in; only groups assigned
  to the sign-in app's roles get in (infra: `environment/entra.tf`, test
  `roles_go_only_to_the_groups_named_for_them`).
- Dagster's metadata is in Azure Database for PostgreSQL, reachable only from the VNet; compute
  logs in a storage account that denies all networks but the Container Apps subnet.
- Data stays in Databricks, Fabric and Snowflake; Dagster sends them jobs and SQL, and reads
  cost metadata (mpo_ops), not business data.

## No secrets

- **CI/CD**: GitHub Actions and Azure DevOps log in to Azure with workload identity federation;
  pull-request CI gets no cloud login at all (`.github/workflows/ci.yml` and
  `change-report.yml` have no `id-token`).
- **Workloads**: every container uses a user-assigned managed identity; Snowflake trusts it
  through workload identity federation, Databricks, Fabric and Power BI through Entra tokens
  (`orchestrator_core/auth.py`, `resources/`).
- **Databases and storage**: Postgres accepts Entra logins only, storage keys are disabled,
  the registry's admin user is off (infra test `no_passwords_or_keys_anywhere`).
- **One exception, outside this system**: alert receivers whose URL holds a key (webhook,
  Logic App, Function, runbook) are read from the receiver owner's Key Vault at plan time;
  Entra-secured webhooks need none (infra: `environment/alerts.tf`).

## Least privilege (identity matrix)

| Identity | May | Where it is defined |
|---|---|---|
| `id-dagster-web-<env>` | Read and write Dagster's rows (role `dagster_rw`, no schema changes; like every Dagster process), read compute logs | `orchestrator_core/admin.py`, infra test `control_plane_rights_stay_narrow` |
| `id-dagster-platform-<env>` (daemon) | Create and read run Jobs in its resource group (custom role), attach domain identities to them | infra `identities.tf` |
| `id-dagster-admin-<env>` | Postgres Entra admin (migrations, logins), delete finished run Jobs | infra `platform.tf`, `identities.tf` |
| `id-cl-<domain>-<env>` | Exactly the grants in `domains/<env>/<domain>.yaml`; read-only grants never write | infra `modules/domain_access`, tests `read_access_never_writes`, `grants_only_what_the_file_lists` |
| `id-cl-mpo-ops-<env>` | Platform cost metadata only; send rows through one data collection rule | infra `insights.tf`, test `ops_rows_go_through_one_rule_only_mpo_ops_may_use` |
| `id-mpo-alerts-<env>` | Read the environment's Log Analytics workspace | infra test `alert_rules_read_only_logs_and_cannot_be_faked` |
| Deployers (infra, app) | Their own environment only; previews only in dev | infra `bootstrap/`, tests `each_repo_logs_in_only_from_its_own_environment`, `previews_log_in_to_dev_only` |

In the UI, roles are per code location: Viewer, Launcher, Editor, Admin
(`orchestrator_core/access.py`; 14 tests against Dagster's real GraphQL API in
`tests/test_access.py`, including that a malformed or missing principal gets read-only).

## Encryption

- In transit: HTTPS only from Front Door to the webserver (`forwarding_protocol = HttpsOnly`);
  Postgres and storage over TLS 1.2+ (`min_tls_version = TLS1_2`).
- At rest: Azure's platform encryption for Postgres, storage (with infrastructure encryption)
  and Log Analytics.

## Logging and audit

- Every UI action that changes something is one audit line with the signed-in user, the
  mutation and its variables (`orchestrator_core/audit.py`); runs launched from the UI carry
  `mpo/launched_by`.
- Alerts, health, cost and insight rows, and Container Apps system logs are in Log Analytics,
  kept `log_retention_days` (default 30; infra `variables.tf`).

## Evidence

- Solution CI (`deploy/scripts/ci.sh`): ruff, pyright, shellcheck, pytest (access, audit,
  alerts, health, cost, previews, render, catalog), `dagster definitions validate`.
- Infra CI (`scripts/ci.sh`): `terraform validate` and `terraform test` with mocked providers,
  every alert/workbook/Fabric query parsed with Microsoft's Kusto parser, Trivy (HIGH and
  CRITICAL fail the build).
- Pull requests: a change report of what they change in the Dagster definitions; previews only
  after a reviewer approves, in dev, under their own asset keys.

## Support

Dagster itself is supported by its community (Slack, GitHub issues); there is no vendor
support contract. docs/RUNBOOK.md and an on-call rotation replace a private support channel.
