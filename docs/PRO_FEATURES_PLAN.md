# Dagster+ Pro Features on Open-Source Dagster — Plan

Status: **Phases A–H implemented and tested locally (role-based access, audit, alerts, cost and insights, Fabric cost, catalog, change reports and previews, uptime and security pack, Purview). What needs Azure or the platforms to confirm is listed in §10.**
Builds on [PLAN.md](PLAN.md). Decisions in §11.

How every statement here was checked:

- **[S]** read in the installed source (Dagster 1.13.26, dagster-graphql, dagster-webserver,
  dagster-dbt / dagster-azure 0.29.26, dagster-powerbi 0.29.26)
- **[P]** proven against the real webserver and GraphQL API; the prototypes became the tests
  in `libs/orchestrator_core/tests` (test_access.py, test_alerts.py)
- **[D]** vendor documentation (links in §12)
- **[?]** not verifiable without Azure / the platforms → listed as a spike (§10)

dagster.io could not be opened from the research environment, so the Pro feature list
comes from search results quoting the pricing page (§12). Confirm it against
https://dagster.io/pricing before relying on it.

## 1. The gaps to close

| # | Pro feature | Open-source Dagster today | Plan |
|---|---|---|---|
| F1 | Role-based access control | Read-only or full access for everyone ([S]) | §2: roles per code location from Entra app roles |
| F2 | Audit logs | None | §3: who signed in, who launched what, every UI action |
| F3 | Alert policies | Sensors in code ([S]) | §4: alert sensors + Azure Monitor delivery, no secrets |
| F4 | Insights and cost tracking | None; `with_insights()` needs closed-source `dagster_cloud` ([S]) | §5: run metrics + Snowflake/Databricks/Fabric cost per asset |
| F5 | Catalog: column search and lineage, BI assets | Asset catalog with most filters; column/table/status filters and column-lineage graph are Dagster+-only ([S]) | §6: column metadata, Power BI assets, optional Purview |
| F6 | Branch deployments | None | §7: change report on every PR + approved preview locations |
| F7 | Uptime SLA | None | §8: health checks, alerts, SLOs, optional zone redundancy |
| F8 | Security questionnaires | None | §9: security pack from the code |
| F9 | Onboarding, private Slack, vendor support | — | Not buildable: services, not software (§9) |

Already covered by the current solution: unlimited code locations and deployments, SSO
(Entra), user provisioning (Entra assignment), EU data residency, Hybrid-style compute.

## 2. F1 — Role-based access control per domain

### 2.1 What was found

- The GraphQL server checks every UI action against permissions **per code location** [S]:
  launch, re-execute, terminate, delete run, schedules, sensors, backfills, dynamic partitions,
  wipe assets, reload location. Only reload-workspace, auto-materialize toggle and concurrency
  limits are global (`LOCATION_SCOPED_PERMISSIONS`, `dagster/_core/workspace/permissions.py`).
- Actions on **existing runs** (terminate, delete, re-execute) are checked against the run's
  own code location (`has_permission_for_run`, `dagster_graphql/implementation/utils.py`) [S].
- The webserver builds a request context per request from the HTTP connection:
  `WorkspaceProcessContext.create_request_context(source=conn)` [S]. The request context
  already supports per-location read-only (`read_only_locations`) [S], and its
  `permissions_for_location` can be overridden for finer roles [P].
- `dagster-webserver` is a thin CLI around `WorkspaceProcessContext` and
  `host_dagster_ui_with_workspace_process_context` [S], so a custom entry point can serve the
  same UI with our own process context.
- Container Apps puts the signed-in user's claims in `X-MS-CLIENT-PRINCIPAL` (Base64 JSON) and
  `X-MS-CLIENT-PRINCIPAL-NAME`; external requests cannot set these headers [D].
- Entra puts app roles in the ID token's `roles` claim, also when granted through a group;
  group-to-role assignment needs Entra ID P1 [D] (Conditional Access in PLAN.md already does).

Prototype result [P] (`rbac_proto.py`, `roles_proto.py`):

| Caller | Launch in `example_sales` | Edit sensor | Reload location | Reload workspace |
|---|---|---|---|---|
| no header / Viewer / other domain's role | refused | refused | refused | refused |
| `Dagster.Launcher.example_sales` | allowed | refused | refused | refused |
| `Dagster.Editor.example_sales` | allowed | allowed | allowed | refused |
| `Dagster.Admin` | allowed | allowed | allowed | allowed |

### 2.2 Design

- **Roles** (Dagster+'s model), as app roles on one Entra app `mpo-ui-<env>`:
  - `Dagster.Viewer` — read everything (the default for any assigned user)
  - `Dagster.Launcher.<location>` — launch, re-execute, terminate runs, launch/cancel backfills
  - `Dagster.Editor.<location>` — everything scoped to that location
  - `Dagster.Admin` — everything, including global settings
- Groups are assigned to roles in Terraform (`azuread_app_role_assignment`), generated from
  the domain files: each `domains/<env>/<name>.yaml` gains `launchers:` / `editors:` group IDs.
- `orchestrator_core.webserver` (new entry point, replaces `dagster-webserver` in the image):
  `EntraWorkspaceProcessContext` decodes `X-MS-CLIENT-PRINCIPAL`, accepts both the `roles`
  claim and the mapped `role_typ` URI, and returns a `RoleRequestContext`. Global read-only
  unless `Dagster.Admin`; **deny by default**: no header or an unparsable header means Viewer.
- **One webserver instead of two.** The read-only webserver and its second Entra app become
  unnecessary; Viewers use the same URL. Removes one container app, one Entra app, one Front
  Door endpoint/route.
- Schedules and sensors still run as the daemon; roles only govern people.
- Later option: owner-based permissions. The context also has `permissions_for_owner` [S], so
  roles like `Dagster.Editor.team:finance` could cover assets by `owners`.

### 2.3 Risks

- Uses Dagster internals (`dagster._core.workspace.*`). Mitigation: Dagster stays pinned; the
  prototypes become tests that run in CI against the real GraphQL API, so a Dependabot upgrade
  that breaks the hook fails before merge.
- The header is only trustworthy behind Container Apps auth [D]. The webserver must never get
  another ingress path: render.py tests already assert only the webserver is external; add a
  test that its auth config is enabled.

## 3. F2 — Audit logs

| What | How | Verified |
|---|---|---|
| Who signed in | Entra sign-in logs → Log Analytics (diagnostic setting) | [?] not researched here: licence and retention in your tenant |
| Who launched a run | `AuditedRunCoordinator` (subclass of `QueuedRunCoordinator`) reads `X-MS-CLIENT-PRINCIPAL-NAME` via `SubmitRunContext.get_request_header` and tags the run `mpo/launched_by` — visible and filterable in the run list | [S] [P] |
| Every UI action (terminate, wipe, sensor edits, backfills, ...) | ASGI middleware on our webserver: logs each GraphQL **mutation** (user, object ID, mutation names, variables, result type) as one JSON line to stdout → `ContainerAppConsoleLogs_CL` | [P] middleware; [D] table |
| Infra and deployment changes | Azure Activity Log + the Git history of both repos | [?] Activity Log retention not researched here |
| Long-term, tamper-evident copy | Optional: Log Analytics data export to a storage container with an immutability policy | [?] spike, cost |

Notes:
- Backfills launched from the UI create their runs later in the daemon, so the user is only
  in the mutation log, not on those runs [S].
- Variables are logged as sent; none of ours carry secrets (there are none).

## 4. F3 — Alert policies, without secrets

### 4.1 Detection (Dagster side)

A new internal code location **`mpo_ops`** (own identity, no business-data access) holds:

| Alert | Mechanism | Verified |
|---|---|---|
| Run failure (any domain) | `run_failure_sensor(monitor_all_code_locations=True)` | [S] |
| Asset check failed / warned | Cursor-based sensor over `ASSET_CHECK_EVALUATION` events (`instance.event_log_storage.get_event_records(EventRecordsFilter(..., after_cursor=...))`; the instance method is deprecated) | [S] |
| Freshness violated | Same, `FRESHNESS_STATE_CHANGE` events, written by the open-source freshness daemon (`dagster/_daemon/freshness.py`); policies via `FreshnessPolicy` / `build_last_update_freshness_checks` | [S] |

Not available: "asset health changed". The event type exists, but nothing in open-source
Dagster emits it [S]; check-failure and freshness alerts cover the same ground.

Each sensor writes one JSON line per alert to stdout: `{"mpo_alert": kind, "time",
"severity", "location", "job" | "asset" + "check" | "instigator" | "daemon", "run_id",
"launched_by", "summary", "url"}`. `location` is the code location (= domain). Asset
owners are not added: the sensors run in `mpo_ops` and cannot read other code locations'
definitions, so routing stays by code location; owners show in the catalog.

Failures a sensor cannot see (the daemon itself down, schedule/sensor tick failures, code
locations that fail to load) are checked by a **health job**: a scheduled Container Apps Job
every 5 minutes (same image, admin identity) that reads `get_daemon_statuses()`, failed
ticks (`all_instigator_state()` + `get_ticks(statuses=[FAILURE])`) and code-location
`load_error`s, and writes the same JSON lines [S].

### 4.2 Delivery (Azure side, in the infra repo)

- **Log search alert rules** over `ContainerAppConsoleLogs_CL` (`Log_s` holds the line; Jobs
  log there too, with `ContainerJobName_s`) [D], one per route and severity, generated by
  Terraform from `alerts/<env>.yaml` (which kinds, which code locations, which severities →
  which action groups). They split by kind, code location and job/asset, not by run, as
  Microsoft advises against volatile dimensions [D].
- Rules query as a dedicated identity with Log Analytics Reader on the workspace; without one a
  rule runs with the rights of whoever last edited it [D]. Query validation is skipped because
  the table only exists after the first log line [D]; CI parses every query with Microsoft's
  Kusto parser instead.
- **Action groups** with every receiver type: email, SMS, voice, push, Azure role, Entra-secured
  webhook, Event Hub, ITSM, webhook, Logic App (e.g. Teams), Function, runbook [D]. The last
  four need a URL that holds a key (Azure Monitor's API has no other way) [D]; it lives only in
  its owner's Key Vault and Terraform reads it with Entra at plan time, so it is never in git or
  a pipeline variable. Action-group managed identities would remove that, but are in preview and
  not in the azurerm provider [D].
- An "absence" rule (no `health_check` line from the `dagster-health` job for 15 minutes)
  catches the health job, or the environment, failing. Only that job's lines count, so a code
  location cannot keep it quiet by printing the line.

Alerts are configuration-as-code (pull request), not a settings screen like Dagster+.

## 5. F4 — Insights and cost tracking

### 5.1 Operational insights (Dagster's own data)

A daily `insights` job in `mpo_ops` reads the Dagster instance [S] (`get_run_records`, event
records) and computes per asset and per job: runs, failures, duration p50/p95, retries,
materializations, check pass rate, freshness misses.

### 5.2 Cost per asset

| Platform | Source | Join key | Verified |
|---|---|---|---|
| Snowflake | `SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY.CREDITS_ATTRIBUTED_COMPUTE` (+ query acceleration credits) | `QUERY_TAG` = `{"mpo_run","mpo_step"}`, set by `SnowflakeResource` (session parameter) and by dbt (profile `query_tag` from `MPO_QUERY_TAG`); a shipped dbt macro adds `mpo_dbt_node` per model | [D]: readable with database role `USAGE_VIEWER`; up to 8 h latency; queries ≤ ~100 ms and idle time not attributed |
| Databricks | `system.billing.usage` ⋈ `system.billing.list_prices` (`pricing.effective_list.default`, the price Databricks documents for cost) | `usage_metadata.job_run_id` — stored on materializations as `databricks_run_id` | [D]: only job and serverless compute; list prices; for classic compute the Azure VM cost is separate |
| Fabric | Capacity operation events (`Microsoft.Fabric.CapacityOperationEvents.Operation`) via Real-Time hub → Eventstream → Eventhouse, read through the KQL function `MpoCapacityOperations` | `operationId` = the job instance / refresh ID a materialization recorded; else `itemId` + the run's time window | [D] fields `workspaceId`, `itemId`, `operationId` ("unique run ID for the operation"), `capacityUnitMs`, `operationStartTime`, `releaseType` (Preview is unbilled); [?] whether `operationId` equals our job instance ID (S2) |

### 5.3 Where it shows

- **In the Dagster UI**: the job emits `AssetObservation`s with numeric metadata (`cost_usd`,
  `credits`, `duration_s`, ...). The open-source asset page charts numeric metadata of
  materializations **and observations** ("Plots") [S: UI bundle text "Include numeric
  metadata entries in your materializations and observations to see data graph"].
- **Across assets**: the same rows go to a Log Analytics custom table through the Logs
  Ingestion API (`azure-monitor-ingestion`, `DefaultAzureCredential`, role *Monitoring
  Metrics Publisher* on the data collection rule) [D], shown in an Azure Monitor Workbook
  (in Terraform) or Power BI.

### 5.4 Access for `mpo_ops`

Read-only, platform-metadata only, no business data:
- Snowflake: database role `SNOWFLAKE.USAGE_VIEWER` [D] (one-time `snowflake/cost_access.sql`), and `USAGE` on one warehouse
- Databricks: `USE CATALOG system`, `USE SCHEMA`/`SELECT` on `system.billing` (one-time, by a metastore admin), `CAN_USE` on the cost warehouse
- Fabric: Viewer on the monitoring Eventhouse
- Azure: *Monitoring Metrics Publisher* on the data collection rule
- Postgres: `dagster_rw` like every Dagster process (reads run history)

## 6. F5 — Catalog

### 6.1 What open source already does [S]

Catalog filters in the UI bundle: key, kind, code location, group, owner, tag, partitions,
automation, sensor, schedule, job, flag. Marked Dagster+-only: column, column tag, table
name, status, changed-in-branch. Column lineage (`dagster/column_lineage`) and schema
(`dagster/column_schema`) metadata are stored and displayed, but lineage only as JSON, not as
a graph.

### 6.2 Plan

1. **Make the free filters useful**: components set `owners` (from domain file), `kinds`
   (already), `tags` (domain, layer, PII flag) and `group_name` consistently.
2. **Column schema and lineage metadata**: dbt via the component's column metadata option
   (`fetch_column_metadata(with_column_lineage=True)`, uses sqlglot) [S]; other components get
   an optional `columns:` block.
3. **BI assets**: `dagster-powerbi` (open source) [S] with a small `EntraPowerBICredentials`
   subclass of its abstract `PowerBICredentials` (`api_token` → our managed-identity token), so
   no client secret [S]. Use its default (non-scan) mode; scan mode needs Power BI admin APIs
   [S]. Semantic models and reports join the lineage graph.
4. **Optional, for column-level search and lineage graphs across platforms**: Microsoft
   Purview. Native scanners for Snowflake, Databricks and Fabric; Dagster lineage pushed through
   the Purview REST API, including column mappings [D]. Cost reported by third parties as
   ≈ $0.50 per governed asset per month on pay-as-you-go [D, third-party — verify]. Decide
   after F1–F4.

Rejected: Marquez/OpenLineage for column lineage. Column lineage is confirmed in Marquez's
API, but not in its UI [D].

## 7. F6 — Branch deployments

Two parts, keeping the rule that pull-request code never gets cloud credentials:

1. **Change report on every pull request (no credentials)**: CI loads the definitions of
   `main` and of the pull request and posts a comment listing added/removed/changed assets,
   dependencies, checks, partitions, automation and code versions. This covers Dagster+'s
   "changed in branch" view.
2. **Preview on approval**: a reviewer approves the GitHub (or ADO) environment `preview`. Only
   then a job deploys the pull request's code location into **dev** as `<domain>-pr<N>`, with
   automation off, and removes it when the pull request closes.
   - The workspace file moves from the image to a Container Apps secret-volume mount, so
     adding a location needs no image rebuild [D: secrets mount as files].
   - Runs use the domain's dev identity and dev data, like any dev run.
   - Federated credential with subject `environment:preview` on the dev app deployer.

Difference from Dagster+: previews share dev's metadata database and UI rather than getting a
separate deployment each; a separate one per pull request would add a database, daemon and
webserver per preview.

## 8. F7 — Uptime

| Item | How | Cost |
|---|---|---|
| Liveness | Container Apps probes: HTTP `/server_info` on the webserver (endpoint exists [S]), TCP on the gRPC code servers | free |
| Restarts, failed revisions | Azure Monitor metric alerts | ~free |
| Daemon down, tick failures, location errors | Health job (§4.1) + absence alert | ≈ $3/month (8,640 runs × ~20 s at 0.5 vCPU) |
| SLOs | `docs/SLO.md`: UI availability, schedule latency, run-start latency, with the queries | — |
| Restore | Runbook + quarterly test of Postgres point-in-time restore | — |
| Zone redundancy (optional) | Container Apps environment zone-redundant (needs ≥ 2 replicas of webserver and daemon); Postgres zone-redundant HA doubles its cost | [?] prices to confirm |

No external party guarantees this; it is your SLA to run.

## 9. F8 / F9 — Security pack and support

- **F8**: `docs/SECURITY.md` built from the code: architecture and data flows, identity matrix
  (from Terraform), no-secrets statement, network exposure, encryption, logging and
  retention, test evidence (Terraform tests, render tests, RBAC tests), Trivy results.
  Answers most vendor questionnaires.
- **F9**: Dagster's community Slack and GitHub issues remain the only Dagster support;
  internal runbooks and an on-call rotation replace a private channel.

## 10. Spikes (need Azure or the platforms)

| # | Question | Pass criterion |
|---|---|---|
| S1 | Container Apps passes `roles` in `X-MS-CLIENT-PRINCIPAL` for group-assigned app roles | Header decoded in a test app shows the role values |
| S2 | Fabric `operationId` equals the job instance ID returned by Run On Demand Item Job | Same ID in both; else use item ID + time window |
| S3 | Snowflake `QUERY_TAG` set by the resource and by dbt appears in `QUERY_ATTRIBUTION_HISTORY` | Tagged rows with credits for a test run |
| S4 | Log search alert → action group → email and Teams (Logic App) | Alert delivered within 10 min of a failed run |
| S5 | Workspace file from a secret volume, reloaded without image rebuild | Preview location appears in the UI |
| S6 | Purview custom lineage with column mappings renders | Column edges visible |
| S7 | Event Hub receiver without a managed identity: does the hub need a role? | Alert arrives on the hub; note any role needed |
| S8 | May the infra role (`MANAGE GRANTS`) grant `SNOWFLAKE.USAGE_VIEWER`? | Grant succeeds from Terraform; then `cost_access.sql` moves into Terraform |
| S9 | The dbt query-tag macro on dbt-snowflake | Model queries show `mpo_dbt_node` in `QUERY_HISTORY` |
| S10 | mpo_ops's managed identity queries the Eventhouse with workspace Viewer; the eventstream table matches `capacity_operations.kql`; note what `identityValue` holds for a managed identity | The cost job reads operations; then the item + time fallback can also require the domain's identity, so manual runs of the same item are not charged to the asset |
| S11 | Purview: which role may create the two type definitions, and Data Curator on a collection lets mpo_ops upsert entities | `typedefs` by an admin and a `sync` run succeed |

## 11. Delivery

| Phase | Content | Depends on |
|---|---|---|
| A | F1 RBAC + one webserver; F2 audit (coordinator tag, mutation log); RBAC/audit tests in CI | S1 |
| B | `mpo_ops` code location + identity; F3 alerts (sensors, health job, Terraform alert rules) | S4 |
| C | F4 insights: Dagster metrics, Snowflake + Databricks cost, UI plots, Workbook | S3 |
| D | F4 Fabric cost (Eventstream/Eventhouse) | S2 |
| E | F5 catalog: owners/tags, column metadata, Power BI assets | — |
| F | F6 change report, then previews | S5 |
| G | F7 uptime items, F8 security pack | A–C |
| H | Optional: Purview (F5.4), zone redundancy (F7) | decisions |

**Cost delta (rough):** −1 container app and endpoint (single webserver) ≈ −$12/month in prod;
health job ≈ $3; Log Analytics ingestion for audit/alerts/insights a few GB ≈ $5–15; Fabric
Eventhouse uses Fabric capacity units (on your existing capacity); Purview and zone
redundancy only if chosen.

**Decisions (2026-10-25):**
1. Entra ID P1 at least, usually P2: group-to-role assignment is available.
2. Alert channels: all action-group receiver types are supported in `alerts.yaml` (email,
   SMS, voice, push, Teams via Logic App, webhook with Entra auth, ITSM, Event Hubs).
3. Fabric cost tracking: yes. The Eventstream and Eventhouse are **not** created by Terraform;
   a capacity admin creates them, and their query URI and database are configuration inputs.
4. Purview: yes (phase H). Uses the tenant's existing Purview account (Microsoft provides one
   per tenant); its endpoint is an input, nothing is created by Terraform.
5. Previews running approved pull-request code against dev data: yes.

**Phase A, as built:**
- `orchestrator_core.access` (roles → per-location permissions), `orchestrator_core.audit`
  (mutation log middleware), `orchestrator_core.run_coordinator` (`mpo/launched_by` tag; a
  separate module because every Dagster process loads it and only the webserver has the
  GraphQL packages), `orchestrator_core.webserver` (entry point).
- 13 tests against the real GraphQL API with two code locations, plus a run of the real entry
  point over HTTP. A mutation check (Launchers given every right) makes two of them fail.
- One webserver instead of two; contract `schema_version` 2 (no read-only app or hostname).
- A new webserver starts with internal-only ingress until Entra sign-in is on, so it is never
  reachable from Front Door without sign-in.
- Found while testing: Dagster checks an asset launch against the code location that defines
  the asset, so **each table must have exactly one writing domain**; other domains list it
  under `reads:`.

**Phase B, as built:**
- Code location `mpo_ops` with `orchestrator_core.alerts` (run failure, failed checks,
  freshness; a sensor starts at the newest event instead of replaying history) and the
  `dagster-health` job (`orchestrator_core.health`: unhealthy daemons, failed ticks of the last
  5½ minutes, code locations that fail to load, and a `health_check` line every run).
- 6 tests on a real instance (non-run-sharded event log, like Postgres), no mocks.
- Infra: `alerts/<env>.yaml`, `environment/alerts.tf`, identity `mpo_ops` without platform
  grants; 11 environment and 6 module plan tests, including every receiver type, refused
  mistakes, and a removed-check mutation that a test catches.

**Phase C, as built:**
- `orchestrator_core.query_tag`, the Snowflake session tag in `SnowflakeResource` and
  `SnowflakeSql`, `MPO_QUERY_TAG` for dbt and `dbt_macros/mpo_query_tag.sql` (overrides
  dbt-snowflake's dispatched `snowflake__set_query_tag`/`unset_query_tag`; read from its
  source).
- `orchestrator_core.cost` (`mpo_cost`, daily, for the day before yesterday because Snowflake
  attribution lags up to 8 h): finished runs → materializations → cost. A dbt node tag goes to
  its model; a step tag is split over that step's assets; cost no asset can carry (failed
  steps) is kept per run as unattributed. Observations carry `cost_usd`, `snowflake_credits`,
  `databricks_dbus`, ...; `cost_usd` only when every part can be priced in USD.
- `orchestrator_core.insights` (`mpo_insights`, nightly): per job runs, outcomes, p50/p95
  duration; per asset materializations, check results, freshness failures.
- `orchestrator_core.log_ingestion`: Logs Ingestion API with the managed identity; nothing is
  sent without the contract's `insights` settings.
- Infra: tables from one `tables.json`, a `Direct` data collection rule (own ingestion
  endpoint, no DCE [D]), *Monitoring Metrics Publisher* for mpo_ops on that rule only, the
  workbook, the cost warehouse. Row timestamps are the upload time and the reported day is a
  column, so no ingestion-age limit applies.
- Found while testing: `platform_resources()` cannot be used outside Dagster's resource
  initialisation (its `EnvVar` fields would be the variable names), so the cost job builds its
  clients from the values.
- 16 new tests (attribution rules, the real example components' tags end to end, Databricks
  paging and failure, schedules, insights), a mutation check, and the workbook queries parsed
  against the table definitions.

**Phase D, as built:**
- `cost.fetch_fabric_operations` reads the Eventhouse with `azure-kusto-data` and the
  managed identity (the client asks the Eventhouse for its token audience); workspace IDs are
  checked to be GUIDs before they go into the query. CU-hours × the capacity's price per CU
  hour give USD; Preview operations are counted but not priced.
- `FabricItemJob` and `FabricSemanticModelRefresh` record workspace and item IDs next to the
  job instance / refresh ID.
- Infra: the Eventhouse is an input (`fabric_capacity_events`), not a resource;
  `fabric/capacity_operations.kql` is the one function to create; mpo_ops gets Viewer on that
  workspace through its domain file. CI parses the function against the documented event
  shape and the report's query against the function.
- Found while testing: the Kusto parser's schema-string form silently drops every column after
  a keyword name (`time`), which made a correct query look wrong; the CI check now builds
  tables column by column.
- 6 more tests (operation ID match, item + time fallback, Preview, the Kusto call, GUID check,
  the example's Fabric component end to end).

**Phase E, as built:**
- Platform components take `owners`, `tags` and `columns` (type, description, column tags,
  `derived_from: ["<asset key>:<column>"]`); they become `dagster/column_schema` and
  `dagster/column_lineage` on the definition. Unknown assets or upstreams are refused at load.
- dbt: `include_metadata: [column_metadata]` (dagster-dbt's own option). A real dbt build in the
  tests (DuckDB) shows materializations, column schema and column lineage all under the
  canonical keys, so `CanonicalDbtProject`'s keys reach dagster-dbt's event handling too.
- `orchestrator_core.powerbi.PowerBIReports` (extra `powerbi`): dagster-powerbi with
  `EntraPowerBICredentials` (managed-identity token for Power BI, no secret) and the
  workspace APIs (no admin scan). Reports `fabric/<ws>/report/<name>` depend on the canonical
  semantic model key; dashboards on their tiles' reports. Semantic models are only added on
  request, because the refresh component defines them.

**Phase F, as built:**
- `deploy/change_report.py` + `change-report.sh`: the base branch is snapshotted in its own
  checkout and environment, the pull request in its own; the diff covers assets, deps, owners,
  tags, columns, checks, partitions, automation, schedules, sensors and locations that fail to
  load. GitHub: one sticky comment (`pull-requests: write`, no `id-token`) and the job summary;
  fork pull requests get the summary only. ADO: the summary tab.
- `orchestrator_core.preview`: a preview defines only assets and checks, keyed
  `preview/pr<N>/<key>` (upstreams of other domains keep their real keys; found while
  testing: `resolve_all_asset_keys()` includes those upstream stubs and must not be used to
  pick what to rename), group `preview_pr<N>`, no automation conditions, schedules, sensors or
  jobs. Its location `<domain>_pr<N>` has the domain's roles.
- The workspace file is generated by `render.py` and mounted from a Container Apps secret into
  the webserver, daemon and health job [D: secret volumes]; a hash variable rolls a new
  revision on every change, because a changed secret alone does not [D].
- `preview.sh deploy|remove` changes only the preview's code server and the workspace readers;
  `deploy.sh` keeps running previews (found by the `mpo-preview` tag, with their images).
  GitHub: label `preview` + approval of environment `preview`; hourly `preview-cleanup` from
  main removes previews of closed pull requests. ADO: a manual pipeline behind environment
  `mpo-preview`.
- Found while reviewing: ADO's deploy pipeline named its service connections `mpo-<env>`
  while the federated credentials trust `mpo-app-<env>`; now `mpo-app-<env>`.
- Approving a preview runs that pull request's code, workflow files included, with dev's
  app-deployer rights; reviewers must read the diff first. Previews write to dev's tables like
  the dev domain does.

**Phase G, as built:**
- Probes on every server (render.py): startup allowance of 5 minutes, then liveness and
  readiness; HTTP `/server_info` on the webserver (version info only, no database), TCP on the
  gRPC code servers [D: probe fields; the automatic defaults are documented for the portal
  only, so they are set explicitly].
- Infra: one stateful alert per app on Container Apps errors (`ContainerAppSystemLogs_CL`,
  `LogLevel_s == "Error"`): failed revisions, crash loops, probes. Not a `RestartCount` metric
  alert, because multi-resource metric alerts for Container Apps are not documented [?].
- docs/SLO.md (SLIs and Kusto queries, parsed), docs/RUNBOOK.md (alerts, point-in-time restore
  copied back into the Terraform-managed server, roll back), docs/SECURITY.md (each statement
  tied to code or a test).
- Zone redundancy stays optional (phase H decision): it needs two replicas of webserver and
  daemon and zone-redundant Postgres HA, which doubles Postgres cost.

**Phase H, as built:**
- `orchestrator_core.purview`: the Data Map REST API (Atlas v2, api-version 2023-09-01,
  scope `https://purview.azure.net/.default`, base `<endpoint>/datamap/api` [S: the official
  `azure-purview-datamap` package's source]). Custom types `dagster_asset` (a `DataSet`) and
  `dagster_process` (a `Process` with `columnMapping`); an admin creates them once
  (`typedefs`), the daily job only upserts entities. Assets first, then processes that refer
  to them by qualified name; `columnMapping` in the documented DatasetMapping/ColumnMapping
  form [D; that Purview renders it for custom types is S6].
- The `dagster-catalog` Container Apps job (render.py, only when the contract has `catalog`)
  runs as mpo_ops with the workspace file mounted, so it reads every code location's
  definitions through their code servers.
- Tests run the real example domain through a real workspace and check the payloads against a
  mocked Purview.
- Not done: linking `dagster://` assets to the entities Purview's own scanners create for the
  same Snowflake, Databricks and Fabric tables (their qualified-name formats are not verified
  here); zone redundancy (cost decision).

## 12. Sources

- Pricing (third-party summaries of dagster.io/pricing): https://panoply.io/tools/dagster/pricing/ ·
  https://datapipelinetool.com/vendor/dagster · https://markaicode.com/pricing/dagster-pricing/
- Container Apps authentication and headers: https://learn.microsoft.com/en-us/azure/container-apps/authentication ·
  https://learn.microsoft.com/en-us/azure/app-service/configure-authentication-user-identities
- Entra app roles in tokens, group assignment licensing: https://learn.microsoft.com/en-us/entra/identity-platform/howto-add-app-roles-in-apps ·
  https://learn.microsoft.com/en-us/entra/identity/enterprise-apps/assign-user-or-group-access-portal ·
  https://learn.microsoft.com/en-us/entra/architecture/customize-tokens
- Container Apps logs table: https://learn.microsoft.com/en-us/azure/container-apps/log-monitoring
- Action groups: https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/action-groups
- Logs Ingestion API (Python): https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-ingestion-readme ·
  https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview
- Snowflake query attribution: https://docs.snowflake.com/en/sql-reference/account-usage/query_attribution_history ·
  https://docs.snowflake.com/en/user-guide/cost-attributing · https://docs.snowflake.com/en/sql-reference/snowflake-db-roles
- Databricks job cost: https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost ·
  https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing
- Fabric capacity events: https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-capacity-operation-events
- Container Apps secret volumes: https://learn.microsoft.com/en-us/azure/container-apps/manage-secrets
- Purview custom lineage and pricing: https://learn.microsoft.com/en-us/purview/data-gov-api-create-lineage-relationships ·
  https://datatrail.ai/blog/microsoft-purview-pricing (third-party)
- Marquez column lineage: https://marquezproject.ai/blog/column-lineage-demo
