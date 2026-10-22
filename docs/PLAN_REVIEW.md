# Plan Review and Audit (ponytail-review + ponytail-audit)

**Status: all 16 findings applied in PLAN.md v7** (marked *R1*–*R16* there).

Reviewed: `docs/PLAN.md` v6 and `docs/COSTS.md`, including the latest change (shared Front Door
Premium). Method: the [ponytail](https://github.com/DietrichGebert/ponytail) `ponytail-review` and
`ponytail-audit` skills, applied to a plan instead of code. Claims about Dagster were checked
against the `dagster-azure` / `dagster-postgres` 0.29.26 source.

**What this repo does:** It holds the plan for one Dagster deployment on Azure Container Apps
that runs and tracks data work on Databricks, Fabric, Snowflake and Azure SQL. All logins use
Entra ID. Each data domain gets its own identity, and there are three environments plus one shared
Front Door. No code exists yet.

**Load assumed:** a small data team; up to ~200 runs/day in prod, with occasional backfills of
hundreds of partitions; 2–10 people using the UI.

## Must fix

1. **Runs cannot save their results** (PLAN §5.1, line 201)
   - **What this is:** Each run is a separate container that uses its domain's identity, e.g. `id-cl-sales`.
   - **Problem:** A run writes its events and status to Dagster's Postgres database. The plan only gives database access to the platform and webserver identities. With `azure_wif` login, the run logs in as the domain identity, so the database rejects it and every run fails at start.
   - **Fix:** Create one Postgres role, `dagster_rw`, that can read and write data but cannot change tables. Grant it to every domain identity. Only the platform identity can change tables.
   - **If we skip it:** No run works in Azure.

2. **Sensors and schedules run without the domain's identity** (PLAN §2 diagram line 54, §5.1)
   - **What this is:** A code-location server is the always-on container that loads a domain's code. Dagster runs sensors (small checks such as "has the Fabric job finished?") inside it, not inside runs.
   - **Problem:** The plan gives the domain identity only to run containers. A sensor that calls Fabric or Databricks gets no token. It also needs database access through `context.instance`.
   - **Fix:** Attach the domain identity to that domain's code-location server too, with the same `dagster_rw` role as in finding 1.
   - **If we skip it:** Every sensor that touches a platform fails, including the lineage and observation sensors the plan relies on.

3. **Finished runs leave a job behind forever** (PLAN §2; `dagster_azure/container_apps/launcher.py`)
   - **What this is:** The Container Apps launcher creates one Container Apps "Job" resource per run.
   - **Problem:** Its own docs say: "Completed Jobs are left in place, so the resource group holds one Job per finished run." At 200 runs/day that is ~6,000 resources a month in one resource group. It will reach Azure's per-environment and per-resource-group limits within weeks, and listing them slows down.
   - **Fix:** Add one small daily Dagster schedule, run by the platform identity, that deletes Jobs of runs that finished more than 7 days ago. Logs stay in Blob storage and Postgres.
   - **If we skip it:** New runs fail to launch once the limit is hit.

4. **The control plane can act as any domain** (PLAN §1 line 16, §5.1 line 199)
   - **What this is:** The daemon (the scheduler process) uses the platform identity to create run Jobs, and attaches a domain identity to each one.
   - **Problem:** To attach an identity, the platform identity needs the "assign" right on every domain identity. Anyone who controls the daemon can start a Job with any image and the `finance` identity, and read finance data. So the claim "the control plane cannot read business data" is false as written.
   - **Fix:** Say it honestly in the plan. Limit the damage with an Azure Policy that only allows images from our own registry, and make the app deployer the only identity that can push to that registry. Run the daemon on its own identity with no other rights.
   - **If we skip it:** A security review will find the gap, and an attacker in the daemon gets every domain's data.

5. **Pull-request CI gets live dev credentials** (PLAN §9 line 378, §4.2 line 171)
   - **What this is:** The plan runs a "lineage drift vs dev" check on every pull request.
   - **Problem:** That check must log in to dev Databricks, Snowflake and Fabric. So any branch in a pull request runs its own code with a dev data identity. A changed test can print or send the token.
   - **Fix:** Pull-request CI uses no cloud login at all. Run the drift check after merge in the `dev` environment, and as a scheduled Dagster check.
   - **If we skip it:** Anyone who can open a pull request can read dev data.

6. **Nothing can reach the private database to set it up or upgrade it** (PLAN §6.1, §9)
   - **What this is:** Postgres is only reachable inside the virtual network (VNet). Two jobs must still run against it: create the Entra database users (`pgaadauth_create_principal`) and run `dagster instance migrate` after each Dagster upgrade. Azure SQL contained users need the same.
   - **Problem:** GitHub-hosted runners and Microsoft-hosted ADO agents are outside the VNet, so Terraform and the deploy scripts cannot connect. The plan also has no migration step at all. After a Dagster version bump, the new daemon meets an old schema and stops.
   - **Fix:** Add one "admin" Container Apps Job inside the VNet, using the same image. The deploy pipeline starts it (`az containerapp job start`) before it updates the apps, and it runs the database user setup and `dagster instance migrate`. No self-hosted runners are needed.
   - **If we skip it:** The first deploy cannot create database users, and the first upgrade breaks prod.

7. **Front Door health probes break the login and keep containers awake** (PLAN §6.2; COSTS §3)
   - **What this is:** By default, Front Door checks that the app is alive with a request every ~30 seconds from many edge sites.
   - **Problem:** Those checks have no Entra login, so Easy Auth answers with a redirect, not "200 OK". Front Door can then mark the app as unhealthy. The checks also wake the webservers all the time, so "scale to zero" never happens and the cost estimate is too low by about $20 per environment.
   - **Fix:** Turn health probes off on each origin group. Microsoft recommends this when a group has only one origin, and ours each have one.
   - **If we skip it:** The UI may be unreachable, and the bill is higher than estimated.

8. **A backfill can flood the database and Azure** (PLAN §2, §6.3)
   - **What this is:** A backfill re-runs an asset for many dates at once, e.g. 500 days.
   - **Problem:** The plan sets no limit on parallel runs. 500 runs start 500 Jobs at once. Each opens Postgres connections, and the B1ms tier allows only about 50. Azure management calls also get throttled.
   - **Fix:** Set the run queue limit in `dagster.yaml` (`max_concurrent_runs`: e.g. 5 in dev/test, 20 in prod). Add per-platform tag limits so one platform cannot take all the slots. This is a few lines of config.
   - **If we skip it:** The first big backfill fails half its runs and slows the UI for everyone.

## Should fix

9. **Permission logic has no test** (PLAN §3.2, `domains/<name>.yaml` → grants)
   - **What this is:** Terraform turns each domain file into identities and grants on Databricks, Snowflake and Fabric.
   - **Problem:** This mapping is security logic. One mistake, such as treating `read` like `write`, gives a domain write access to production data, and nothing would catch it.
   - **Fix:** Add `terraform test` with plan-only checks. A read-only domain gets no `MODIFY`/`INSERT`/Contributor grant, and a domain never gets grants on another domain's schema.
   - **If we skip it:** Wrong grants reach prod silently.

10. **Domain separation does not apply to people** (PLAN §5.1, §6.2)
    - **What this is:** Operators use the full webserver.
    - **Problem:** In open-source Dagster, any Operator can start any domain's job, and that job runs with that domain's identity. So a sales operator can run finance jobs.
    - **Fix:** State this limit in the plan. If people must be separated per domain, the options are Dagster+ (role-based access) or one deployment per domain. Do not build a custom solution.
    - **If we skip it:** People assume a separation that does not exist.

11. **The local run launcher hands the container host control** (PLAN §2.3, §8 line 341)
    - **What this is:** Locally, the plan starts each run in its own container through the Podman socket.
    - **Problem:** Mounting the Podman socket into the daemon container lets it control the developer's machine. It also needs extra Podman setup on every laptop.
    - **Fix:** Locally, use Dagster's default launcher, which starts each run as a process inside the daemon container. This deletes the socket mount and the `dagster-docker` dependency.
    - **If we skip it:** Setup is harder, and a bad dependency in a local run can take over the laptop.

12. **Lineage discovery is a large feature nobody needs yet** (PLAN §4.2–4.4, §10 Phase 3)
    - **What this is:** The plan builds lineage discovery for four platforms, an alias map, a drift check, column lineage and a Purview/OpenLineage export.
    - **Problem:** The DAG only needs the declared lineage plus the dbt manifest, and those already link the platforms. Discovery adapters each need their own platform permissions and tests, and might never be used.
    - **Fix:** Phase 3 becomes: canonical key helper plus the dbt translator. Add discovery for one platform only when drift actually causes a problem. Move the export to a "later" list.
    - **If we skip it:** Several weeks of work and extra permissions for a feature that does not affect scheduling.

13. **Two Fabric components do the same thing** (PLAN §7.1 line 306)
    - **What this is:** `FabricPipeline` and `FabricNotebook`.
    - **Problem:** Both call the same Fabric API (run item job) and differ only in `jobType` and parameters. The generic runner already covers them.
    - **Fix:** Keep one `FabricItemJob` component with a `job_type` field, plus `FabricSemanticModelRefresh`, which uses the Power BI API and really is different.
    - **If we skip it:** Three copies of the same polling and cancel code that must change together.

## Nice to have

14. **Options nobody asked for** (PLAN §5.3 line 227, §6.1 lines 244–250)
    - **What this is:** The `private_endpoints` list, an optional NAT Gateway, and a Key Vault that holds no secrets.
    - **Problem:** Each one is extra Terraform to write, test and keep working, and none is needed now.
    - **Fix:** Delete them from the first version. Add a NAT Gateway or private endpoint only when a platform requires one.
    - **If we skip it:** More Terraform to maintain.

15. **The contract schema is checked in both repos** (PLAN §3.1, §11 line 429)
    - **What this is:** The JSON Schema lives in the solution repo but is validated by both pipelines.
    - **Problem:** The infra pipeline then has to fetch a file from the other repo, which couples the two repos the split was meant to separate.
    - **Fix:** Only the solution validates the contract when it reads it. The infra repo just publishes Terraform outputs.
    - **If we skip it:** Cross-repo fetches in CI and a release-order dependency.

16. **`dbt parse` at image build needs fake values** (PLAN §7.2)
    - **What this is:** The plan builds the dbt manifest while building the image.
    - **Problem:** `profiles.yml` reads tokens with `env_var()`. At build time no token exists, and `env_var` without a default stops the parse.
    - **Fix:** Use `env_var('X', 'unset')` defaults in `profiles.yml`. One change per line.
    - **If we skip it:** Image builds fail.

## Verdict

**Fix 1–8 before implementing.** They are small edits to the plan and decide whether the first deployment works. Points 4 and 10 are also honesty fixes to what the plan claims.

**Lean:** −2 components, −3 infra options, roughly half of the lineage phase, and −1 dependency (`dagster-docker`).

**Not checked:** Azure limits for Container Apps Jobs per environment, ACA Easy Auth group/forwarded-host settings, and the Fabric API's support for managed identities on every item type. These need the Phase 0 tests or current Azure docs, which could not be reached from this session.
