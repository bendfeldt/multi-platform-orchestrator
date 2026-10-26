# Service levels

What this platform promises its users, measured only with data it already produces. Nobody
else guarantees these: they are yours to run (docs/PRO_FEATURES_PLAN.md §8). Every query runs
in the environment's Log Analytics workspace; they are checked with Microsoft's Kusto parser
against the tables they read.

| SLI | Target (suggested) | Source |
|---|---|---|
| Platform health: 5-minute windows in which the health job reported and found no unhealthy daemon | 99.5 % per 30 days | `health_check` / `daemon_unhealthy` lines (`orchestrator_core.health`) |
| Run success: finished runs that succeeded, per job | 98 % per 30 days | `MpoInsights_CL` (`mpo_insights`) |
| Freshness: asset-days without a freshness failure | 99 % per 30 days | `MpoInsights_CL` |
| Platform errors: Container Apps errors (failed revisions, crash loops, probes) | 0 open for > 1 h | `ContainerAppSystemLogs_CL` |

UI availability is covered by Container Apps' liveness and readiness probes on
`/server_info` (deploy/render.py) and by the platform-errors alert; a separate availability
test from outside would need a signed-in test user, which this setup deliberately has not.

## Queries

Platform health (share of good 5-minute windows, last 30 days):

```kusto
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(30d) and ContainerJobName_s == "dagster-health"
| where Log_s startswith '{"mpo_alert"'
| extend a = parse_json(Log_s)
| extend alert_kind = tostring(a["mpo_alert"])
| where alert_kind in ("health_check", "daemon_unhealthy")
| summarize reported = countif(alert_kind == "health_check"),
    unhealthy = countif(alert_kind == "daemon_unhealthy")
    by window = bin(TimeGenerated, 5m)
| summarize good = countif(reported > 0 and unhealthy == 0)
| extend expected = 30 * 24 * 12, health_pct = round(100.0 * good / (30 * 24 * 12), 2)
```

Run success per job:

```kusto
MpoInsights_CL
| summarize arg_max(TimeGenerated, *) by Day, Scope, Location, Job, Asset // a re-run day counts once
| where Day > ago(30d) and Scope == "job"
| summarize Runs = sum(Runs), Succeeded = sum(Succeeded) by Location, Job
| extend success_pct = round(100.0 * Succeeded / Runs, 2)
| order by success_pct asc
```

Freshness per asset:

```kusto
MpoInsights_CL
| summarize arg_max(TimeGenerated, *) by Day, Scope, Location, Job, Asset // a re-run day counts once
| where Day > ago(30d) and Scope == "asset"
| summarize days = dcount(Day), failed_days = dcountif(Day, FreshnessFailures > 0) by Asset
| extend fresh_pct = round(100.0 * (days - failed_days) / days, 2)
| order by fresh_pct asc
```

Platform errors per app (last 24 hours):

```kusto
ContainerAppSystemLogs_CL
| where TimeGenerated > ago(1d) and LogLevel_s == "Error"
| summarize errors = count(), last_error = max(TimeGenerated) by ContainerAppName_s
| order by errors desc
```

## When an SLO is missed

Follow docs/RUNBOOK.md: the alert that fired names the code location, job or app.
