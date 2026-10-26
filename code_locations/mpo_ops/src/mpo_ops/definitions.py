"""Internal code location for operations: alert sensors across every code location, and the
daily cost and insight reports (docs/PRO_FEATURES_PLAN.md §4–5)."""

import dagster as dg
from orchestrator_core.alerts import ALERT_SENSORS
from orchestrator_core.cost import cost_job, cost_schedule
from orchestrator_core.insights import insights_job, insights_schedule

defs = dg.Definitions(
    jobs=[cost_job, insights_job],
    schedules=[cost_schedule, insights_schedule],
    sensors=ALERT_SENSORS,
)
