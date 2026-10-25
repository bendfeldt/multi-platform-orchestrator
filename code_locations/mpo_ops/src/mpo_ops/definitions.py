"""Internal code location for operations: alert sensors across every code location."""

import dagster as dg
from orchestrator_core.alerts import ALERT_SENSORS

defs = dg.Definitions(sensors=ALERT_SENSORS)
