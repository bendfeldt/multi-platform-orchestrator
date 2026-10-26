"""Health check run every 5 minutes as a Container Apps Job (docs/PRO_FEATURES_PLAN.md §4.1).

Finds what sensors cannot see, because sensors run inside the daemon and code servers: a
daemon that stopped, failed schedule/sensor ticks, code locations that fail to load. Writes the
same alert lines as `alerts.py`, and one `health_check` line per run so Azure Monitor can alert
when the check itself stops.

    python -m orchestrator_core.health /opt/dagster/workspace.yaml
"""

import sys
import time

from dagster import DagsterInstance
from dagster._core.scheduler.instigation import TickStatus
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import WorkspaceFileTarget

from orchestrator_core.alerts import emit, ui_url

# A little more than the 5-minute schedule so no failed tick falls between two runs; a tick
# near the edge can be reported twice.
TICK_WINDOW_S = 330


def check(instance: DagsterInstance, workspace_path: str | None, now: float | None = None) -> int:
    """Emits alert lines; returns how many."""
    now = time.time() if now is None else now
    alerts = 0

    for daemon, status in instance.get_daemon_statuses().items():
        if status.required and not status.healthy:
            emit("daemon_unhealthy", severity="error", daemon=daemon)
            alerts += 1

    for state in instance.all_instigator_state():
        failed = instance.get_ticks(
            state.instigator_origin_id,
            state.selector_id,
            after=now - TICK_WINDOW_S,
            statuses=[TickStatus.FAILURE],
        )
        for tick in failed:
            location = state.origin.repository_origin.code_location_origin.location_name
            emit(
                "tick_failure",
                severity="error",
                location=location,
                instigator=state.instigator_name,
                instigator_type=state.instigator_type.value,
                summary=tick.error.message if tick.error else None,
                url=ui_url(f"/locations/{location}/{state.instigator_type.value}s"),
            )
            alerts += 1

    if workspace_path:
        target = WorkspaceFileTarget(paths=[workspace_path])
        with WorkspaceProcessContext(instance, workspace_load_target=target) as context:
            for name, entry in context.get_current_workspace().code_location_entries.items():
                if entry.load_error:
                    emit(
                        "code_location_error",
                        severity="error",
                        location=name,
                        summary=entry.load_error.message[:1000],
                    )
                    alerts += 1

    emit("health_check", severity="info", alerts=alerts)
    return alerts


if __name__ == "__main__":
    with DagsterInstance.get() as instance:
        check(instance, sys.argv[1] if len(sys.argv) > 1 else None)
