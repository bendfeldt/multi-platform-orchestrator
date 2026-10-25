"""Run coordinator for every Dagster process (named in dagster.yaml), so it imports only core
Dagster."""

from dagster._core.run_coordinator import QueuedRunCoordinator, SubmitRunContext
from dagster._core.storage.dagster_run import DagsterRun

LAUNCHED_BY_TAG = "mpo/launched_by"
# Set by Container Apps' Entra sign-in; outside callers cannot set it.
_USER_HEADER = "x-ms-client-principal-name"


class AuditedRunCoordinator(QueuedRunCoordinator):
    """Queues runs like `QueuedRunCoordinator`; runs launched from the UI get the user's name.

    Runs started by schedules, sensors and backfills have no request, so they get no tag;
    backfills launched from the UI are in the mutation log instead.
    """

    def submit_run(self, context: SubmitRunContext) -> DagsterRun:
        user = context.get_request_header(_USER_HEADER)
        if user:
            run_id = context.dagster_run.run_id
            self._instance.add_run_tags(run_id, {LAUNCHED_BY_TAG: user})
            run = self._instance.get_run_by_id(run_id)
            if run is not None:
                context = context._replace(dagster_run=run)
        return super().submit_run(context)
