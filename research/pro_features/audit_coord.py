from dagster._core.run_coordinator import QueuedRunCoordinator, SubmitRunContext


class AuditedRunCoordinator(QueuedRunCoordinator):
    def submit_run(self, context: SubmitRunContext):
        user = context.get_request_header("x-ms-client-principal-name")
        if user:
            self._instance.add_run_tags(context.dagster_run.run_id, {"mpo/launched_by": user})
            context = context._replace(
                dagster_run=self._instance.get_run_by_id(context.dagster_run.run_id)
            )
        return super().submit_run(context)
