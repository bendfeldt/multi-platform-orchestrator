"""Databricks jobs with Entra tokens from the run's identity."""

from typing import Any

import dagster as dg
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.credentials_provider import credentials_strategy
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from orchestrator_core.auth import get_token
from orchestrator_core.resources._wait import wait_until_done

# The fixed Entra application ID of Azure Databricks.
DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

_DONE = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}


class DatabricksError(Exception):
    pass


@credentials_strategy("entra-identity", ["host"])
def _entra(_config: Config):
    # Called per request, so long polls never use an expired token.
    return lambda: {"Authorization": f"Bearer {get_token(DATABRICKS_SCOPE)}"}


class DatabricksResource(dg.ConfigurableResource):
    host: str
    poll_interval_seconds: float = 30.0

    def client(self) -> WorkspaceClient:
        return WorkspaceClient(config=Config(host=self.host, credentials_strategy=_entra))

    def run_job(self, job_id: int, job_parameters: dict[str, str] | None = None) -> dict[str, Any]:
        """Runs an existing Databricks job and waits for it."""
        # shortcut: no Dagster Pipes, so job logs stay in Databricks (the run URL is attached);
        # switch to PipesDatabricksClient when jobs should stream logs and metadata to Dagster.
        client = self.client()
        run_id = client.jobs.run_now(job_id=job_id, job_parameters=job_parameters).run_id
        run = wait_until_done(
            poll=lambda: client.jobs.get_run(run_id),
            is_done=lambda run: run.state is not None and run.state.life_cycle_state in _DONE,
            cancel=lambda: client.jobs.cancel_run(run_id),
            interval=self.poll_interval_seconds,
        )
        assert run.state is not None
        if run.state.result_state != RunResultState.SUCCESS:
            raise DatabricksError(
                f"Databricks run {run_id} ended {run.state.result_state}: {run.state.state_message}"
            )
        return {"run_id": run_id, "run_page_url": run.run_page_url}
