"""Microsoft Fabric item jobs and semantic model refreshes over REST, with Entra tokens."""

import time
from typing import Any

import dagster as dg
import requests

from orchestrator_core.auth import get_token
from orchestrator_core.resources._wait import wait_until_done

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
POWER_BI_API = "https://api.powerbi.com/v1.0/myorg"
POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

_JOB_DONE = {"Completed", "Failed", "Cancelled", "Deduped"}
# Enhanced refresh reports a running refresh as "Unknown".
_REFRESH_DONE = {"Completed", "Failed", "Cancelled", "Disabled"}


class FabricError(Exception):
    pass


def _request(method: str, url: str, scope: str, **kwargs: Any) -> requests.Response:
    # Fabric and Power BI throttle with 429 + Retry-After; polling many runs hits it.
    attempts = 5
    while True:
        response = requests.request(
            method,
            url,
            headers={"Authorization": f"Bearer {get_token(scope)}"},
            timeout=60,
            **kwargs,
        )
        attempts -= 1
        if response.status_code != 429 or attempts == 0:
            response.raise_for_status()
            return response
        time.sleep(int(response.headers.get("Retry-After", "30")))


class FabricResource(dg.ConfigurableResource):
    poll_interval_seconds: float = 15.0

    def run_item_job(
        self,
        workspace_id: str,
        item_id: str,
        job_type: str,
        execution_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Runs any Fabric item with a job type (Pipeline, RunNotebook, sparkjob, ...)."""
        start = _request(
            "POST",
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances",
            FABRIC_SCOPE,
            params={"jobType": job_type},
            json={"executionData": execution_data} if execution_data else None,
        )
        job_url = start.headers["Location"]
        job = wait_until_done(
            poll=lambda: _request("GET", job_url, FABRIC_SCOPE).json(),
            is_done=lambda job: job["status"] in _JOB_DONE,
            cancel=lambda: _request("POST", f"{job_url}/cancel", FABRIC_SCOPE),
            interval=self.poll_interval_seconds,
        )
        if job["status"] != "Completed":
            raise FabricError(
                f"Fabric job {job.get('id')} ended {job['status']}: {job.get('failureReason')}"
            )
        return job

    def refresh_semantic_model(
        self, workspace_id: str, dataset_id: str, objects: list[dict[str, str]] | None = None
    ) -> dict[str, Any]:
        """Enhanced refresh of a semantic model; `objects` limits it to tables or partitions."""
        body: dict[str, Any] = {"type": "full"}
        if objects:
            body["objects"] = objects
        start = _request(
            "POST",
            f"{POWER_BI_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            POWER_BI_SCOPE,
            json=body,
        )
        refresh_url = start.headers["Location"]
        refresh = wait_until_done(
            poll=lambda: _request("GET", refresh_url, POWER_BI_SCOPE).json(),
            is_done=lambda refresh: refresh["status"] in _REFRESH_DONE,
            cancel=lambda: _request("DELETE", refresh_url, POWER_BI_SCOPE),
            interval=self.poll_interval_seconds,
        )
        if refresh["status"] != "Completed":
            raise FabricError(
                f"Semantic model refresh {refresh_url} ended {refresh['status']}: "
                f"{refresh.get('messages') or refresh.get('extendedStatus')}"
            )
        return refresh
