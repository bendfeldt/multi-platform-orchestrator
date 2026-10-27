"""GitHub refuses to start a workflow (startup_failure) when a job it calls asks for more
token permissions than the calling job gives, even if that job would be skipped."""

from pathlib import Path

import pytest
import yaml
from render import REPO

WORKFLOWS = REPO / ".github" / "workflows"
LEVEL = {"none": 0, "read": 1, "write": 2}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _permissions(job: dict, workflow: dict) -> dict[str, str]:
    permissions = job.get("permissions", workflow.get("permissions"))
    assert isinstance(permissions, dict), "set permissions explicitly (least privilege)"
    return permissions


CALLS = [
    (caller.name, name, job)
    for caller in sorted(WORKFLOWS.glob("*.yml"))
    for name, job in _load(caller)["jobs"].items()
    if job.get("uses", "").startswith("./.github/workflows/")
]


@pytest.mark.parametrize(("caller", "name", "job"), CALLS, ids=[f"{c}:{n}" for c, n, _ in CALLS])
def test_called_jobs_ask_for_no_more_than_the_caller_gives(caller, name, job):
    granted = _permissions(job, _load(WORKFLOWS / caller))
    called = _load(REPO / job["uses"].removeprefix("./"))
    for called_name, called_job in called["jobs"].items():
        for scope, level in _permissions(called_job, called).items():
            assert LEVEL[granted.get(scope, "none")] >= LEVEL[level], (
                f"{caller} job {name} gives {scope}: {granted.get(scope, 'none')}, but "
                f"{job['uses']} job {called_name} asks for {scope}: {level}"
            )


def test_every_reusable_workflow_call_is_checked():
    assert {Path(job["uses"]).name for _, _, job in CALLS} == {"ci.yml", "promote.yml"}
