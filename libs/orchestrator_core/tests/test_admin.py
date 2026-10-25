from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from orchestrator_core import admin

OLD = datetime.now(UTC) - timedelta(days=30)
NEW = datetime.now(UTC) - timedelta(hours=1)


def test_cleanup_deletes_only_old_finished_run_jobs(monkeypatch):
    jobs = {
        "dagster-run-old": ({"dagster-run-id": "1"}, [OLD]),
        "dagster-run-recent": ({"dagster-run-id": "2"}, [NEW]),
        "dagster-run-running": ({"dagster-run-id": "3"}, [OLD, None]),
        "dagster-run-never-started": ({"dagster-run-id": "4"}, []),
        "dagster-migrate": ({}, [OLD]),  # admin job: no run tag, never deleted
    }
    deleted = []
    client = SimpleNamespace(
        jobs=SimpleNamespace(
            list_by_resource_group=lambda rg: [
                SimpleNamespace(name=name, tags=tags) for name, (tags, _) in jobs.items()
            ],
            begin_delete=lambda rg, name: deleted.append(name),
        ),
        jobs_executions=SimpleNamespace(
            list=lambda rg, name: [SimpleNamespace(end_time=end) for end in jobs[name][1]]
        ),
    )
    monkeypatch.setattr(admin, "ContainerAppsAPIClient", lambda credential, subscription: client)
    monkeypatch.setattr(admin, "DefaultAzureCredential", lambda: None)
    monkeypatch.setenv("AZURE_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub")

    assert admin.cleanup(keep_days=7) == ["dagster-run-old"]
    assert deleted == ["dagster-run-old"]
