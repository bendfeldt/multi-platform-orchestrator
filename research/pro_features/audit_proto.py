import sys, tempfile

sys.path.insert(0, sys.argv[1])
import dagster as dg
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import ModuleTarget
from dagster_webserver.app import create_app_from_workspace_process_context
from starlette.testclient import TestClient

LAUNCH = """mutation { launchRun(executionParams: { mode: "default",
  selector: { repositoryLocationName: "example_sales", repositoryName: "__repository__", jobName: "__ASSET_JOB",
              assetSelection: [{ path: ["fabric", "sales", "lh_raw", "dbo", "orders"] }] } })
  { __typename ... on LaunchRunSuccess { run { runId status tags { key value } } } ... on PythonError { message } } }"""
with (
    tempfile.TemporaryDirectory() as home,
    dg.instance_for_test(
        temp_dir=home,
        overrides={"run_coordinator": {"module": "audit_coord", "class": "AuditedRunCoordinator"}},
    ) as instance,
):
    t = ModuleTarget(
        module_name="example_sales.definitions",
        attribute="defs",
        working_directory=None,
        location_name="example_sales",
    )
    with WorkspaceProcessContext(instance, workspace_load_target=t, version="p") as ctx:
        c = TestClient(create_app_from_workspace_process_context(ctx))
        r = c.post(
            "/graphql",
            json={"query": LAUNCH},
            headers={"x-ms-client-principal-name": "anna@contoso.com"},
        ).json()
        print("RESULT", r["data"]["launchRun"], flush=True)
