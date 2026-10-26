import dagster as dg
from change_report import diff, snapshot, snapshot_definitions
from render import REPO


def _defs(owner="team:a", extra_dep=False, check=False, schedule="0 1 * * *", removed=False):
    deps = ["raw"] + (["other"] if extra_dep else [])

    @dg.asset(deps=deps, owners=[owner], group_name="g")
    def orders(): ...

    @dg.asset
    def old(): ...

    checks = []
    if check:

        @dg.asset_check(asset=orders)
        def not_empty():
            return dg.AssetCheckResult(passed=True)

        checks = [not_empty]
    job = dg.define_asset_job("nightly", selection=[orders])
    return dg.Definitions(
        assets=[orders] + ([] if removed else [old]),
        asset_checks=checks,
        jobs=[job],
        schedules=[dg.ScheduleDefinition(job=job, cron_schedule=schedule)],
    )


def test_report_lists_what_changed():
    base = {"sales": snapshot_definitions(_defs())}
    head = {
        "sales": snapshot_definitions(
            _defs(owner="team:b", extra_dep=True, check=True, schedule="0 2 * * *", removed=True)
        )
    }
    report = diff(base, head)
    assert "**removed asset** `old`" in report
    assert "`orders` deps: `['raw']` → `['other', 'raw']`" in report
    assert "`orders` owners: `['team:a']` → `['team:b']`" in report
    assert "`orders` checks: `[]` → `['not_empty']`" in report
    assert (
        "schedules: `{'nightly_schedule': '0 1 * * *'}` → `{'nightly_schedule': '0 2 * * *'}`"
        in report
    )


def test_no_changes_and_new_or_broken_locations():
    same = {"sales": snapshot_definitions(_defs())}
    assert "No changes" in diff(same, same)
    report = diff({}, {"finance": {"error": "ImportError: no module x"}} | same)
    assert (
        "### `finance`" in report and ":x: **fails to load:** `ImportError: no module x`" in report
    )
    assert "**new code location**" in report
    assert "sensors: `[]` → `[]`" not in report  # an empty list is no change


def test_snapshot_reads_every_code_location_of_the_repo():
    snap = snapshot(REPO)
    assert {"example_sales", "mpo_ops"} <= set(snap)
    mart = snap["example_sales"]["assets"]["snowflake/sales/mart/fct_orders"]
    assert mart["owners"] == ["team:sales-data"] and mart["columns"][0] == "order_id:number"
