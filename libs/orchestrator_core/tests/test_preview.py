import dagster as dg
from orchestrator_core.preview import preview_definitions

UPSTREAM = dg.AssetKey(["databricks", "sales", "silver", "orders"])  # another domain's


def _defs():
    @dg.asset(key=["snowflake", "sales", "mart", "a"], deps=[UPSTREAM],
              automation_condition=dg.AutomationCondition.eager())  # fmt: skip
    def a(): ...

    @dg.asset(key=["snowflake", "sales", "mart", "b"], deps=[a])
    def b(): ...

    @dg.asset_check(asset=a)
    def not_empty():
        return dg.AssetCheckResult(passed=True)

    spec = dg.AssetSpec(["fabric", "sales", "report", "r"], deps=[b])
    job = dg.define_asset_job("nightly", selection=[a, b])
    return dg.Definitions(
        assets=[a, b, spec],
        asset_checks=[not_empty],
        jobs=[job],
        schedules=[dg.ScheduleDefinition(job=job, cron_schedule="0 1 * * *")],
        sensors=[dg.AutomationConditionSensorDefinition("auto", target=dg.AssetSelection.all())],
    )


def test_preview_renames_its_own_assets_and_keeps_real_upstreams():
    preview = preview_definitions(_defs(), 12)
    graph = preview.resolve_asset_graph()
    a = dg.AssetKey(["preview", "pr12", "snowflake", "sales", "mart", "a"])
    b = dg.AssetKey(["preview", "pr12", "snowflake", "sales", "mart", "b"])
    report = dg.AssetKey(["preview", "pr12", "fabric", "sales", "report", "r"])
    assert set(preview.resolve_all_asset_keys()) == {a, b, report, UPSTREAM}  # upstream: a stub
    assert graph.get(a).parent_keys == {UPSTREAM}  # the real dev table it reads
    assert graph.get(b).parent_keys == {a} and graph.get(report).parent_keys == {b}
    assert {k.asset_key for k in graph.asset_check_keys} == {a}
    groups = {s.key: s.group_name for s in preview.resolve_all_asset_specs()}
    assert {groups[k] for k in (a, b, report)} == {"preview_pr12"}


def test_preview_has_no_automation():
    preview = preview_definitions(_defs(), 12)
    assert not preview.schedules and not preview.sensors and not preview.jobs
    assert all(s.automation_condition is None for s in preview.resolve_all_asset_specs())


def test_preview_runs_and_the_real_example_domain_loads_as_preview():
    preview = preview_definitions(_defs(), 3)
    assets = [x for x in preview.assets or [] if isinstance(x, dg.AssetsDefinition)]
    result = dg.materialize(assets, selection=[assets[0].key])
    assert result.success

    from example_sales.definitions import defs

    real = preview_definitions(defs, 7)
    specs = real.resolve_all_asset_specs()
    own = {s.key for s in specs if s.group_name == "preview_pr7"}
    assert len(own) == 4 and all(k.path[:2] == ["preview", "pr7"] for k in own)
    assert {s.key for s in specs} == own  # every upstream of the example is in the example
    assert dg.Definitions.validate_loadable(real) is None
