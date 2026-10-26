import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import dagster as dg
import pytest
from dagster.components.testing import create_defs_folder_sandbox
from orchestrator_core.dbt import CanonicalDbtProject, canonical_dbt_key

PROJECT = Path(__file__).parent / "dbt_project"


def test_explicit_source_key_wins():
    props = {
        "name": "orders",
        "database": "x",
        "schema": "y",
        "meta": {"dagster": {"asset_key": ["Databricks", "Sales", "silver", "orders"]}},
    }
    assert canonical_dbt_key(props, "snowflake", None).path == [
        "databricks",
        "sales",
        "silver",
        "orders",
    ]


def test_fabric_needs_workspace():
    with pytest.raises(ValueError):
        canonical_dbt_key({"name": "t", "database": "wh", "schema": "dbo"}, "fabric", None)


def test_dbt_models_get_canonical_keys_and_cross_platform_deps(tmp_path):
    project = tmp_path / "dbt_project"
    shutil.copytree(PROJECT, project)
    # Production builds the manifest at image build time (docs/PLAN.md §7.2).
    dbt = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [dbt, "parse", "--project-dir", project, "--profiles-dir", project], check=True, cwd=project
    )
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "dbt"
        defs_path.mkdir()
        (defs_path / "defs.yaml").write_text(
            "type: orchestrator_core.dbt.CanonicalDbtProject\n"
            "attributes:\n"
            f"  project: {project}\n"
            "  platform: snowflake\n"
        )
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (component, defs):
            assert isinstance(component, CanonicalDbtProject)
            graph = defs.resolve_asset_graph()
            fct = dg.AssetKey(["snowflake", "test", "main", "fct_orders"])
            stg = dg.AssetKey(["snowflake", "test", "main", "stg_orders"])
            assert graph.get(fct).parent_keys == {stg}
            assert graph.get(stg).parent_keys == {
                dg.AssetKey(["databricks", "sales", "silver", "orders"])
            }
    # The query-tag macro (dbt_macros/) parses and overrides dbt-snowflake's.
    manifest = json.loads((project / "target" / "manifest.json").read_text())
    assert {"macro.test_project.snowflake__set_query_tag",
            "macro.test_project.snowflake__unset_query_tag"} <= set(manifest["macros"])  # fmt: skip
    shipped = Path(__file__).parents[1] / "dbt_macros" / "mpo_query_tag.sql"
    assert (PROJECT / "macros" / "mpo_query_tag.sql").read_text() == shipped.read_text()


def test_dbt_gets_a_token_and_query_tag_only_while_it_runs(monkeypatch):
    import os

    from orchestrator_core import dbt

    monkeypatch.setattr(dbt, "get_token", lambda scope: f"token-for-{scope}")
    with dbt._run_env("databricks", "tag"):
        assert os.environ["DATABRICKS_TOKEN"].startswith("token-for-2ff814a6")
        assert "MPO_QUERY_TAG" not in os.environ  # query tags are Snowflake's
    assert "DATABRICKS_TOKEN" not in os.environ
    with dbt._run_env("snowflake", "tag"):  # workload identity: no token to hand over
        assert "SNOWFLAKE_TOKEN" not in os.environ
        assert os.environ["MPO_QUERY_TAG"] == "tag"
    assert "MPO_QUERY_TAG" not in os.environ


def test_dbt_build_reports_columns_and_lineage_under_canonical_keys(tmp_path, monkeypatch):
    """A real dbt build (DuckDB): materializations, column schema and column lineage all use
    the canonical keys, so the catalog and lineage join the other platforms."""
    import duckdb

    project = tmp_path / "dbt_project"
    shutil.copytree(PROJECT, project)
    database = project / "test.duckdb"
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(database))
    venv_bin = str(Path(sys.executable).parent)
    monkeypatch.setenv("PATH", f"{venv_bin}{os.pathsep}{os.environ['PATH']}")
    with duckdb.connect(str(database)) as con:
        con.execute("create schema silver; create table silver.orders (id integer, amount double)")
    dbt = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [dbt, "parse", "--project-dir", project, "--profiles-dir", project], check=True, cwd=project
    )
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "dbt"
        defs_path.mkdir()
        (defs_path / "defs.yaml").write_text(
            "type: orchestrator_core.dbt.CanonicalDbtProject\n"
            "attributes:\n"
            f"  project: {project}\n"
            "  platform: snowflake\n"
            "  include_metadata: [column_metadata]\n"
        )
        monkeypatch.chdir(project)  # dbt finds profiles.yml here
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (_, defs):
            assets = [a for a in defs.assets or [] if isinstance(a, dg.AssetsDefinition)]
            result = dg.materialize(assets, resources=defs.resources)
    assert result.success
    fct = dg.AssetKey(["snowflake", "test", "main", "fct_orders"])
    stg = dg.AssetKey(["snowflake", "test", "main", "stg_orders"])
    events = {
        e.asset_key: e.materialization.metadata for e in result.get_asset_materialization_events()
    }
    assert set(events) == {fct, stg}
    schema = events[fct]["dagster/column_schema"].value
    assert isinstance(schema, dg.TableSchema)
    assert [c.name for c in schema.columns] == ["id", "amount"]
    lineage = events[fct]["dagster/column_lineage"].value
    assert isinstance(lineage, dg.TableColumnLineage)
    assert {d.asset_key for d in lineage.deps_by_column["amount"]} == {stg}
