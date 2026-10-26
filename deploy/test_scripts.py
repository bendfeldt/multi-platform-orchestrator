"""The deploy scripts' bookkeeping, against a stand-in `az` that lists apps from a file."""

import json
import os
import subprocess
from pathlib import Path

from render import REPO

APPS = [
    {"name": "dagster-daemon", "properties": {"template": {"containers": [{"image": "acr/p@1"}]}}},
    {
        "name": "cl-example-sales",
        "properties": {"template": {"containers": [{"image": "acr/s@1"}]}},
    },
    {
        "name": "cl-example-sales-pr12",
        "tags": {"mpo-preview": "example_sales:12"},
        "properties": {"template": {"containers": [{"image": "acr/s@pr12"}]}},
    },
    {"name": "dagster-web", "properties": {"template": {"containers": [{"image": "acr/p@1"}]}}},
]


def _run(tmp_path: Path, script: str) -> str:
    stub = tmp_path / "bin" / "az"
    stub.parent.mkdir(exist_ok=True)
    stub.write_text('#!/usr/bin/env bash\ncat "$AZ_APPS"\n')
    stub.chmod(0o755)
    (tmp_path / "apps.json").write_text(json.dumps(APPS))
    env = os.environ | {"PATH": f"{stub.parent}{os.pathsep}{os.environ['PATH']}",
                        "AZ_APPS": str(tmp_path / "apps.json")}  # fmt: skip
    lib = REPO / "deploy/scripts/lib.sh"
    return subprocess.run(
        ["bash", "-c", f"set -euo pipefail; rg=rg; out=out; source {lib}; {script}"],
        env=env, check=True, capture_output=True, text=True,
    ).stdout  # fmt: skip


def test_running_previews_and_images_are_found(tmp_path):
    assert _run(tmp_path, "previews").strip() == "example_sales:12"
    images = json.loads(_run(tmp_path, "running_images"))
    assert images == {
        "platform": "acr/p@1",
        "example_sales": "acr/s@1",
        "example_sales_pr12": "acr/s@pr12",
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout


def test_changed_locations(tmp_path):
    repo = tmp_path / "repo"
    script = repo / "deploy/scripts/changed-locations.sh"
    script.parent.mkdir(parents=True)
    script.write_text((REPO / "deploy/scripts/changed-locations.sh").read_text())
    for path in ("code_locations/sales/x.py", "code_locations/finance/x.py",
                 "code_locations/mpo_ops/x.py", "libs/core/x.py"):  # fmt: skip
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        (repo / path).write_text("v1")
    _git(repo, "init", "-q")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "add", ".")
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base")
    base = _git(repo, "rev-parse", "HEAD").strip()

    def changed(*paths: str) -> list[str]:
        for path in paths:
            (repo / path).write_text("v2")
        _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qam", "change")
        out = subprocess.run(
            ["bash", str(script), base], check=True, capture_output=True, text=True
        )
        return out.stdout.split()

    assert changed("code_locations/sales/x.py") == ["sales"]
    # Cumulative since base: sales stays; the mpo_ops change adds nothing (never previewed).
    assert changed("code_locations/mpo_ops/x.py") == ["sales"]
    assert changed("libs/core/x.py") == ["finance", "sales"]  # shared code: every domain
