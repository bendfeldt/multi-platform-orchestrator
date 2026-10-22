# multi-platform-orchestrator

Dagster orchestration of data assets across Databricks, Microsoft Fabric and Snowflake, deployed
to Azure Container Apps with Entra ID everywhere and least-privilege identities.

- Plan: [docs/PLAN.md](docs/PLAN.md) · Costs: [docs/COSTS.md](docs/COSTS.md) · Review: [docs/PLAN_REVIEW.md](docs/PLAN_REVIEW.md)
- Infrastructure (Terraform) lives in a separate repo, `multi-platform-orchestrator-infra`.

## Layout

| Path | What |
|---|---|
| `libs/orchestrator_core` | Shared library: Entra credentials, canonical asset keys, environment contract |
| `code_locations/<domain>` | One Dagster code location per domain (own image and identity in Azure) |
| `containers/Containerfile` | One Containerfile for all images; `SYNC_ARGS` picks the package |
| `deploy/` | Dagster instance/workspace config and the scripts CI calls |
| `local/compose.yaml` | Local stack: Postgres, webserver, daemon, code locations |
| `.github/workflows/`, `.azuredevops/pipelines/` | Thin CI wrappers around `deploy/scripts` |

## Develop

Needs [uv](https://docs.astral.sh/uv/) and [Podman](https://podman.io/) with a compose provider
(`podman-compose` or `docker-compose`).

```bash
uv sync --all-packages                            # one venv with every package
deploy/scripts/ci.sh                              # lint, types, tests, definitions check
podman compose -f local/compose.yaml up --build   # UI at http://127.0.0.1:3000
```

For work that calls real dev platforms, run Dagster on your machine so it can use your
`az login`: `uv run dagster dev -m example_sales.definitions`.

## Add a domain

1. Copy `code_locations/example_sales` to `code_locations/<domain>` and rename the package.
2. Add it to `deploy/workspace.local.yaml` and `local/compose.yaml`.
3. In the infra repo, add `domains/<domain>.yaml` for its identity and grants.
