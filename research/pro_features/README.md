# Pro-feature prototypes

Throwaway scripts that proved the mechanisms in [docs/PRO_FEATURES_PLAN.md](../../docs/PRO_FEATURES_PLAN.md)
against the real Dagster 1.13.26 webserver and GraphQL API (no mocks). They become proper
modules and tests during implementation.

```bash
uv run --all-packages --all-extras --with httpx2 python research/pro_features/rbac_proto.py
uv run --all-packages --all-extras --with httpx2 python research/pro_features/roles_proto.py
uv run --all-packages --all-extras --with httpx2 python research/pro_features/audit_proto.py research/pro_features
uv run --all-packages --all-extras --with httpx2 python research/pro_features/audit_mw_proto.py
```

Each script waits up to 60 s at the end while Dagster shuts its code server down.

| Script | Proves | Result (2026-10-25) |
|---|---|---|
| `rbac_proto.py` | Per-code-location write access from Container Apps' `X-MS-CLIENT-PRINCIPAL` header | Anonymous, viewer and other-domain operator: `UnauthorizedError`; domain operator: allowed for its location only; admin: everything incl. workspace reload |
| `roles_proto.py` | Launcher vs Editor roles per location | Launcher: launch/terminate yes, edit sensor/start schedule/wipe/reload no; Editor: all |
| `audit_proto.py` + `audit_coord.py` | Runs launched from the UI carry the signed-in user | Queued run tagged `mpo/launched_by: anna@contoso.com` |
| `audit_mw_proto.py` | Every GraphQL mutation logged with user, without breaking requests | Query and mutation succeed; audit record has user, mutation and variables |
