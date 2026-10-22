# Cost Estimate

Monthly Azure list-price estimate for the orchestrator's **own** infrastructure, per environment.
Pay-as-you-go, no reservations or discounts. Prices are US-dollar list prices gathered
October 2026. West Europe can be 0–10 % higher than the US rates some figures are based on.
**Verify in the Azure pricing calculator before budgeting.**

Not included: compute on the data platforms themselves (Databricks DBUs, Snowflake credits,
Fabric capacity), which Dagster triggers but does not add to, and Entra ID P1 licences
(needed for Conditional Access, assumed already in place).

## 1. Unit prices used

| Item | Unit price |
|---|---|
| Container Apps (Consumption), active vCPU | $0.000024 / vCPU-s (≈ $63.07 per vCPU-month) |
| Container Apps, idle vCPU (replica at min scale, not processing) | $0.000003 / vCPU-s (≈ $7.88 per vCPU-month) |
| Container Apps, memory (active or idle) | $0.000003 / GiB-s (≈ $7.88 per GiB-month) |
| Container Apps free grant | 180,000 vCPU-s + 360,000 GiB-s + 2 M requests per **subscription** per month (≈ $5.40) |
| PostgreSQL Flexible, Burstable B1ms (1 vCPU, 2 GiB) | ≈ $15 / month (West Europe) |
| PostgreSQL Flexible, Burstable B2s (2 vCPU, 4 GiB) | ≈ $58 / month (West Europe) |
| PostgreSQL storage | ≈ $0.13 / GB-month (backup storage up to the provisioned size included) |
| Container Registry Basic / Standard / Premium | ≈ $0.167 / $0.667 / $1.667 per day (≈ $5 / $20 / $50 per month) |
| Private endpoint | ≈ $7.30 / month each + ≈ $0.01 / GB processed |
| Front Door **Standard** base fee | $35 / month per profile |
| Front Door **Premium** base fee | $330 / month per profile |
| Front Door Standard requests | ≈ $0.009 per 10,000 (NA/EU) |
| Front Door data transfer edge → client | ≈ $0.083 / GB (first tier, NA/EU) |
| Log Analytics (Analytics logs) | ≈ $2.30–3.00 / GB ingested (first 5 GB/month per billing account free) |
| Private DNS zone | $0.50 / zone / month |
| Blob storage (Hot LRS) | ≈ $0.02 / GB-month |

A month is 730 hours = 2,628,000 seconds.

## 2. Cost-driven design choices

These changes to the plan keep the bill low without weakening security:

| Instead of | Use | Saving / env |
|---|---|---|
| Private endpoint for PostgreSQL | **VNet integration** (delegated subnet, private access only) | ≈ $7 |
| Private endpoints for Storage, Key Vault, Azure SQL | **Service endpoints + resource firewall** limited to the ACA subnet, public access otherwise denied | ≈ $22 |
| ACR Premium (needed for private endpoint) | **ACR Basic**, admin user off, Entra-only pull/push, no anonymous pull | ≈ $45 |
| Webservers always on | **Scale to zero** (a few seconds' cold start on first visit) | ≈ $10–24 |
| Front Door Premium | **Front Door Standard**, prod only by default (toggle per env) | ≈ $295 |
| Postgres high availability | Off (HA doubles compute); daily backups with 7-day retention | ≈ 1× compute |

Private endpoints stay available as a per-service toggle (`private_endpoints = [...]`) for
anyone whose policy requires them, e.g. Databricks / Snowflake PrivateLink (≈ $7.30 each).

## 3. Workload assumptions

| | dev | test | prod |
|---|---|---|---|
| Code locations (domains) | 1 | 1 | 2 |
| Daemon size | 0.25 vCPU / 0.5 GiB, always active | 0.25 / 0.5 | 0.5 / 1, always active |
| Code-location server | 0.25 vCPU / 0.5 GiB each, always active | same | same |
| Webservers (full + read-only) | 0.5 vCPU / 1 GiB, scale to zero, ~40 h use/month | same | read-only min 1 replica (mostly idle), full scales to zero |
| Runs (ACA Jobs, 0.5 vCPU / 1 GiB) | 20/day × 3 min | 20/day × 3 min | 200/day × 5 min |
| PostgreSQL | B1ms, 32 GB | B1ms, 32 GB | B2s, 64 GB |
| Ingress | Easy Auth | Easy Auth | Front Door Standard + Easy Auth |
| UI traffic | small | small | ~3 M requests, ~20 GB/month (Dagster UI polls frequently) |

A run job costs about $0.0009 per minute (0.5 vCPU + 1 GiB, active rate). Runs mostly wait on
Databricks / Fabric / Snowflake, so they are short and small.

## 4. Estimate per environment (USD / month)

| Component | dev | test | prod |
|---|---|---|---|
| ACA — daemon | 19.70 | 19.70 | 39.40 |
| ACA — code-location servers | 19.70 | 19.70 | 39.40 |
| ACA — webservers | 2.20 | 2.20 | 15.00 |
| ACA — run jobs | 1.60 | 1.60 | 27.00 |
| ACA free grant (if own subscription) | −5.40 | −5.40 | −5.40 |
| PostgreSQL compute | 15.00 | 15.00 | 58.00 |
| PostgreSQL storage | 4.20 | 4.20 | 8.30 |
| Container Registry (Basic) | 5.00 | 5.00 | 5.00 |
| Storage (compute logs, contract, TF state) | 1.00 | 1.00 | 2.00 |
| Log Analytics (daily cap, short retention) | 0–5 | 0–5 | 5–15 |
| Private DNS zones | 1.00 | 1.00 | 1.00 |
| Key Vault (no secrets stored; operations only) | <1 | <1 | <1 |
| Front Door Standard (base + requests + transfer) | — | — | 40.00 |
| **Total** | **≈ $65–70** | **≈ $65–70** | **≈ $235–245** |

**All three environments: ≈ $365–385 / month.**

### Variants

| Variant | Change | All envs / month |
|---|---|---|
| Lean | No Front Door anywhere | ≈ $325–345 |
| Prod on B1ms to start | Postgres B1ms in prod, scale up when needed | ≈ $320–340 |
| Lean + prod B1ms | both of the above | ≈ $280–300 |
| Front Door Standard in every env | +2 profiles | ≈ $445–465 |
| One shared subscription | Free ACA grant counted once instead of three times | +≈ $11 |
| Databricks + Snowflake PrivateLink | +2 private endpoints per env | +≈ $45 |
| Front Door **Premium** in prod instead of Standard | managed WAF rules, private origin | +≈ $295 |

### What moves the number

1. **Always-on containers** (daemon + one code-location server per domain) are the largest ACA
   cost: ≈ $20 per 0.25 vCPU always-on container per month. Each extra domain in an environment
   adds ≈ $20.
2. **PostgreSQL SKU**: B1ms → B2s is +$43 per environment.
3. **Front Door**: $35 per profile per month plus small traffic charges.
4. **Run volume**: ≈ $0.0009 per run-minute; 10,000 run-minutes ≈ $9.

## 5. CI/CD costs

- **GitHub Actions**: private repositories get 2,000 (Free) / 3,000 (Team) minutes per month
  included; expected usage (CI + image builds + Terraform) fits within that for a small team.
- **Azure DevOps** (if used): one Microsoft-hosted parallel job with 1,800 minutes/month free
  for private projects; additional parallel jobs ≈ $40/month each.

## 6. Front Door Standard vs Premium

| | Standard ($35) | Premium ($330) |
|---|---|---|
| WAF custom rules (IP/geo filtering, rate limiting) | Yes | Yes |
| WAF managed rule sets (OWASP / DRS) and bot protection | No | Yes |
| Private Link to origin | No — origin stays public, locked down as below | Yes |

Front Door Standard origin lock-down for Container Apps:

- ACA ingress IP restrictions allow only the `AzureFrontDoor.Backend` service-tag ranges
  (maintained in Terraform from the published service tags).
- Those ranges are shared by all Front Door customers, so the webserver app also checks the
  `X-Azure-FDID` header equals our profile ID (small reverse-proxy sidecar; spike to confirm the
  IP-rule limit and sidecar cost, which is ≈ $0 extra while the app is scaled to zero).
- Easy Auth still enforces Entra sign-in behind Front Door.
