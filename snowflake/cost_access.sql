-- Run once per Snowflake account as ACCOUNTADMIN, after the environment's first apply has
-- created the mpo_ops role. Lets the cost report read QUERY_ATTRIBUTION_HISTORY (account usage
-- metadata only, no business data). Replace <ENV> with DEV, TEST or PROD.
--
-- Not in Terraform yet: whether the infra role's MANAGE GRANTS suffices to grant SNOWFLAKE
-- database roles is unverified (solution repo, docs/PRO_FEATURES_PLAN.md spike S8).
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE MPO_MPO_OPS_<ENV>;
