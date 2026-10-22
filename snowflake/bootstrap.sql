-- Run once per Snowflake account as ACCOUNTADMIN, before the first apply of that environment.
-- Creates the login the infra pipeline uses: a service user that trusts the environment's
-- infra deployer through workload identity federation (no password, no key).
-- Replace <env>, <tenant_id> and <infra_deployer_object_id> (bootstrap/ output).

CREATE ROLE IF NOT EXISTS MPO_INFRA_<ENV>;
-- Creates the domain roles and service users ...
GRANT CREATE ROLE, CREATE USER ON ACCOUNT TO ROLE MPO_INFRA_<ENV>;
-- ... and grants them access to existing databases and schemas. MANAGE GRANTS is broad; to
-- narrow it, have each database owner grant the needed privileges to MPO_INFRA_<ENV>
-- WITH GRANT OPTION instead.
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE MPO_INFRA_<ENV>;

CREATE USER IF NOT EXISTS MPO_INFRA_<ENV>
  TYPE = SERVICE
  DEFAULT_ROLE = MPO_INFRA_<ENV>
  WORKLOAD_IDENTITY = (
    TYPE = AZURE
    ISSUER = 'https://login.microsoftonline.com/<tenant_id>/v2.0'
    SUBJECT = '<infra_deployer_object_id>'
  );
GRANT ROLE MPO_INFRA_<ENV> TO USER MPO_INFRA_<ENV>;
