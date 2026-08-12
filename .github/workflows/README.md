# GitHub Actions setup for dbt

This repository includes two GitHub Actions workflows:

- `.github/workflows/dbt-ci.yml`: runs on pull requests and manual dispatch.
- `.github/workflows/dbt-deploy.yml`: runs on pushes to `main` and manual dispatch.

## Required GitHub repository secrets

Add these secrets in GitHub under `Settings -> Secrets and variables -> Actions`:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `WAYPLORER_LANDING_SCHEMA`

Example values for this project:

- `SNOWFLAKE_ACCOUNT`: `BIPCWDF-CIC01156`
- `SNOWFLAKE_ROLE`: `ACCOUNTADMIN`
- `SNOWFLAKE_WAREHOUSE`: `WAYPLORER_DBT_WH`
- `SNOWFLAKE_DATABASE`: `WAYPLORER_DBT_POC`
- `SNOWFLAKE_SCHEMA`: `TRANSFORM`
- `WAYPLORER_LANDING_SCHEMA`: `LANDING`

## Workflow behavior

`dbt-ci.yml` runs:

- `dbt debug`
- `dbt parse`
- `dbt build`

`dbt-deploy.yml` runs:

- `dbt debug`
- `dbt run`
- `dbt test`

## GitHub Actions cost notes

According to GitHub Docs:

- Actions on public repositories using standard GitHub-hosted runners are free.
- Actions on private repositories use your plan's included monthly minutes first.
- Extra usage beyond included minutes is billable unless you cap spending.
- Self-hosted runners do not consume GitHub-hosted runner minutes.
