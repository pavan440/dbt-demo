# Wayplorer dbt Snowflake PoC

This repository now contains a Snowflake/dbt PoC for the Wayplorer multi-tenant CRM Silver layer.

## Included

- Snowflake stage and landing macros for the uploaded Silver PoC S3 data
- dbt source definitions for Snowflake landing tables
- staging models for records, activities, and KPI daily summaries
- intermediate scorecard models
- mart models for tenant pipeline health and module snapshots

## Current S3 PoC Location

`s3://wayplorer-data-lake-449/wayplorerdbtpoc/`

## Current Glue Iceberg Silver Tables

- `wayplorer_db_silver.silver_records_resolved_poc_iceberg`
- `wayplorer_db_silver.silver_activity_resolved_poc_iceberg`
- `wayplorer_db_silver.silver_kpi_daily_summary_poc_iceberg`

## Suggested dbt Flow

```bash
dbt run-operation create_wayplorer_silver_poc_stage --vars "{wayplorer_storage_integration: YOUR_STORAGE_INTEGRATION}"
dbt run-operation create_wayplorer_silver_poc_landing_tables
dbt run-operation load_wayplorer_silver_poc_from_stage
dbt run
dbt test
```

## Example Snowflake profile

Use a `profiles.yml` entry similar to:

```yaml
wayplorer:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: BIPCWDF-CIC01156
      user: RISHIKAREDDYNARRA
      authenticator: externalbrowser
      role: ACCOUNTADMIN
      warehouse: WAYPLORER_DBT_WH
      database: WAYPLORER_DBT_POC
      schema: LANDING
      threads: 4
```
