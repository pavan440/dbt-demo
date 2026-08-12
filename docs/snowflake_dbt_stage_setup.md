# Snowflake dbt Stage Setup

You can use dbt to create the Snowflake file format, external stage, and landing tables for the Wayplorer Silver PoC.

## What is still needed

Your Snowflake connection is mostly known:

- account: `BIPCWDF-CIC01156`
- user: `RISHIKAREDDYNARRA`
- authenticator: `externalbrowser`
- role: `ACCOUNTADMIN`

The remaining required values are:

- `warehouse`
- `database`
- `schema`
- `storage_integration`

## dbt macros added

- [macros/wayplorer_snowflake_stage.sql](/D:/DBT_snowflake/macros/wayplorer_snowflake_stage.sql:1)
- [macros/wayplorer_snowflake_copy.sql](/D:/DBT_snowflake/macros/wayplorer_snowflake_copy.sql:1)

## S3 source used by the stage

`s3://wayplorer-data-lake-449/wayplorerdbtpoc/`

## Suggested dbt commands

Create the Snowflake stage objects:

```bash
dbt run-operation create_wayplorer_silver_poc_stage --vars "{wayplorer_storage_integration: YOUR_INTEGRATION_NAME}"
```

Create the Snowflake landing tables:

```bash
dbt run-operation create_wayplorer_silver_poc_landing_tables
```

Load the Parquet files from the external stage:

```bash
dbt run-operation load_wayplorer_silver_poc_from_stage
```

## Example vars

```yaml
wayplorer_storage_integration: YOUR_INTEGRATION_NAME
wayplorer_stage_name: YOUR_DATABASE.YOUR_SCHEMA.WAYPLORER_SILVER_POC_STAGE
wayplorer_file_format_name: YOUR_DATABASE.YOUR_SCHEMA.WAYPLORER_SILVER_POC_FMT
wayplorer_landing_schema: LANDING
wayplorer_s3_stage_url: s3://wayplorer-data-lake-449/wayplorerdbtpoc/
```
