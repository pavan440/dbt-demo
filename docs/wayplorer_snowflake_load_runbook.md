# Wayplorer Snowflake Load Runbook

Last updated: 2026-08-17

## Objective

Document the current Wayplorer sample-data flow used to generate local files, upload them to S3, and load them into Snowflake landing tables for dbt consumption.

## Sources Used

### Silver PoC source dataset

Local generator:
- `scripts/wayplorer/generate_wayplorer_silver_poc.py`

Generated local root:
- `data/silver_poc`

Generated source groups:
- `silver_records_resolved`
- `silver_activity_resolved`
- `silver_kpi_daily_summary`

Logical schema represented by the generated files:
- Multi-tenant CRM records for modules `ACCOUNTS`, `LEADS`, `OPPORTUNITIES`
- Multi-tenant activity records for modules `CALLS`, `EMAILS`, `TASKS`
- Daily KPI snapshots by `organization_id` and `kpi_date`

### Bronze raw sample sources

Local generators:
- `scripts/wayplorer/generate_wayplorer_bronze.py`
- `scripts/wayplorer/generate_wayplorer_task_source.py`

Generated local root:
- `data/bronze`

Generated raw entities:
- `account`
- `lead`
- `opportunity`
- `contract`
- `call`
- `email`
- `task`

Note:
- These bronze JSON files are local sample sources for the broader ingestion flow.
- The Snowflake landing flow in this repo currently loads the Silver PoC parquet dataset, not the bronze JSON dataset.

## dbt Source Definitions

Source YAML files:
- `models/sources/wayplorer_landing.yml`
- `models/sources/wayplorer_bronze.yml`

Defined dbt sources:
- `wayplorer_landing`
- `wayplorer_bronze`

Landing source tables:
- `SILVER_RECORDS_RESOLVED_RAW`
- `SILVER_ACTIVITY_RESOLVED_RAW`
- `SILVER_KPI_DAILY_SUMMARY_RAW`

Bronze source tables:
- `ACCOUNT_RAW`
- `LEAD_RAW`
- `OPPORTUNITY_RAW`
- `CONTRACT_RAW`
- `CALL_RAW`
- `EMAIL_RAW`
- `TASK_RAW`

## S3 Location Used

Configured S3 bucket:
- `wayplorer-data-lake-449`

Configured S3 prefix:
- `wayplorerdbtpoc`

Default stage URL used by dbt macro:
- `s3://wayplorer-data-lake-449/wayplorerdbtpoc/`

Upload script:
- `scripts/wayplorer/upload_silver_poc_to_s3.py`

Upload behavior:
- Uploads every file under `data/silver_poc`
- Preserves the relative partitioned directory layout under the S3 prefix

## Snowflake Stage and Landing Table Schema

Macros used:
- `macros/wayplorer_snowflake_stage.sql`
- `macros/wayplorer_snowflake_copy.sql`

Expected dbt run-operations:
- `create_wayplorer_silver_poc_stage`
- `create_wayplorer_silver_poc_landing_tables`
- `load_wayplorer_silver_poc_from_stage`

### Stage objects

Default file format:
- `<database>.<schema>.WAYPLORER_SILVER_POC_FMT`

Default external stage:
- `<database>.<schema>.WAYPLORER_SILVER_POC_STAGE`

### Landing schema

Default landing schema variable:
- `wayplorer_landing_schema`

Default landing schema value:
- `LANDING` when referenced from source YAML
- `target.schema` when referenced directly by the create/load macros unless overridden

### Landing table schema: `SILVER_RECORDS_RESOLVED_RAW`

Columns:
- `organization_id string`
- `tenant_name string`
- `tenant_domain string`
- `tenant_company_size string`
- `application_id string`
- `module_key string`
- `record_id string`
- `resolved_record_id string`
- `resolved_owner_id string`
- `resolved_team_id string`
- `resolved_territory_id string`
- `resolved_stage_value string`
- `schema_version string`
- `effective_schema_hash string`
- `cdc_timestamp timestamp_ntz`
- `operation_type string`
- `record_name string`
- `employee_band string`
- `annual_revenue_usd number`
- `health_score float`
- `lead_source string`
- `lead_score_hint float`
- `amount_usd float`
- `forecast_category string`
- `extras_json string`
- `pipeline_run_id string`
- `src_filename string`
- `loaded_at timestamp_ntz default current_timestamp()`

### Landing table schema: `SILVER_ACTIVITY_RESOLVED_RAW`

Columns:
- `organization_id string`
- `tenant_name string`
- `tenant_domain string`
- `application_id string`
- `module_key string`
- `record_id string`
- `resolved_record_id string`
- `resolved_owner_id string`
- `resolved_team_id string`
- `resolved_territory_id string`
- `resolved_stage_value string`
- `schema_version string`
- `effective_schema_hash string`
- `cdc_timestamp timestamp_ntz`
- `activity_date date`
- `operation_type string`
- `account_id string`
- `lead_id string`
- `activity_type string`
- `duration_seconds number`
- `outcome string`
- `extras_json string`
- `pipeline_run_id string`
- `src_filename string`
- `loaded_at timestamp_ntz default current_timestamp()`

### Landing table schema: `SILVER_KPI_DAILY_SUMMARY_RAW`

Columns:
- `organization_id string`
- `tenant_name string`
- `tenant_domain string`
- `tenant_company_size string`
- `kpi_date date`
- `pipeline_created_count number`
- `qualified_lead_count number`
- `active_opportunity_count number`
- `stage_conversion_rate float`
- `avg_stage_dwell_days float`
- `activity_volume number`
- `goal_attainment_ratio float`
- `pipeline_coverage_ratio float`
- `kpi_readiness_status string`
- `pipeline_run_id string`
- `src_filename string`
- `loaded_at timestamp_ntz default current_timestamp()`

## Scripts Used In This Run

Executed on 2026-08-17:
- `scripts/wayplorer/generate_wayplorer_silver_poc.py`
- `scripts/wayplorer/generate_wayplorer_bronze.py`
- `scripts/wayplorer/generate_wayplorer_task_source.py`
- `scripts/wayplorer/upload_silver_poc_to_s3.py`

dbt commands attempted on 2026-08-17:
- `.venv\Scripts\dbt.exe run --profiles-dir .dbt`

## Current Execution Results

Successful local generation on 2026-08-17:
- Silver PoC generation completed successfully.
- Bronze sample generation completed successfully.
- Task-only source generation completed successfully.

Observed generation output counts:
- `generate_wayplorer_silver_poc.py`: 419 records rows, 640 activity rows, 70 KPI rows
- `generate_wayplorer_bronze.py`: 25 account, 25 lead, 25 opportunity, 25 contract, 50 call, 75 email, 50 task
- `generate_wayplorer_task_source.py`: 50 task rows

## Issue Log

### Critical: S3 upload authentication failure

Date observed:
- 2026-08-17

Command:
- `.venv\Scripts\python.exe scripts\wayplorer\upload_silver_poc_to_s3.py`

Issue details:
- Initial failure was caused by a missing AWS CRT runtime dependency for the login credential provider.
- After installing `awscrt`, the next failure was a broken proxy configuration in the current shell: `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` were set to `http://127.0.0.1:9`.
- After clearing those proxy variables for the process, the upload still failed because the AWS login credential provider returned an expired or invalid OAuth grant.

Exact final error class:
- `boto3.exceptions.S3UploadFailedError`

Root cause summary:
- The local AWS profile used by the upload script (`learning`) does not currently have a valid login token for S3 upload.

Impact:
- Refreshed local Silver PoC data was not uploaded to S3 during this run.
- Snowflake cannot load the refreshed batch from S3 until the AWS login session is renewed.

Recommended remediation:
- Refresh or re-authenticate the AWS `learning` profile.
- Re-run `scripts/wayplorer/upload_silver_poc_to_s3.py` after the login token is valid.

### Critical: Snowflake dbt authentication missing

Date observed:
- 2026-08-17

Command:
- `.venv\Scripts\dbt.exe run --profiles-dir .dbt`

Issue details:
- dbt successfully found the local profile in `.dbt/profiles.yml`.
- Snowflake connection failed because the environment variable `DBT_ENV_SECRET_SNOWFLAKE_PASSWORD` was empty.

Exact database error:
- `251006: Password is empty`

Impact:
- No dbt models were built in Snowflake during this run.
- No Snowflake stage, landing-table creation, or `COPY INTO` execution could be validated from this session.

Recommended remediation:
- Set `DBT_ENV_SECRET_SNOWFLAKE_PASSWORD` in the current shell or update the profile to use the intended authentication method.
- Re-run the required dbt operations after authentication is available.

## Recommended Next Commands

After AWS login is refreshed:

```powershell
$env:HTTP_PROXY=''
$env:HTTPS_PROXY=''
$env:ALL_PROXY=''
.venv\Scripts\python.exe scripts\wayplorer\upload_silver_poc_to_s3.py
```

After Snowflake password is set:

```powershell
$env:DBT_ENV_SECRET_SNOWFLAKE_PASSWORD='your_password'
.venv\Scripts\dbt.exe run-operation create_wayplorer_silver_poc_stage --profiles-dir .dbt --vars "{wayplorer_storage_integration: YOUR_INTEGRATION_NAME}"
.venv\Scripts\dbt.exe run-operation create_wayplorer_silver_poc_landing_tables --profiles-dir .dbt
.venv\Scripts\dbt.exe run-operation load_wayplorer_silver_poc_from_stage --profiles-dir .dbt
.venv\Scripts\dbt.exe run --profiles-dir .dbt
.venv\Scripts\dbt.exe test --profiles-dir .dbt
```
## Status Addendum

Updated on 2026-08-17 after the initial runbook draft.

### S3 upload completed

- AWS `learning` profile login was refreshed successfully.
- `scripts/wayplorer/upload_silver_poc_to_s3.py --profile learning --region us-east-1` completed successfully.
- Total files uploaded: `1263`
- Target prefix: `s3://wayplorer-data-lake-449/wayplorerdbtpoc/`

### Remaining blocker

- Snowflake load is still blocked because `DBT_ENV_SECRET_SNOWFLAKE_PASSWORD` is not set in the current shell.
- The dbt profile in `.dbt/profiles.yml` still uses password-based authentication with `authenticator: snowflake`.

### Next required step

Set the Snowflake password in the shell, then run:

```powershell
$env:DBT_ENV_SECRET_SNOWFLAKE_PASSWORD='your_password'
.venv\Scripts\dbt.exe run-operation create_wayplorer_silver_poc_stage --profiles-dir .dbt --vars "{wayplorer_storage_integration: YOUR_INTEGRATION_NAME}"
.venv\Scripts\dbt.exe run-operation create_wayplorer_silver_poc_landing_tables --profiles-dir .dbt
.venv\Scripts\dbt.exe run-operation load_wayplorer_silver_poc_from_stage --profiles-dir .dbt
.venv\Scripts\dbt.exe run --profiles-dir .dbt
.venv\Scripts\dbt.exe test --profiles-dir .dbt
```