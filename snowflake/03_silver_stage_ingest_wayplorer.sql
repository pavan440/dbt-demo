-- Snowflake batch ingest from the Wayplorer Silver PoC S3 prefix.
-- Update the storage integration, role, warehouse, and bucket path placeholders before use.

use role SYSADMIN;

create database if not exists WAYPLORER_SILVER;
create schema if not exists WAYPLORER_SILVER.LANDING;
create schema if not exists WAYPLORER_SILVER.CORE;

create or replace file format WAYPLORER_SILVER.LANDING.PARQUET_FMT
  type = parquet
  compression = auto;

create or replace stage WAYPLORER_SILVER.LANDING.SILVER_POC_STAGE
  url = 's3://wayplorer-data-lake-449/wayplorerdbtpoc/'
  storage_integration = <snowflake_storage_integration>
  file_format = WAYPLORER_SILVER.LANDING.PARQUET_FMT;

create or replace table WAYPLORER_SILVER.LANDING.SILVER_RECORDS_RESOLVED_RAW (
  organization_id string,
  tenant_name string,
  tenant_domain string,
  tenant_company_size string,
  application_id string,
  module_key string,
  record_id string,
  resolved_record_id string,
  resolved_owner_id string,
  resolved_team_id string,
  resolved_territory_id string,
  resolved_stage_value string,
  schema_version string,
  effective_schema_hash string,
  cdc_timestamp timestamp_ntz,
  operation_type string,
  record_name string,
  employee_band string,
  annual_revenue_usd number,
  health_score float,
  lead_source string,
  lead_score_hint float,
  amount_usd number,
  forecast_category string,
  extras_json string,
  pipeline_run_id string,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

create or replace table WAYPLORER_SILVER.LANDING.SILVER_ACTIVITY_RESOLVED_RAW (
  organization_id string,
  tenant_name string,
  tenant_domain string,
  application_id string,
  module_key string,
  record_id string,
  resolved_record_id string,
  resolved_owner_id string,
  resolved_team_id string,
  resolved_territory_id string,
  resolved_stage_value string,
  schema_version string,
  effective_schema_hash string,
  cdc_timestamp timestamp_ntz,
  activity_date date,
  operation_type string,
  account_id string,
  lead_id string,
  activity_type string,
  duration_seconds number,
  outcome string,
  extras_json string,
  pipeline_run_id string,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

create or replace table WAYPLORER_SILVER.LANDING.SILVER_KPI_DAILY_SUMMARY_RAW (
  organization_id string,
  tenant_name string,
  tenant_domain string,
  tenant_company_size string,
  kpi_date date,
  pipeline_created_count number,
  qualified_lead_count number,
  active_opportunity_count number,
  stage_conversion_rate float,
  avg_stage_dwell_days float,
  activity_volume number,
  goal_attainment_ratio float,
  pipeline_coverage_ratio float,
  kpi_readiness_status string,
  pipeline_run_id string,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

copy into WAYPLORER_SILVER.LANDING.SILVER_RECORDS_RESOLVED_RAW (
  organization_id,
  tenant_name,
  tenant_domain,
  tenant_company_size,
  application_id,
  module_key,
  record_id,
  resolved_record_id,
  resolved_owner_id,
  resolved_team_id,
  resolved_territory_id,
  resolved_stage_value,
  schema_version,
  effective_schema_hash,
  cdc_timestamp,
  operation_type,
  record_name,
  employee_band,
  annual_revenue_usd,
  health_score,
  lead_source,
  lead_score_hint,
  amount_usd,
  forecast_category,
  extras_json,
  pipeline_run_id,
  src_filename
)
from (
  select
    $1:organization_id::string,
    $1:tenant_name::string,
    $1:tenant_domain::string,
    $1:tenant_company_size::string,
    $1:application_id::string,
    $1:module_key::string,
    $1:record_id::string,
    $1:resolved_record_id::string,
    $1:resolved_owner_id::string,
    $1:resolved_team_id::string,
    $1:resolved_territory_id::string,
    $1:resolved_stage_value::string,
    $1:schema_version::string,
    $1:effective_schema_hash::string,
    $1:cdc_timestamp::timestamp_ntz,
    $1:operation_type::string,
    $1:record_name::string,
    $1:employee_band::string,
    $1:annual_revenue_usd::number,
    $1:health_score::float,
    $1:lead_source::string,
    $1:lead_score_hint::float,
    $1:amount_usd::number,
    $1:forecast_category::string,
    $1:extras_json::string,
    $1:pipeline_run_id::string,
    metadata$filename
  from @WAYPLORER_SILVER.LANDING.SILVER_POC_STAGE/silver_records_resolved/
)
pattern = '.*[.]parquet'
on_error = continue;

copy into WAYPLORER_SILVER.LANDING.SILVER_ACTIVITY_RESOLVED_RAW (
  organization_id,
  tenant_name,
  tenant_domain,
  application_id,
  module_key,
  record_id,
  resolved_record_id,
  resolved_owner_id,
  resolved_team_id,
  resolved_territory_id,
  resolved_stage_value,
  schema_version,
  effective_schema_hash,
  cdc_timestamp,
  activity_date,
  operation_type,
  account_id,
  lead_id,
  activity_type,
  duration_seconds,
  outcome,
  extras_json,
  pipeline_run_id,
  src_filename
)
from (
  select
    $1:organization_id::string,
    $1:tenant_name::string,
    $1:tenant_domain::string,
    $1:application_id::string,
    $1:module_key::string,
    $1:record_id::string,
    $1:resolved_record_id::string,
    $1:resolved_owner_id::string,
    $1:resolved_team_id::string,
    $1:resolved_territory_id::string,
    $1:resolved_stage_value::string,
    $1:schema_version::string,
    $1:effective_schema_hash::string,
    $1:cdc_timestamp::timestamp_ntz,
    $1:activity_date::date,
    $1:operation_type::string,
    $1:account_id::string,
    $1:lead_id::string,
    $1:activity_type::string,
    $1:duration_seconds::number,
    $1:outcome::string,
    $1:extras_json::string,
    $1:pipeline_run_id::string,
    metadata$filename
  from @WAYPLORER_SILVER.LANDING.SILVER_POC_STAGE/silver_activity_resolved/
)
pattern = '.*[.]parquet'
on_error = continue;

copy into WAYPLORER_SILVER.LANDING.SILVER_KPI_DAILY_SUMMARY_RAW (
  organization_id,
  tenant_name,
  tenant_domain,
  tenant_company_size,
  kpi_date,
  pipeline_created_count,
  qualified_lead_count,
  active_opportunity_count,
  stage_conversion_rate,
  avg_stage_dwell_days,
  activity_volume,
  goal_attainment_ratio,
  pipeline_coverage_ratio,
  kpi_readiness_status,
  pipeline_run_id,
  src_filename
)
from (
  select
    $1:organization_id::string,
    $1:tenant_name::string,
    $1:tenant_domain::string,
    $1:tenant_company_size::string,
    $1:kpi_date::date,
    $1:pipeline_created_count::number,
    $1:qualified_lead_count::number,
    $1:active_opportunity_count::number,
    $1:stage_conversion_rate::float,
    $1:avg_stage_dwell_days::float,
    $1:activity_volume::number,
    $1:goal_attainment_ratio::float,
    $1:pipeline_coverage_ratio::float,
    $1:kpi_readiness_status::string,
    $1:pipeline_run_id::string,
    metadata$filename
  from @WAYPLORER_SILVER.LANDING.SILVER_POC_STAGE/silver_kpi_daily_summary/
)
pattern = '.*[.]parquet'
on_error = continue;

