-- Batch ingest pattern for Wayplorer Gold Parquet exports.
-- Replace placeholder values before running in Snowflake.

use role SYSADMIN;

create database if not exists WAYPLORER_RAW;
create schema if not exists WAYPLORER_RAW.INGEST;
create schema if not exists WAYPLORER_RAW.CURATED;

create or replace file format WAYPLORER_RAW.INGEST.PARQUET_FMT
  type = parquet
  compression = auto;

create or replace stage WAYPLORER_RAW.INGEST.WAYPLORER_GOLD_STAGE
  url = 's3://<bucket>/wayplorer/gold/'
  storage_integration = <snowflake_storage_integration>
  file_format = WAYPLORER_RAW.INGEST.PARQUET_FMT;

create or replace table WAYPLORER_RAW.INGEST.MART_CUSTOMER_CHURN_FEATURES_RAW (
  account_id string,
  application_id string,
  client_id string,
  feature_date date,
  days_since_last_touch number,
  interaction_count_last_30d number,
  interaction_count_last_60d number,
  num_calls number,
  avg_call_duration float,
  total_interactions_lifetime number,
  support_tickets_last_30d number,
  engagement_trend_ratio float,
  days_to_contract_end number,
  contract_value_tier string,
  is_in_renewal_window boolean,
  interaction_frequency_score float,
  is_churned boolean,
  churn_risk_score float,
  risk_category string,
  contract_id string,
  contract_start_date date,
  contract_end_date date,
  contract_value number(18,2),
  contract_status string,
  account_name string,
  industry string,
  revenue_category string,
  employee_category string,
  exported_at timestamp_ntz,
  export_client_scope string,
  export_snapshot_date string,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

copy into WAYPLORER_RAW.INGEST.MART_CUSTOMER_CHURN_FEATURES_RAW (
  account_id,
  application_id,
  client_id,
  feature_date,
  days_since_last_touch,
  interaction_count_last_30d,
  interaction_count_last_60d,
  num_calls,
  avg_call_duration,
  total_interactions_lifetime,
  support_tickets_last_30d,
  engagement_trend_ratio,
  days_to_contract_end,
  contract_value_tier,
  is_in_renewal_window,
  interaction_frequency_score,
  is_churned,
  churn_risk_score,
  risk_category,
  contract_id,
  contract_start_date,
  contract_end_date,
  contract_value,
  contract_status,
  account_name,
  industry,
  revenue_category,
  employee_category,
  exported_at,
  export_client_scope,
  export_snapshot_date,
  src_filename
)
from (
  select
    $1:account_id::string,
    $1:application_id::string,
    $1:client_id::string,
    $1:feature_date::date,
    $1:days_since_last_touch::number,
    $1:interaction_count_last_30d::number,
    $1:interaction_count_last_60d::number,
    $1:num_calls::number,
    $1:avg_call_duration::float,
    $1:total_interactions_lifetime::number,
    $1:support_tickets_last_30d::number,
    $1:engagement_trend_ratio::float,
    $1:days_to_contract_end::number,
    $1:contract_value_tier::string,
    $1:is_in_renewal_window::boolean,
    $1:interaction_frequency_score::float,
    $1:is_churned::boolean,
    $1:churn_risk_score::float,
    $1:risk_category::string,
    $1:contract_id::string,
    $1:contract_start_date::date,
    $1:contract_end_date::date,
    $1:contract_value::number(18,2),
    $1:contract_status::string,
    $1:account_name::string,
    $1:industry::string,
    $1:revenue_category::string,
    $1:employee_category::string,
    $1:exported_at::timestamp_ntz,
    $1:export_client_scope::string,
    $1:export_snapshot_date::string,
    metadata$filename
  from @WAYPLORER_RAW.INGEST.WAYPLORER_GOLD_STAGE/mart_customer_churn_features/
)
pattern = '.*[.]parquet'
on_error = continue
force = false;

create or replace table WAYPLORER_RAW.CURATED.MART_CUSTOMER_CHURN_FEATURES as
select *
from WAYPLORER_RAW.INGEST.MART_CUSTOMER_CHURN_FEATURES_RAW
qualify row_number() over (
  partition by client_id, account_id, feature_date
  order by loaded_at desc
) = 1;
