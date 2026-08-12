-- Near-real-time Snowpipe pattern for Wayplorer Gold Parquet exports.
-- Assumes S3 event notifications are connected to Snowflake auto-ingest.

use role SYSADMIN;

create database if not exists WAYPLORER_RT;
create schema if not exists WAYPLORER_RT.INGEST;
create schema if not exists WAYPLORER_RT.CURATED;

create or replace file format WAYPLORER_RT.INGEST.PARQUET_FMT
  type = parquet
  compression = auto;

create or replace stage WAYPLORER_RT.INGEST.WAYPLORER_GOLD_STAGE
  url = 's3://<bucket>/wayplorer/gold/'
  storage_integration = <snowflake_storage_integration>
  file_format = WAYPLORER_RT.INGEST.PARQUET_FMT;

create or replace table WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW (
  lead_id string,
  application_id string,
  account_id string,
  client_id string,
  feature_date date,
  lead_score float,
  lead_score_band string,
  touchpoints_last_30d number,
  days_since_last_touch number,
  lead_source string,
  lead_status string,
  company string,
  industry string,
  exported_at timestamp_ntz,
  export_client_scope string,
  export_snapshot_date string,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

create or replace pipe WAYPLORER_RT.INGEST.WAYPLORER_LEAD_SCORE_PIPE
  auto_ingest = true
as
copy into WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW (
  lead_id,
  application_id,
  account_id,
  client_id,
  feature_date,
  lead_score,
  lead_score_band,
  touchpoints_last_30d,
  days_since_last_touch,
  lead_source,
  lead_status,
  company,
  industry,
  exported_at,
  export_client_scope,
  export_snapshot_date,
  src_filename
)
from (
  select
    $1:lead_id::string,
    $1:application_id::string,
    $1:account_id::string,
    $1:client_id::string,
    $1:feature_date::date,
    $1:lead_score::float,
    $1:lead_score_band::string,
    $1:touchpoints_last_30d::number,
    $1:days_since_last_touch::number,
    $1:lead_source::string,
    $1:lead_status::string,
    $1:company::string,
    $1:industry::string,
    $1:exported_at::timestamp_ntz,
    $1:export_client_scope::string,
    $1:export_snapshot_date::string,
    metadata$filename
  from @WAYPLORER_RT.INGEST.WAYPLORER_GOLD_STAGE/mart_lead_score/
)
pattern = '.*[.]parquet'
on_error = continue;

create or replace stream WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW_STREAM
  on table WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW;

create or replace table WAYPLORER_RT.CURATED.MART_LEAD_SCORE (
  lead_id string,
  application_id string,
  account_id string,
  client_id string,
  feature_date date,
  lead_score float,
  lead_score_band string,
  touchpoints_last_30d number,
  days_since_last_touch number,
  lead_source string,
  lead_status string,
  company string,
  industry string,
  exported_at timestamp_ntz,
  export_client_scope string,
  export_snapshot_date string,
  src_filename string,
  loaded_at timestamp_ntz
);

create or replace task WAYPLORER_RT.INGEST.MERGE_LEAD_SCORE_TASK
  warehouse = <snowflake_warehouse>
  schedule = '1 minute'
when
  system$stream_has_data('WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW_STREAM')
as
merge into WAYPLORER_RT.CURATED.MART_LEAD_SCORE target
using (
  select *
  from WAYPLORER_RT.INGEST.MART_LEAD_SCORE_RAW_STREAM
  qualify row_number() over (
    partition by client_id, lead_id, feature_date
    order by loaded_at desc
  ) = 1
) src
on target.client_id = src.client_id
and target.lead_id = src.lead_id
and target.feature_date = src.feature_date
when matched then update set
  application_id = src.application_id,
  account_id = src.account_id,
  lead_score = src.lead_score,
  lead_score_band = src.lead_score_band,
  touchpoints_last_30d = src.touchpoints_last_30d,
  days_since_last_touch = src.days_since_last_touch,
  lead_source = src.lead_source,
  lead_status = src.lead_status,
  company = src.company,
  industry = src.industry,
  exported_at = src.exported_at,
  export_client_scope = src.export_client_scope,
  export_snapshot_date = src.export_snapshot_date,
  src_filename = src.src_filename,
  loaded_at = src.loaded_at
when not matched then insert (
  lead_id,
  application_id,
  account_id,
  client_id,
  feature_date,
  lead_score,
  lead_score_band,
  touchpoints_last_30d,
  days_since_last_touch,
  lead_source,
  lead_status,
  company,
  industry,
  exported_at,
  export_client_scope,
  export_snapshot_date,
  src_filename,
  loaded_at
) values (
  src.lead_id,
  src.application_id,
  src.account_id,
  src.client_id,
  src.feature_date,
  src.lead_score,
  src.lead_score_band,
  src.touchpoints_last_30d,
  src.days_since_last_touch,
  src.lead_source,
  src.lead_status,
  src.company,
  src.industry,
  src.exported_at,
  src.export_client_scope,
  src.export_snapshot_date,
  src.src_filename,
  src.loaded_at
);

alter task WAYPLORER_RT.INGEST.MERGE_LEAD_SCORE_TASK resume;
