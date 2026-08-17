{% macro create_wayplorer_silver_poc_stage() %}
  {% set storage_integration = var('wayplorer_storage_integration', 'REPLACE_ME_STORAGE_INTEGRATION') %}
  {% set s3_url = var('wayplorer_s3_stage_url', 's3://wayplorer-data-lake-449/wayplorerdbtpoc/') %}
  {% set file_format_name = var('wayplorer_file_format_name', target.database ~ '.' ~ target.schema ~ '.WAYPLORER_SILVER_POC_FMT') %}
  {% set stage_name = var('wayplorer_stage_name', target.database ~ '.' ~ target.schema ~ '.WAYPLORER_SILVER_POC_STAGE') %}

  {% do log('Creating Snowflake file format: ' ~ file_format_name, info=True) %}
  {% do run_query(
    "create file format if not exists " ~ file_format_name ~ " type = parquet compression = auto"
  ) %}

  {% do log('Creating Snowflake external stage: ' ~ stage_name, info=True) %}
  {% do run_query(
    "create stage if not exists " ~ stage_name ~
    " url = '" ~ s3_url ~ "'" ~
    " storage_integration = " ~ storage_integration ~
    " file_format = " ~ file_format_name
  ) %}
{% endmacro %}


{% macro create_wayplorer_silver_poc_landing_tables() %}
  {% set landing_schema = var('wayplorer_landing_schema', target.schema) %}
  {% set landing_db = target.database %}

  {% do run_query("create schema if not exists " ~ landing_db ~ "." ~ landing_schema) %}

  {% do run_query(
    "create or replace table " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_RECORDS_RESOLVED_RAW (" ~
    "organization_id string," ~
    "tenant_name string," ~
    "tenant_domain string," ~
    "tenant_company_size string," ~
    "application_id string," ~
    "module_key string," ~
    "record_id string," ~
    "resolved_record_id string," ~
    "resolved_owner_id string," ~
    "resolved_team_id string," ~
    "resolved_territory_id string," ~
    "resolved_stage_value string," ~
    "schema_version string," ~
    "effective_schema_hash string," ~
    "cdc_timestamp timestamp_ntz," ~
    "operation_type string," ~
    "record_name string," ~
    "employee_band string," ~
    "annual_revenue_usd number," ~
    "health_score float," ~
    "lead_source string," ~
    "lead_score_hint float," ~
    "amount_usd float," ~
    "forecast_category string," ~
    "extras_json string," ~
    "pipeline_run_id string," ~
    "src_filename string," ~
    "loaded_at timestamp_ntz default current_timestamp()" ~
    ")"
  ) %}

  {% do run_query(
    "create or replace table " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_ACTIVITY_RESOLVED_RAW (" ~
    "organization_id string," ~
    "tenant_name string," ~
    "tenant_domain string," ~
    "application_id string," ~
    "module_key string," ~
    "record_id string," ~
    "resolved_record_id string," ~
    "resolved_owner_id string," ~
    "resolved_team_id string," ~
    "resolved_territory_id string," ~
    "resolved_stage_value string," ~
    "schema_version string," ~
    "effective_schema_hash string," ~
    "cdc_timestamp timestamp_ntz," ~
    "activity_date date," ~
    "operation_type string," ~
    "account_id string," ~
    "lead_id string," ~
    "activity_type string," ~
    "duration_seconds number," ~
    "outcome string," ~
    "extras_json string," ~
    "pipeline_run_id string," ~
    "src_filename string," ~
    "loaded_at timestamp_ntz default current_timestamp()" ~
    ")"
  ) %}

  {% do run_query(
    "create or replace table " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_KPI_DAILY_SUMMARY_RAW (" ~
    "organization_id string," ~
    "tenant_name string," ~
    "tenant_domain string," ~
    "tenant_company_size string," ~
    "kpi_date date," ~
    "pipeline_created_count number," ~
    "qualified_lead_count number," ~
    "active_opportunity_count number," ~
    "stage_conversion_rate float," ~
    "avg_stage_dwell_days float," ~
    "activity_volume number," ~
    "goal_attainment_ratio float," ~
    "pipeline_coverage_ratio float," ~
    "kpi_readiness_status string," ~
    "pipeline_run_id string," ~
    "src_filename string," ~
    "loaded_at timestamp_ntz default current_timestamp()" ~
    ")"
  ) %}

  {% do run_query(
    "create table if not exists " ~ landing_db ~ "." ~ landing_schema ~ ".WAYPLORER_SILVER_LOAD_AUDIT (" ~
    "audit_logged_at timestamp_ntz default current_timestamp()," ~
    "copy_query_id string," ~
    "target_table string," ~
    "stage_name string," ~
    "source_path string," ~
    "file_name string," ~
    "status string," ~
    "rows_parsed number," ~
    "rows_loaded number," ~
    "error_limit number," ~
    "errors_seen number," ~
    "first_error string," ~
    "first_error_line number," ~
    "first_error_character number," ~
    "first_error_column_name string" ~
    ")"
  ) %}
{% endmacro %}