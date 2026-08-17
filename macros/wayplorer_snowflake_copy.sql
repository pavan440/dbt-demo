{% macro log_wayplorer_copy_audit(audit_table, target_table, stage_name, source_path) %}
  {% do run_query(
    "insert into " ~ audit_table ~ " (" ~
    "audit_logged_at, copy_query_id, target_table, stage_name, source_path, file_name, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name" ~
    ") select " ~
    "current_timestamp(), last_query_id(), '" ~ target_table ~ "', '" ~ stage_name ~ "', '" ~ source_path ~ "', " ~
    "file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name " ~
    "from table(result_scan(last_query_id()))"
  ) %}
{% endmacro %}


{% macro load_wayplorer_silver_poc_from_stage() %}
  {% set landing_schema = var('wayplorer_landing_schema', target.schema) %}
  {% set stage_name = var('wayplorer_stage_name', target.database ~ '.' ~ target.schema ~ '.WAYPLORER_SILVER_POC_STAGE') %}
  {% set landing_db = target.database %}
  {% set audit_table = landing_db ~ '.' ~ landing_schema ~ '.WAYPLORER_SILVER_LOAD_AUDIT' %}

  {% do run_query(
    "copy into " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_RECORDS_RESOLVED_RAW (" ~
    "organization_id, tenant_name, tenant_domain, tenant_company_size, application_id, module_key, " ~
    "record_id, resolved_record_id, resolved_owner_id, resolved_team_id, resolved_territory_id, " ~
    "resolved_stage_value, schema_version, effective_schema_hash, cdc_timestamp, operation_type, " ~
    "record_name, employee_band, annual_revenue_usd, health_score, lead_source, lead_score_hint, " ~
    "amount_usd, forecast_category, extras_json, pipeline_run_id, src_filename" ~
    ") from (" ~
    "select " ~
    "split_part(regexp_substr(metadata$filename, 'organization_id=[^/]+'), '=', 2), " ~
    "$1:tenant_name::string, $1:tenant_domain::string, $1:tenant_company_size::string, " ~
    "$1:application_id::string, " ~
    "split_part(regexp_substr(metadata$filename, 'module_key=[^/]+'), '=', 2), " ~
    "$1:record_id::string, $1:resolved_record_id::string, $1:resolved_owner_id::string, $1:resolved_team_id::string, $1:resolved_territory_id::string, " ~
    "$1:resolved_stage_value::string, $1:schema_version::string, $1:effective_schema_hash::string, " ~
    "$1:cdc_timestamp::timestamp_ntz, $1:operation_type::string, $1:record_name::string, " ~
    "$1:employee_band::string, $1:annual_revenue_usd::number, $1:health_score::float, " ~
    "$1:lead_source::string, $1:lead_score_hint::float, $1:amount_usd::float, " ~
    "$1:forecast_category::string, $1:extras_json::string, $1:pipeline_run_id::string, metadata$filename " ~
    "from @" ~ stage_name ~ "/silver_records_resolved/" ~
    ") pattern = '.*[.]parquet' on_error = continue force = true"
  ) %}
  {% do log_wayplorer_copy_audit(audit_table, landing_db ~ '.' ~ landing_schema ~ '.SILVER_RECORDS_RESOLVED_RAW', stage_name, '/silver_records_resolved/') %}

  {% do run_query(
    "copy into " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_ACTIVITY_RESOLVED_RAW (" ~
    "organization_id, tenant_name, tenant_domain, application_id, module_key, record_id, resolved_record_id, " ~
    "resolved_owner_id, resolved_team_id, resolved_territory_id, resolved_stage_value, schema_version, " ~
    "effective_schema_hash, cdc_timestamp, activity_date, operation_type, account_id, lead_id, " ~
    "activity_type, duration_seconds, outcome, extras_json, pipeline_run_id, src_filename" ~
    ") from (" ~
    "select " ~
    "split_part(regexp_substr(metadata$filename, 'organization_id=[^/]+'), '=', 2), " ~
    "$1:tenant_name::string, $1:tenant_domain::string, $1:application_id::string, " ~
    "split_part(regexp_substr(metadata$filename, 'module_key=[^/]+'), '=', 2), " ~
    "$1:record_id::string, $1:resolved_record_id::string, $1:resolved_owner_id::string, $1:resolved_team_id::string, $1:resolved_territory_id::string, $1:resolved_stage_value::string, " ~
    "$1:schema_version::string, $1:effective_schema_hash::string, $1:cdc_timestamp::timestamp_ntz, " ~
    "$1:activity_date::date, $1:operation_type::string, $1:account_id::string, $1:lead_id::string, " ~
    "$1:activity_type::string, $1:duration_seconds::number, $1:outcome::string, $1:extras_json::string, " ~
    "$1:pipeline_run_id::string, metadata$filename " ~
    "from @" ~ stage_name ~ "/silver_activity_resolved/" ~
    ") pattern = '.*[.]parquet' on_error = continue force = true"
  ) %}
  {% do log_wayplorer_copy_audit(audit_table, landing_db ~ '.' ~ landing_schema ~ '.SILVER_ACTIVITY_RESOLVED_RAW', stage_name, '/silver_activity_resolved/') %}

  {% do run_query(
    "copy into " ~ landing_db ~ "." ~ landing_schema ~ ".SILVER_KPI_DAILY_SUMMARY_RAW (" ~
    "organization_id, tenant_name, tenant_domain, tenant_company_size, kpi_date, pipeline_created_count, " ~
    "qualified_lead_count, active_opportunity_count, stage_conversion_rate, avg_stage_dwell_days, " ~
    "activity_volume, goal_attainment_ratio, pipeline_coverage_ratio, kpi_readiness_status, pipeline_run_id, src_filename" ~
    ") from (" ~
    "select " ~
    "split_part(regexp_substr(metadata$filename, 'organization_id=[^/]+'), '=', 2), " ~
    "$1:tenant_name::string, $1:tenant_domain::string, $1:tenant_company_size::string, " ~
    "to_date(split_part(regexp_substr(metadata$filename, 'kpi_date=[^/]+'), '=', 2)), " ~
    "$1:pipeline_created_count::number, $1:qualified_lead_count::number, " ~
    "$1:active_opportunity_count::number, $1:stage_conversion_rate::float, $1:avg_stage_dwell_days::float, " ~
    "$1:activity_volume::number, $1:goal_attainment_ratio::float, $1:pipeline_coverage_ratio::float, " ~
    "$1:kpi_readiness_status::string, $1:pipeline_run_id::string, metadata$filename " ~
    "from @" ~ stage_name ~ "/silver_kpi_daily_summary/" ~
    ") pattern = '.*[.]parquet' on_error = continue force = true"
  ) %}
  {% do log_wayplorer_copy_audit(audit_table, landing_db ~ '.' ~ landing_schema ~ '.SILVER_KPI_DAILY_SUMMARY_RAW', stage_name, '/silver_kpi_daily_summary/') %}
{% endmacro %}