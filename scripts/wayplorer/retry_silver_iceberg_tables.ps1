param(
    [string]$Profile = 'learning',
    [string]$Region = 'us-east-1',
    [string]$Database = 'wayplorer_db_silver',
    [string]$WorkGroup = 'primary',
    [string]$ResultLocation = 's3://wayplorer-data-lake-449/athena-results/',
    [string]$TargetBase = 's3://wayplorer-data-lake-449/silver'
)

function Invoke-AthenaQuery {
    param([string]$Query)
    $start = aws athena start-query-execution --profile $Profile --region $Region --work-group $WorkGroup --query-execution-context Database=$Database --result-configuration OutputLocation=$ResultLocation --query-string $Query --output json | ConvertFrom-Json
    $queryId = $start.QueryExecutionId
    do {
        Start-Sleep -Seconds 3
        $status = aws athena get-query-execution --profile $Profile --region $Region --query-execution-id $queryId --output json | ConvertFrom-Json
        $state = $status.QueryExecution.Status.State
        Write-Output "[$queryId] $state"
    } while ($state -eq 'QUEUED' -or $state -eq 'RUNNING')
    if ($state -ne 'SUCCEEDED') {
        $reason = $status.QueryExecution.Status.StateChangeReason
        throw "Athena query failed: $reason`nQuery:`n$Query"
    }
}

$queries = @(
@"
DROP TABLE IF EXISTS silver_records_resolved_poc_iceberg
"@,
@"
CREATE TABLE silver_records_resolved_poc_iceberg
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  is_external = false,
  location = '$TargetBase/silver_records_resolved_poc_iceberg/',
  partitioning = ARRAY['organization_id', 'module_key', 'month(cdc_timestamp)']
) AS
SELECT
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
  from_iso8601_timestamp(cdc_timestamp) AS cdc_timestamp,
  operation_type,
  record_name,
  employee_band,
  annual_revenue_usd,
  health_score,
  extras_json,
  pipeline_run_id,
  lead_source,
  lead_score_hint,
  CAST(amount_usd AS bigint) AS amount_usd,
  forecast_category,
  CAST(event_date AS date) AS event_date,
  year,
  month,
  day
FROM silver_records_resolved_poc_ext
"@,
@"
DROP TABLE IF EXISTS silver_activity_resolved_poc_iceberg
"@,
@"
CREATE TABLE silver_activity_resolved_poc_iceberg
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  is_external = false,
  location = '$TargetBase/silver_activity_resolved_poc_iceberg/',
  partitioning = ARRAY['organization_id', 'module_key', 'month(cdc_timestamp)']
) AS
SELECT
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
  from_iso8601_timestamp(cdc_timestamp) AS cdc_timestamp,
  CAST(activity_date AS date) AS activity_date,
  operation_type,
  account_id,
  lead_id,
  pipeline_run_id,
  activity_type,
  duration_seconds,
  outcome,
  extras_json,
  year,
  month,
  day
FROM silver_activity_resolved_poc_ext
"@,
@"
DROP TABLE IF EXISTS silver_kpi_daily_summary_poc_iceberg
"@,
@"
CREATE TABLE silver_kpi_daily_summary_poc_iceberg
WITH (
  table_type = 'ICEBERG',
  format = 'PARQUET',
  is_external = false,
  location = '$TargetBase/silver_kpi_daily_summary_poc_iceberg/',
  partitioning = ARRAY['organization_id', 'month(kpi_date)']
) AS
SELECT
  organization_id,
  tenant_name,
  tenant_domain,
  tenant_company_size,
  CAST(kpi_date AS date) AS kpi_date,
  pipeline_created_count,
  qualified_lead_count,
  active_opportunity_count,
  stage_conversion_rate,
  avg_stage_dwell_days,
  activity_volume,
  goal_attainment_ratio,
  pipeline_coverage_ratio,
  kpi_readiness_status,
  pipeline_run_id
FROM silver_kpi_daily_summary_poc_ext
"@
)

foreach ($query in $queries) {
    Invoke-AthenaQuery -Query $query
}

Write-Output 'Iceberg retry complete.'
