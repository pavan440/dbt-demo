select
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
    lead_records_seen,
    opportunity_records_seen,
    account_records_seen,
    crm_activities_joined,
    avg_health_score,
    avg_lead_score_hint,
    pipeline_amount_usd,
    case
        when kpi_readiness_status = 'BLOCKED' then 'BLOCKED'
        when pipeline_coverage_ratio >= 1.10 and stage_conversion_rate >= 0.45 then 'HEALTHY'
        when pipeline_coverage_ratio >= 0.85 then 'WATCH'
        else 'AT_RISK'
    end as pipeline_health_status
from {{ ref('int_organization_daily_scorecard') }}
