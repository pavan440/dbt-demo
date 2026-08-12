with record_daily as (
    select *
    from {{ ref('int_record_daily_activity') }}
),
kpis as (
    select *
    from {{ ref('stg_silver_kpi_daily_summary') }}
)

select
    k.organization_id,
    k.tenant_name,
    k.tenant_domain,
    k.tenant_company_size,
    k.kpi_date,
    k.pipeline_created_count,
    k.qualified_lead_count,
    k.active_opportunity_count,
    k.stage_conversion_rate,
    k.avg_stage_dwell_days,
    k.activity_volume,
    k.goal_attainment_ratio,
    k.pipeline_coverage_ratio,
    k.kpi_readiness_status,
    count_if(r.module_key = 'LEADS') as lead_records_seen,
    count_if(r.module_key = 'OPPORTUNITIES') as opportunity_records_seen,
    count_if(r.module_key = 'ACCOUNTS') as account_records_seen,
    coalesce(sum(r.total_activities), 0) as crm_activities_joined,
    coalesce(avg(r.health_score), 0) as avg_health_score,
    coalesce(avg(r.lead_score_hint), 0) as avg_lead_score_hint,
    coalesce(sum(r.amount_usd), 0) as pipeline_amount_usd
from kpis k
left join record_daily r
    on k.organization_id = r.organization_id
   and k.kpi_date = r.record_date
group by
    k.organization_id,
    k.tenant_name,
    k.tenant_domain,
    k.tenant_company_size,
    k.kpi_date,
    k.pipeline_created_count,
    k.qualified_lead_count,
    k.active_opportunity_count,
    k.stage_conversion_rate,
    k.avg_stage_dwell_days,
    k.activity_volume,
    k.goal_attainment_ratio,
    k.pipeline_coverage_ratio,
    k.kpi_readiness_status
