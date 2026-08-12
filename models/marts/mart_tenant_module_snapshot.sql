with records as (
    select *
    from {{ ref('stg_silver_records_resolved') }}
)

select
    organization_id,
    tenant_name,
    tenant_domain,
    tenant_company_size,
    module_key,
    count(*) as record_count,
    count(distinct resolved_owner_id) as owner_count,
    count(distinct resolved_team_id) as team_count,
    count(distinct resolved_stage_value) as stage_count,
    avg(health_score) as avg_health_score,
    avg(lead_score_hint) as avg_lead_score_hint,
    sum(amount_usd) as total_amount_usd,
    max(cdc_timestamp) as latest_cdc_timestamp
from records
group by
    organization_id,
    tenant_name,
    tenant_domain,
    tenant_company_size,
    module_key
