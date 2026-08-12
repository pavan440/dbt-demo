with records as (
    select *
    from {{ ref('stg_silver_records_resolved') }}
),
activities as (
    select *
    from {{ ref('stg_silver_activity_resolved') }}
),
activity_rollup as (
    select
        organization_id,
        account_id,
        lead_id,
        activity_date,
        count(*) as total_activities,
        count_if(activity_type = 'CALL') as call_count,
        count_if(activity_type = 'EMAIL') as email_count,
        count_if(activity_type = 'TASK') as task_count,
        sum(duration_seconds) as total_duration_seconds
    from activities
    group by 1, 2, 3, 4
)

select
    r.organization_id,
    r.tenant_name,
    r.tenant_domain,
    r.tenant_company_size,
    r.application_id,
    r.module_key,
    r.record_id,
    r.record_name,
    r.resolved_owner_id,
    r.resolved_team_id,
    r.resolved_territory_id,
    r.resolved_stage_value,
    cast(r.cdc_timestamp as date) as record_date,
    coalesce(a.total_activities, 0) as total_activities,
    coalesce(a.call_count, 0) as call_count,
    coalesce(a.email_count, 0) as email_count,
    coalesce(a.task_count, 0) as task_count,
    coalesce(a.total_duration_seconds, 0) as total_duration_seconds,
    r.health_score,
    r.lead_score_hint,
    r.amount_usd
from records r
left join activity_rollup a
    on r.organization_id = a.organization_id
   and (
       (r.module_key = 'ACCOUNTS' and r.record_id = a.account_id)
       or (r.module_key = 'LEADS' and r.record_id = a.lead_id)
   )
   and cast(r.cdc_timestamp as date) = a.activity_date
