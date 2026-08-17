{{
    config(
        materialized='table',
        tags=['goal']
    )
}}

/*
    Actuals at the finest scope the source data supports:
    organization_id x fiscal_period_id x metric_key x team x territory x owner.

    Deliberately NOT pre-aggregated to a single owner axis. Team and territory
    crosscut each other — a rep sits in one team and one territory, but a
    territory spans teams — so any model that picks one axis produces children
    larger than their parents. mart_goal_attainment sums this table against
    whichever scope columns a goal actually constrains, which keeps every child
    a true subset of its parent.

    Only metrics with a real source appear here. Everything else surfaces
    downstream as source_status = 'NO_SOURCE', so a data gap is never mistaken
    for a business result of zero.
*/

with calendar as (

    select distinct
        fiscal_period_id,
        period_start_date,
        period_end_date
    from {{ ref('dim_fiscal_calendar') }}

),

scoped_records as (

    select
        r.organization_id,
        c.fiscal_period_id,
        r.resolved_team_id as team_id,
        r.resolved_territory_id as territory_id,
        r.resolved_owner_id as owner_id,
        r.module_key,
        r.resolved_stage_value,
        r.amount_usd,
        r.record_id
    from {{ ref('stg_silver_records_resolved') }} as r
    inner join calendar as c
        on cast(r.cdc_timestamp as date) between c.period_start_date and c.period_end_date

),

metrics as (

    select
        organization_id, fiscal_period_id, team_id, territory_id, owner_id,
        'closed_won_arr' as metric_key,
        coalesce(sum(case
            when module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_WON'
            then amount_usd
        end), 0) as actual_value
    from scoped_records
    group by 1, 2, 3, 4, 5

    union all

    /*
        net_new_arr is approximated by closed-won until subscription events
        exist. It excludes expansion, contraction and churn — downstream must
        not present it as a true ARR movement figure.
    */
    select
        organization_id, fiscal_period_id, team_id, territory_id, owner_id,
        'net_new_arr' as metric_key,
        coalesce(sum(case
            when module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_WON'
            then amount_usd
        end), 0) as actual_value
    from scoped_records
    group by 1, 2, 3, 4, 5

    union all

    select
        organization_id, fiscal_period_id, team_id, territory_id, owner_id,
        'pipeline_created' as metric_key,
        coalesce(sum(case
            when module_key = 'OPPORTUNITIES'
             and resolved_stage_value not in ('CLOSED_WON', 'CLOSED_LOST')
            then amount_usd
        end), 0) as actual_value
    from scoped_records
    group by 1, 2, 3, 4, 5

    union all

    select
        organization_id, fiscal_period_id, team_id, territory_id, owner_id,
        'qualified_lead_count' as metric_key,
        count(distinct case
            when module_key = 'LEADS' and resolved_stage_value = 'QUALIFIED'
            then record_id
        end) as actual_value
    from scoped_records
    group by 1, 2, 3, 4, 5

)

select
    organization_id,
    fiscal_period_id,
    metric_key,
    team_id,
    territory_id,
    owner_id,
    actual_value,
    'COMPUTED' as source_status,
    case metric_key
        when 'net_new_arr' then 'Approximated by closed-won; excludes expansion, contraction and churn until subscription events land.'
        when 'pipeline_created' then 'Open-stage opportunity value by CDC date; no stage-history table yet, so this is a snapshot rather than true period-created pipeline.'
        else null
    end as source_caveat
from metrics
