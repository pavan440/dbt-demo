{{
    config(
        materialized='table',
        tags=['goal', 'feasibility']
    )
}}

/*
    The inputs a feasibility check is scored against, at the same scope grain as
    int_metric_actual so both join to a goal the same way.

    Feasibility is not "compare the target to last period's total" — that cannot
    tell an ambitious plan from an impossible one. It is "derive what each driver
    would have to do, then check each against its own history and its physical
    limits". This model produces those drivers.

    Two drivers the source data cannot yet support are emitted as null rather
    than estimated:
      - cycle_days      needs opportunity stage history (created -> closed)
      - stage_conversion needs the same, plus lead-to-opportunity lineage
    Both are the reason avg_stage_dwell_days and stage_conversion_rate are
    synthetic upstream. Estimating them here would launder a guess into a
    feasibility score, which is worse than leaving the gap visible.
*/

with calendar as (

    select distinct
        fiscal_period_id,
        period_start_date,
        period_end_date,
        workdays_in_period
    from {{ ref('dim_fiscal_calendar') }}

),

records as (

    select
        r.organization_id,
        c.fiscal_period_id,
        c.period_end_date,
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

deal_drivers as (

    select
        organization_id,
        fiscal_period_id,
        team_id,
        territory_id,
        owner_id,
        count_if(module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_WON') as deals_won,
        count_if(module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_LOST') as deals_lost,
        count_if(module_key = 'OPPORTUNITIES'
                 and resolved_stage_value not in ('CLOSED_WON', 'CLOSED_LOST')) as deals_open,
        sum(case
            when module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_WON'
            then amount_usd
        end) as won_value,
        avg(case
            when module_key = 'OPPORTUNITIES' and resolved_stage_value = 'CLOSED_WON'
            then amount_usd
        end) as avg_deal_size,
        sum(case
            when module_key = 'OPPORTUNITIES'
             and resolved_stage_value not in ('CLOSED_WON', 'CLOSED_LOST')
            then amount_usd
        end) as open_pipeline_value,
        count_if(module_key = 'LEADS' and resolved_stage_value = 'QUALIFIED') as qualified_leads
    from records
    group by 1, 2, 3, 4, 5

),

/*
    Capacity. A ramping rep does not carry a full quota, so a plan that adds
    headcount mid-period cannot claim their full productivity — this is the
    single most common way a capacity-based split overstates what is reachable.
*/
capacity as (

    select
        u.organization_id,
        c.fiscal_period_id,
        u.team_id,
        u.territory_id,
        u.user_id as owner_id,
        1 as headcount,
        case
            when u.hire_date > c.period_end_date then 0
            when u.ramp_end_date <= c.period_end_date then 1
            else 0.5
        end as ramped_headcount,
        case
            when u.hire_date > c.period_end_date then 'NOT_STARTED'
            when u.ramp_end_date <= c.period_end_date then 'FULL'
            else 'RAMPING'
        end as ramp_status,
        u.fully_loaded_cost_usd
    from {{ ref('dim_user') }} as u
    cross join calendar as c
    where u.is_active

),

capacity_rollup as (

    select
        organization_id,
        fiscal_period_id,
        team_id,
        territory_id,
        owner_id,
        sum(headcount) as headcount,
        sum(ramped_headcount) as ramped_headcount,
        max(ramp_status) as ramp_status,
        sum(fully_loaded_cost_usd) as fully_loaded_cost_usd
    from capacity
    group by 1, 2, 3, 4, 5

),

activity as (

    select
        a.organization_id,
        c.fiscal_period_id,
        a.resolved_team_id as team_id,
        a.resolved_territory_id as territory_id,
        a.resolved_owner_id as owner_id,
        count(*) as activity_count,
        sum(a.duration_seconds) as activity_seconds
    from {{ ref('stg_silver_activity_resolved') }} as a
    inner join calendar as c
        on a.activity_date between c.period_start_date and c.period_end_date
    group by 1, 2, 3, 4, 5

),

combined as (

    select
        coalesce(d.organization_id, cap.organization_id, act.organization_id) as organization_id,
        coalesce(d.fiscal_period_id, cap.fiscal_period_id, act.fiscal_period_id) as fiscal_period_id,
        coalesce(d.team_id, cap.team_id, act.team_id) as team_id,
        coalesce(d.territory_id, cap.territory_id, act.territory_id) as territory_id,
        coalesce(d.owner_id, cap.owner_id, act.owner_id) as owner_id,
        coalesce(d.deals_won, 0) as deals_won,
        coalesce(d.deals_lost, 0) as deals_lost,
        coalesce(d.deals_open, 0) as deals_open,
        d.won_value,
        d.avg_deal_size,
        d.open_pipeline_value,
        coalesce(d.qualified_leads, 0) as qualified_leads,
        coalesce(cap.headcount, 0) as headcount,
        coalesce(cap.ramped_headcount, 0) as ramped_headcount,
        cap.ramp_status,
        cap.fully_loaded_cost_usd,
        coalesce(act.activity_count, 0) as activity_count,
        act.activity_seconds
    from deal_drivers as d
    full outer join capacity_rollup as cap
        on  d.organization_id = cap.organization_id
        and d.fiscal_period_id = cap.fiscal_period_id
        and d.team_id = cap.team_id
        and d.territory_id = cap.territory_id
        and d.owner_id = cap.owner_id
    full outer join activity as act
        on  coalesce(d.organization_id, cap.organization_id) = act.organization_id
        and coalesce(d.fiscal_period_id, cap.fiscal_period_id) = act.fiscal_period_id
        and coalesce(d.team_id, cap.team_id) = act.team_id
        and coalesce(d.territory_id, cap.territory_id) = act.territory_id
        and coalesce(d.owner_id, cap.owner_id) = act.owner_id

)

select
    organization_id,
    fiscal_period_id,
    team_id,
    territory_id,
    owner_id,
    deals_won,
    deals_lost,
    deals_open,
    won_value,
    avg_deal_size,
    open_pipeline_value,
    qualified_leads,
    headcount,
    ramped_headcount,
    ramp_status,
    fully_loaded_cost_usd,
    activity_count,
    activity_seconds,
    case
        when (deals_won + deals_lost) = 0 then null
        else deals_won / nullif(deals_won + deals_lost, 0)
    end as win_rate,
    case
        when ramped_headcount = 0 then null
        else won_value / nullif(ramped_headcount, 0)
    end as productivity_per_ramped_head,
    case
        when deals_open = 0 then null
        else activity_count / nullif(deals_open, 0)
    end as activity_per_open_deal,
    /*
        Emitted as null on purpose. See the header: both need opportunity stage
        history, which does not exist yet. A feasibility score that silently
        assumed a cycle length would be a guess wearing a number's clothing.
    */
    cast(null as number(18, 4)) as cycle_days,
    cast(null as number(18, 4)) as stage_conversion_rate
from combined
where organization_id is not null
