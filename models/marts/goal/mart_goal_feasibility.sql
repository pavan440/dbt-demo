{{
    config(
        materialized='table',
        tags=['goal', 'feasibility', 'mart']
    )
}}

/*
    Is this goal still reachable?

    Deliberately a coverage-and-capacity check rather than a trend comparison,
    because a trend comparison needs several closed periods and this warehouse
    currently holds one partial quarter. Coverage does not: it asks whether the
    pipeline that already exists, converted at the rate this scope actually
    converts, covers what is left of the target in the time remaining. That is
    computable today and is the more actionable question in-period anyway.

    What this is NOT yet: a probability. P(attain) needs a fitted distribution
    per driver, which needs history. The band below is a coverage band, and the
    column is named so nobody mistakes one for the other.

    Win rate resolves down an explicit fallback ladder and the rung used travels
    with the answer. A number justified by "peers in your industry" carries very
    different weight from one justified by "your own team's last 40 deals", and
    the person approving the goal needs to see which they are looking at.
*/

with attainment as (

    select *
    from {{ ref('mart_goal_attainment') }}

),

goal_scope as (

    select
        goal_id,
        organization_id,
        scope_team_id,
        scope_territory_id,
        scope_user_id
    from {{ ref('int_goal_tree') }}

),

tenant_profile as (

    select distinct
        organization_id,
        tenant_domain as industry,
        tenant_company_size as company_size
    from {{ ref('stg_silver_records_resolved') }}

),

drivers as (

    select *
    from {{ ref('int_driver_history') }}

),

-- Rung 1: the goal's own scope.
scope_drivers as (

    select
        g.goal_id,
        g.organization_id,
        sum(d.deals_won) as deals_won,
        sum(d.deals_lost) as deals_lost,
        sum(d.deals_open) as deals_open,
        sum(d.open_pipeline_value) as open_pipeline_value,
        avg(d.avg_deal_size) as avg_deal_size,
        sum(d.ramped_headcount) as ramped_headcount,
        sum(d.headcount) as headcount
    from goal_scope as g
    inner join drivers as d
        on  d.organization_id = g.organization_id
        and (g.scope_team_id is null or d.team_id = g.scope_team_id)
        and (g.scope_territory_id is null or d.territory_id = g.scope_territory_id)
        and (g.scope_user_id is null or d.owner_id = g.scope_user_id)
    group by 1, 2

),

-- Rung 2: the whole tenant.
org_rate as (

    select
        organization_id,
        sum(deals_won) as deals_won,
        sum(deals_lost) as deals_lost,
        sum(deals_won) / nullif(sum(deals_won) + sum(deals_lost), 0) as win_rate
    from drivers
    group by 1

),

-- Rung 3: peer tenants in the same industry. The multi-tenant benchmark asset.
peer_rate as (

    select
        p.industry,
        sum(d.deals_won) as deals_won,
        sum(d.deals_lost) as deals_lost,
        count(distinct d.organization_id) as cohort_tenants,
        sum(d.deals_won) / nullif(sum(d.deals_won) + sum(d.deals_lost), 0) as win_rate
    from drivers as d
    inner join tenant_profile as p
        on d.organization_id = p.organization_id
    group by 1

),

-- Rung 4: every tenant on the platform.
global_rate as (

    select
        sum(deals_won) / nullif(sum(deals_won) + sum(deals_lost), 0) as win_rate,
        sum(deals_won) + sum(deals_lost) as decided_deals
    from drivers

),

resolved as (

    select
        a.goal_id,
        a.organization_id,
        a.parent_goal_id,
        a.level,
        a.metric_key,
        a.fiscal_period_id,
        a.relation_type,
        a.status,
        a.confidence as planning_confidence,
        a.target_value,
        a.actual_value,
        a.attainment_pct,
        a.expected_attainment_pct,
        a.pacing_status,
        a.source_status,
        a.as_of_date,
        a.period_end_date,
        a.workdays_in_period,
        a.workday_index_in_period,
        a.workdays_in_period - a.workday_index_in_period as workdays_remaining,
        greatest(a.target_value - coalesce(a.actual_value, 0), 0) as remaining_to_target,
        p.industry,
        p.company_size,
        sd.open_pipeline_value,
        sd.avg_deal_size,
        sd.deals_open,
        sd.deals_won,
        sd.deals_lost,
        sd.ramped_headcount,
        sd.headcount,
        /*
            The ladder. Each rung needs a minimum sample or it hands down to the
            next; a win rate off three deals is noise wearing a percentage sign.
        */
        case
            when coalesce(sd.deals_won, 0) + coalesce(sd.deals_lost, 0) >= 10 then 'OWN_SCOPE'
            when coalesce(o.deals_won, 0) + coalesce(o.deals_lost, 0) >= 10 then 'OWN_TENANT'
            when pr.cohort_tenants >= 2 then 'PEER_COHORT'
            else 'GLOBAL_PRIOR'
        end as evidence_rung,
        case
            when coalesce(sd.deals_won, 0) + coalesce(sd.deals_lost, 0) >= 10
                then sd.deals_won / nullif(sd.deals_won + sd.deals_lost, 0)
            when coalesce(o.deals_won, 0) + coalesce(o.deals_lost, 0) >= 10 then o.win_rate
            when pr.cohort_tenants >= 2 then pr.win_rate
            else g.win_rate
        end as applied_win_rate,
        pr.cohort_tenants
    from attainment as a
    left join goal_scope as gs
        on a.goal_id = gs.goal_id and a.organization_id = gs.organization_id
    left join scope_drivers as sd
        on a.goal_id = sd.goal_id and a.organization_id = sd.organization_id
    left join tenant_profile as p
        on a.organization_id = p.organization_id
    left join org_rate as o
        on a.organization_id = o.organization_id
    left join peer_rate as pr
        on p.industry = pr.industry
    cross join global_rate as g

),

scored as (

    select
        *,
        open_pipeline_value * applied_win_rate as expected_from_pipeline,
        case
            when remaining_to_target <= 0 then null
            when coalesce(open_pipeline_value, 0) = 0 then 0
            else (open_pipeline_value * applied_win_rate) / remaining_to_target
        end as coverage_ratio,
        case
            when coalesce(open_pipeline_value, 0) = 0 then null
            else remaining_to_target / open_pipeline_value
        end as required_win_rate,
        case
            when coalesce(avg_deal_size, 0) = 0 then null
            else ceil(remaining_to_target / avg_deal_size)
        end as deals_needed
    from resolved

)

select
    goal_id,
    organization_id,
    parent_goal_id,
    level,
    metric_key,
    fiscal_period_id,
    relation_type,
    status,
    planning_confidence,
    as_of_date,
    workdays_remaining,
    target_value,
    actual_value,
    remaining_to_target,
    open_pipeline_value,
    deals_open,
    avg_deal_size,
    deals_needed,
    ramped_headcount,
    headcount,
    applied_win_rate,
    required_win_rate,
    expected_from_pipeline,
    coverage_ratio,
    evidence_rung,
    cohort_tenants,
    industry,
    company_size,
    /*
        The binding constraint: the single thing most responsible for the gap.
        A leader can act on "your pipeline is short by $2.1M"; they cannot act
        on "coverage is 0.6".
    */
    case
        when source_status <> 'COMPUTED' then 'NOT_ASSESSABLE'
        when remaining_to_target <= 0 then 'NONE_TARGET_MET'
        when coalesce(open_pipeline_value, 0) = 0 then 'NO_PIPELINE'
        when required_win_rate > 1 then 'PIPELINE_SHORTFALL'
        when required_win_rate > applied_win_rate * 1.5 then 'WIN_RATE_STRETCH'
        /*
            Capacity is only the binding constraint when coverage is also weak.
            Reporting "no ramped capacity" on a goal sitting at 7x coverage
            sends a leader hiring against a problem they do not have.
        */
        when coalesce(ramped_headcount, 0) = 0 and coverage_ratio < 1.0
            then 'NO_RAMPED_CAPACITY'
        else 'ON_PLAN'
    end as binding_constraint,
    /*
        A coverage band, not a probability. Naming it feasibility_band rather
        than p_attain keeps the distinction visible until enough closed periods
        exist to fit real driver distributions.
    */
    case
        when source_status <> 'COMPUTED' then 'NOT_ASSESSABLE'
        when remaining_to_target <= 0 then 'ATTAINED'
        when coverage_ratio is null then 'NOT_ASSESSABLE'
        when coverage_ratio >= 2.0 then 'CONSERVATIVE'
        when coverage_ratio >= 1.0 then 'REALISTIC'
        when coverage_ratio >= 0.5 then 'STRETCH'
        else 'UNREALISTIC'
    end as feasibility_band
from scored
