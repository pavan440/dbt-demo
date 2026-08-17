{{
    config(
        materialized='table',
        tags=['goal', 'mart']
    )
}}

/*
    One row per goal per period: target, actual, attainment, and pacing against
    the period's expected curve. This is the table the orchestrator and every
    department agent read; nothing downstream should compute attainment itself.

    as_of_date defaults to today and can be pinned with the goal_as_of_date var
    for reproducible runs and backtests.
*/

{% set as_of_date = var('goal_as_of_date', none) %}

with as_of as (

    select
        {% if as_of_date %}
        to_date('{{ as_of_date }}') as as_of_date
        {% else %}
        current_date() as as_of_date
        {% endif %}

),

goals as (

    select *
    from {{ ref('int_goal_tree') }}

),

period_position as (

    select
        c.fiscal_period_id,
        c.period_start_date,
        c.period_end_date,
        c.days_in_period,
        c.workdays_in_period,
        c.day_index_in_period,
        c.workday_index_in_period,
        c.expected_attainment_pct_linear,
        a.as_of_date
    from {{ ref('dim_fiscal_calendar') }} as c
    cross join as_of as a
    where c.calendar_date = least(a.as_of_date, c.period_end_date)

),

/*
    Which metrics have a source at all, per tenant and period.

    This separates two situations a single null would otherwise conflate:
    a metric with no source table anywhere (NO_SOURCE — a data gap), and a
    metric that is perfectly measurable but whose goal scope matched no records
    (NO_SCOPE_MATCH — either a genuine zero or a scope defined against people or
    territories the facts do not agree with). "This rep cannot be measured" and
    "this rep has sold nothing" are very different conversations to walk into.
*/
measurable as (

    select distinct
        organization_id,
        fiscal_period_id,
        metric_key
    from {{ ref('int_metric_actual') }}

),

/*
    Composite scope resolution. A goal constrains any subset of team, territory
    and owner; a null scope column means "unconstrained on that axis". Summing
    int_metric_actual under that predicate guarantees a child goal's actual is a
    true subset of its parent's, because the child's scope is strictly narrower.
*/
actuals as (

    select
        g.goal_id,
        g.organization_id,
        sum(a.actual_value) as actual_value,
        max(a.source_status) as source_status,
        max(a.source_caveat) as source_caveat,
        count(*) as contributing_rows
    from {{ ref('int_goal_tree') }} as g
    inner join {{ ref('int_metric_actual') }} as a
        on  a.organization_id = g.organization_id
        and a.fiscal_period_id = g.fiscal_period_id
        and a.metric_key = g.metric_key
        and (g.scope_team_id is null or a.team_id = g.scope_team_id)
        and (g.scope_territory_id is null or a.territory_id = g.scope_territory_id)
        and (g.scope_user_id is null or a.owner_id = g.scope_user_id)
    group by 1, 2

),

joined as (

    select
        g.goal_id,
        g.organization_id,
        g.parent_goal_id,
        g.root_goal_id,
        g.goal_path,
        g.depth,
        g.level,
        g.owner_type,
        g.owner_id,
        g.metric_key,
        g.fiscal_period_id,
        g.target_value,
        g.currency,
        g.split_method,
        g.relation_type,
        g.confidence,
        g.status,
        g.version,
        g.rollup_status,
        g.rollup_variance,
        g.rollup_variance_pct,
        p.as_of_date,
        p.period_start_date,
        p.period_end_date,
        p.workdays_in_period,
        p.workday_index_in_period,
        p.expected_attainment_pct_linear,
        case
            when m.metric_key is null then null
            else coalesce(act.actual_value, 0)
        end as actual_value,
        coalesce(act.contributing_rows, 0) as contributing_rows,
        case
            when m.metric_key is null then 'NO_SOURCE'
            when act.goal_id is null then 'NO_SCOPE_MATCH'
            else 'COMPUTED'
        end as source_status,
        act.source_caveat
    from goals as g
    left join period_position as p
        on g.fiscal_period_id = p.fiscal_period_id
    left join measurable as m
        on  g.organization_id = m.organization_id
        and g.fiscal_period_id = m.fiscal_period_id
        and g.metric_key = m.metric_key
    left join actuals as act
        on  g.goal_id = act.goal_id
        and g.organization_id = act.organization_id

),

scored as (

    select
        *,
        case
            when source_status = 'NO_SOURCE' then null
            when target_value = 0 then null
            else actual_value / target_value
        end as attainment_pct,
        case
            when as_of_date > period_end_date then 1.0
            when as_of_date < period_start_date then 0.0
            else expected_attainment_pct_linear
        end as expected_attainment_pct
    from joined

)

select
    goal_id,
    organization_id,
    parent_goal_id,
    root_goal_id,
    goal_path,
    depth,
    level,
    owner_type,
    owner_id,
    metric_key,
    fiscal_period_id,
    period_start_date,
    period_end_date,
    as_of_date,
    workdays_in_period,
    workday_index_in_period,
    target_value,
    currency,
    actual_value,
    contributing_rows,
    attainment_pct,
    expected_attainment_pct,
    case
        when attainment_pct is null or expected_attainment_pct in (0, null) then null
        else attainment_pct / expected_attainment_pct
    end as pace_ratio,
    target_value - coalesce(actual_value, 0) as remaining_to_target,
    split_method,
    relation_type,
    confidence,
    status,
    version,
    rollup_status,
    rollup_variance,
    rollup_variance_pct,
    source_status,
    source_caveat,
    /*
        NOT_MEASURABLE is a first-class outcome, not a failure. A goal whose
        metric has no source must never be reported as AT_RISK — that reads as
        a business problem when it is a data gap, and it is the single fastest
        way to lose a stakeholder's trust in the whole system.
    */
    case
        when source_status = 'NO_SOURCE' then 'NOT_MEASURABLE'
        when relation_type = 'GUARDRAIL' then 'GUARDRAIL'
        when source_status = 'NO_SCOPE_MATCH' then 'NO_SCOPE_MATCH'
        when as_of_date < period_start_date then 'NOT_STARTED'
        when attainment_pct >= 1.0 then 'ATTAINED'
        when attainment_pct >= expected_attainment_pct * 1.05 then 'AHEAD'
        when attainment_pct >= expected_attainment_pct * 0.90 then 'ON_TRACK'
        when attainment_pct >= expected_attainment_pct * 0.70 then 'BEHIND'
        else 'AT_RISK'
    end as pacing_status
from scored
