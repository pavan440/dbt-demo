{{
    config(
        materialized='table',
        tags=['goal']
    )
}}

/*
    Flattens the self-referencing goal hierarchy into one row per goal, carrying
    depth, root, and the full ancestry path.

    Tenant isolation: organization_id is part of the recursive join predicate.
    Goal IDs are only unique within a tenant, so joining on parent_goal_id alone
    would splice one tenant's subtree onto another tenant's parent.
*/

with goals as (

    select *
    from {{ ref('goal') }}
    where status in ('APPROVED', 'LOCKED', 'PROPOSED')

),

goal_tree as (

    select
        goal_id,
        organization_id,
        parent_goal_id,
        level,
        owner_type,
        owner_id,
        scope_team_id,
        scope_territory_id,
        scope_user_id,
        metric_key,
        fiscal_period_id,
        target_value,
        currency,
        split_method,
        relation_type,
        confidence,
        status,
        version,
        1 as depth,
        goal_id as root_goal_id,
        cast(goal_id as varchar(4000)) as goal_path
    from goals
    where parent_goal_id is null

    union all

    select
        child.goal_id,
        child.organization_id,
        child.parent_goal_id,
        child.level,
        child.owner_type,
        child.owner_id,
        child.scope_team_id,
        child.scope_territory_id,
        child.scope_user_id,
        child.metric_key,
        child.fiscal_period_id,
        child.target_value,
        child.currency,
        child.split_method,
        child.relation_type,
        child.confidence,
        child.status,
        child.version,
        parent.depth + 1 as depth,
        parent.root_goal_id,
        cast(parent.goal_path || ' > ' || child.goal_id as varchar(4000)) as goal_path
    from goals as child
    inner join goal_tree as parent
        on child.parent_goal_id = parent.goal_id
       and child.organization_id = parent.organization_id

),

/*
    Reconciliation: only SUM children roll up to the parent. DERIVED children
    (marketing pipeline, computed through a coverage ratio) and GUARDRAIL
    children (finance and retention thresholds) constrain the plan rather than
    contributing to it, so including them would make every parent look unbalanced.
*/
child_rollup as (

    select
        organization_id,
        parent_goal_id,
        fiscal_period_id,
        sum(case when relation_type = 'SUM' then target_value end) as sum_of_children,
        count_if(relation_type = 'SUM') as sum_child_count,
        count_if(relation_type = 'DERIVED') as derived_child_count,
        count_if(relation_type = 'GUARDRAIL') as guardrail_child_count
    from goal_tree
    where parent_goal_id is not null
    group by 1, 2, 3

)

select
    t.goal_id,
    t.organization_id,
    t.parent_goal_id,
    t.root_goal_id,
    t.goal_path,
    t.depth,
    t.level,
    t.owner_type,
    t.owner_id,
    t.scope_team_id,
    t.scope_territory_id,
    t.scope_user_id,
    t.metric_key,
    t.fiscal_period_id,
    t.target_value,
    t.currency,
    t.split_method,
    t.relation_type,
    t.confidence,
    t.status,
    t.version,
    coalesce(r.sum_child_count, 0) as sum_child_count,
    coalesce(r.derived_child_count, 0) as derived_child_count,
    coalesce(r.guardrail_child_count, 0) as guardrail_child_count,
    r.sum_of_children,
    case
        when r.sum_of_children is null then null
        else r.sum_of_children - t.target_value
    end as rollup_variance,
    case
        when r.sum_of_children is null then null
        when t.target_value = 0 then null
        else (r.sum_of_children - t.target_value) / t.target_value
    end as rollup_variance_pct,
    case
        when r.sum_of_children is null then 'NO_SUM_CHILDREN'
        when abs(r.sum_of_children - t.target_value) <= 0.005 * t.target_value then 'BALANCED'
        when r.sum_of_children > t.target_value then 'OVER_ALLOCATED'
        else 'UNDER_ALLOCATED'
    end as rollup_status
from goal_tree as t
left join child_rollup as r
    on t.goal_id = r.parent_goal_id
   and t.organization_id = r.organization_id
   and t.fiscal_period_id = r.fiscal_period_id
