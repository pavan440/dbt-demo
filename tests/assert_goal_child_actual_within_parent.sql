/*
    A child goal's actual must never exceed its parent's.

    This guards a real defect: team and territory crosscut each other, so
    resolving actuals on a single owner axis produced children larger than their
    parents (a territory-scoped pod outscoring the team that contains it). The
    composite scope resolution in mart_goal_attainment fixes that; this test
    stops it silently regressing.

    Compared only where parent and child track the same metric — a marketing
    pipeline child under a revenue parent is measured in different units and is
    legitimately larger.
*/

select
    child.organization_id,
    child.goal_id as child_goal_id,
    parent.goal_id as parent_goal_id,
    child.metric_key,
    child.actual_value as child_actual,
    parent.actual_value as parent_actual,
    child.actual_value - parent.actual_value as excess
from {{ ref('mart_goal_attainment') }} as child
inner join {{ ref('mart_goal_attainment') }} as parent
    on  child.parent_goal_id = parent.goal_id
    and child.organization_id = parent.organization_id
where child.actual_value is not null
  and parent.actual_value is not null
  and child.metric_key = parent.metric_key
  and child.actual_value > parent.actual_value + 0.01
