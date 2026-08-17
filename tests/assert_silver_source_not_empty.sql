/*
    Fails when a Silver staging model resolves to zero rows.

    This guards a failure mode that is worse than a broken build: a green one.
    The source schema is chosen by the wayplorer_landing_schema var, which
    defaults to LANDING, while the load macros actually write to target.schema.
    Point it at the wrong schema and every model still builds, every existing
    test still passes, and every mart is silently full of zeros - goal attainment
    reads 0%, every goal looks AT_RISK, and nothing anywhere says why.

    An empty Silver source is never a legitimate state for this project, so
    assert it directly rather than trusting the var to be set correctly.
*/

select
    'stg_silver_records_resolved' as model_name,
    count(*) as row_count
from {{ ref('stg_silver_records_resolved') }}
having count(*) = 0

union all

select
    'stg_silver_activity_resolved' as model_name,
    count(*) as row_count
from {{ ref('stg_silver_activity_resolved') }}
having count(*) = 0

union all

select
    'stg_silver_kpi_daily_summary' as model_name,
    count(*) as row_count
from {{ ref('stg_silver_kpi_daily_summary') }}
having count(*) = 0
