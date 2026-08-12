select
    organization_id,
    kpi_date,
    count(*) as row_count
from {{ ref('int_organization_daily_scorecard') }}
group by 1, 2
having count(*) > 1
