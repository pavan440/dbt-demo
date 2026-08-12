select
    organization_id,
    kpi_date,
    count(*) as row_count
from {{ ref('mart_tenant_daily_pipeline_health') }}
group by 1, 2
having count(*) > 1
