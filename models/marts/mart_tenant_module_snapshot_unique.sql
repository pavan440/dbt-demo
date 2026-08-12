select
    organization_id,
    module_key,
    count(*) as row_count
from {{ ref('mart_tenant_module_snapshot') }}
group by 1, 2
having count(*) > 1
