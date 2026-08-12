select
    organization_id,
    record_id,
    record_date,
    count(*) as row_count
from {{ ref('int_record_daily_activity') }}
group by 1, 2, 3
having count(*) > 1
