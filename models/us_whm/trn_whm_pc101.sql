
select
*
from {{ source('sales', 'sales_agent_incr') }}
