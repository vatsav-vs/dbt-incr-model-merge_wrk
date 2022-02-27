{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id'
    )
}}

select
    sa_inc.*,'1' as update_check
from {{ source('sales', 'sales_agent_incr') }} sa_inc