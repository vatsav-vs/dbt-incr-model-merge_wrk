{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id'
    )
}}

select
    *
from {{ source('sales', 'sales_agent_incr') }}