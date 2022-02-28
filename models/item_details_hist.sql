{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id'
    )
}}

select
    *
from {{ ref('sub_order_hist') }}
where update_check=1