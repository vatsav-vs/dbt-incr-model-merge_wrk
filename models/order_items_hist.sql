{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id',
        post_hook = ["update VATSAV_DB.hist_schema.sub_order_hist SET update_check = 0;
        "]
    )
}}

select
    *
from {{ ref('sub_order_hist') }}
where update_check=1