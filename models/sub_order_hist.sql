{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id',
        pre_hook = ["call VATSAV_DB.hist_schema.UPDT_INCR_COL('VATSAV_DB.hist_schema.sub_order_hist');
        "]
    )
}}

select
    sa_inc.*,'1' as update_check
from {{ source('sales', 'sales_agent_incr') }} sa_inc