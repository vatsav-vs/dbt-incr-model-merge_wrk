{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id',
        post_hook = [" {{post_updts_and_stats()}};"]
    )
}}

{% set load_time = current_timestamp() %}

select
    SALES_AGENT_ID,
    NAME,
    CITY,
    SALEC_COMMISION_PCT,
     {{load_time}} as sys_ts,
    'I' as load_type
from {{ source('sales', 'sales_agent_str') }}
where METADATA$ACTION != 'DELETE'
and METADATA$ISUPDATE = 'FALSE'