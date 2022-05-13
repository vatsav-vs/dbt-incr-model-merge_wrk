{{
    config(
        materialized='incremental',
        transient= false,
        unique_key='sales_agent_id',
        post_hook = "{{uswhm_mcr()}}"
    )
}}

{% set load_time = current_timestamp() %}

{% if is_incremental() %}
with dlt_cols as (
    select
    tgt.SALES_AGENT_ID,
    'U' as load_type
    from
    {{this}} tgt
    inner join {{ ref('trnsfrm') }} src
    on src.SALES_AGENT_ID = tgt.SALES_AGENT_ID
    where src.zz_date >= (select max(loaddatetime) from {{ this }})
)
{% endif %}


select
    vw.SALES_AGENT_ID,
    NAME,
    CITY,
    SALEC_COMMISION_PCT,
     {{load_time}} as loaddatetime,
{% if is_incremental() %}
    coalesce(dlt.load_type,'I') as load_type,
{% else %}
    'I' as load_type,
{% endif %}
{% if is_incremental() %}
    'DELTA' as run_type
{% else %}
    'FULL' as run_type
{% endif %}
    from
{{ ref('trnsfrm') }} vw
{% if is_incremental() %}
    left join dlt_cols dlt
    on dlt.SALES_AGENT_ID=vw.SALES_AGENT_ID
  -- this filter will only be applied on an incremental run
  where vw.zz_date >= (select max(loaddatetime) from {{ this }})

{% endif %}
