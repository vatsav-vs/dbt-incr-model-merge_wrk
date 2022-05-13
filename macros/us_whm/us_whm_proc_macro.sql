{% macro uswhm_mcr() %}

{% set load_time = current_timestamp() %}


{% set del_updts %} 
    update {{this}} tgt
    set
    LOAD_TYPE= 'D',
    run_type='DELTA',
    LOADDATETIME=del.ldt
    from
    (
    select del_A.*,del_b.* from
    (
        select SALES_AGENT_ID from {{this}}
        where LOAD_TYPE !='D'
        minus
        select SALES_AGENT_ID from {{ ref('trn_whm_pc101') }}
    )del_A,
    (select max(LOADDATETIME) as ldt from {{this}})del_b
    )del
    
    where del.SALES_AGENT_ID=tgt.SALES_AGENT_ID;
{% endset %}
{% do run_query(del_updts) %}
{% do log("updated deleted records in target table", info=True) %}

{% set stats_tbl %}
    create table if not exists  {{ env_var('DBT_DB_NAME') }}.hist_schema.uswhm_exec_time_log_pc101 (
	LOAD_DT TIMESTAMP_TZ(9),
	insertcount NUMBER(32,0),
	updatecount NUMBER(32,0),
    deletecount NUMBER(32,0)
);
{% endset %}
{% do run_query(stats_tbl) %}
{% do log("table created if not exists", info=True) %}

{% set insert_stats %}
    insert into  {{ env_var('DBT_DB_NAME') }}.hist_schema.uswhm_exec_time_log_pc101
    select
    {{load_time}} as LOAD_DT,
    ins.*,upd.*,del.* from
    (select
    count(*) as insrt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'I'
	{% if is_incremental() %}
    --insert logic for incremental run
	and LOADDATETIME >= (select max(LOAD_DT)as ts from  {{ env_var('DBT_DB_NAME') }}.hist_schema.uswhm_exec_time_log_pc101)
	{% endif %}	
    )ins,
    (
    select
    count(*)as upt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'U'
	{% if is_incremental() %}
    --insert logic for incremental run
	and LOADDATETIME >= (select max(LOAD_DT)as ts from  {{ env_var('DBT_DB_NAME') }}.hist_schema.uswhm_exec_time_log_pc101)
	{% endif %}
    )upd,
    (
    select
    count(*)as del_cnt 
    from
    {{this}}
    where LOAD_TYPE= 'D'
	{% if is_incremental() %}
    --insert logic for incremental run
	and LOADDATETIME >= (select max(LOAD_DT)as ts from  {{ env_var('DBT_DB_NAME') }}.hist_schema.uswhm_exec_time_log_pc101)
	{% endif %}
    )del;
    

{% endset %}
{% do run_query(insert_stats) %}
{% do log("inserted stats", info=True) %}

{% endmacro %}