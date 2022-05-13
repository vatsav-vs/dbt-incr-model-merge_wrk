{% macro uswhm_mcr() %}

{% set load_time = current_timestamp() %}


{% set del_updts %} 
    update {{this}} tgt
    set
    LOAD_TYPE= 'D',
    run_type='DELTA'
    from
    (
    select SALES_AGENT_ID from {{this}}
    minus
    select SALES_AGENT_ID from {{ ref('trnsfrm') }}
    )del
    where del.SALES_AGENT_ID=tgt.SALES_AGENT_ID;
{% endset %}
{% do run_query(del_updts) %}
{% do log("updated deleted records in target table", info=True) %}

{% set stats_tbl %}
    create table if not exists vatsav_db.hist_schema.stats_cmplx_proc (
	LOAD_DT TIMESTAMP_TZ(9),
	insertcount NUMBER(32,0),
	updatecount NUMBER(32,0),
    deletecount NUMBER(32,0)
);
{% endset %}
{% do run_query(stats_tbl) %}
{% do log("table created if not exists", info=True) %}

{% set insert_stats %}
    insert into vatsav_db.hist_schema.stats_cmplx_proc
    {% if is_incremental() %}
    --insert logic for incremental run
    select
    {{load_time}} as LOAD_DT,
    ins.*,upd.*,del.* from
    (select
    count(*) as insrt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'I'
	and LOADDATETIME >= (select max(LOAD_DT)as ts from vatsav_db.hist_schema.stats_cmplx_proc)
    )ins,
    (
    select
    count(*)as upt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'U'
	and LOADDATETIME >= (select max(LOAD_DT)as ts from vatsav_db.hist_schema.stats_cmplx_proc)
    )upd,
    (
    select
    count(*)as del_cnt 
    from
    {{this}}
    where LOAD_TYPE= 'D'
	and LOADDATETIME >= (select max(LOAD_DT)as ts from vatsav_db.hist_schema.stats_cmplx_proc)
    )del;

    {% else %}
    --insert logic for first run
    select
    {{load_time}} as LOAD_DT,
    ins.*,upd.*,del.* from
    (select
    count(*) as insrt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'I'
    )ins,
    (
    select
    count(*)as upt_cnt
    from
    {{this}}
    where LOAD_TYPE= 'U'
    )upd,
    (
    select
    count(*)as del_cnt 
    from
    {{this}}
    where LOAD_TYPE= 'D'
    )del;
    {% endif %}

{% endset %}
{% do run_query(insert_stats) %}
{% do log("inserted stats", info=True) %}

{% endmacro %}