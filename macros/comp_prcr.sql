{% macro post_updts_and_stats() %}

{% set sql1 %}
    create stream if not exists vatsav_db.hist_schema.proc_strm on table {{this}} ;
{% endset %}
{% do run_query(sql1) %}
{% do log("stream created if not exists", info=True) %}

{% set sql2 %}
    create table if not exists vatsav_db.hist_schema.stats_cmplx_proc as
    select current_timestamp() as load_dt,a.cnt INSERT_CNT,0 as UPDT_CNT
    from
    (select count(*) as cnt from {{this}})a;
{% endset %}
{% do run_query(sql2) %}
{% do log("table created if not exists", info=True) %}

{% set sql3 %}
    INSERT INTO vatsav_db.hist_schema.stats_cmplx_proc
SELECT * FROM
(
SELECT current_timestamp(),A.*,B.*
FROM
(select
count(*)as insert_cnt from VATSAV_DB.hist_schema.proc_strm
where METADATA$ACTION != 'DELETE'
and METADATA$ISUPDATE = 'FALSE')A,
(select
count(*)as UPDT_CNT from VATSAV_DB.hist_schema.proc_strm
where METADATA$ACTION != 'DELETE'
and METADATA$ISUPDATE = 'TRUE')B)
{% endset %}
{% do run_query(sql3) %}
{% do log("inserted stats", info=True) %}

{% set sql4 %}
    update {{this}} tgt
set
LOAD_TYPE= 'U'
from
(select * from {{ source('sales', 'sales_agent_updt_str') }}
where METADATA$ACTION != 'DELETE'
and METADATA$ISUPDATE = 'TRUE') ut
where ut.SALES_AGENT_ID=tgt.SALES_AGENT_ID
{% endset %}
{% do run_query(sql4) %}
{% do log("updated records", info=True) %}


{% set sql5 %}
update {{this}} tgt
set
LOAD_TYPE= 'D'
from
(
select SALES_AGENT_ID from {{this}}
minus
select SALES_AGENT_ID from {{ source('sales', 'sales_agent_incr') }}
  )del
where del.SALES_AGENT_ID=tgt.SALES_AGENT_ID;

{% endset %}
{% do run_query(sql5) %}
{% do log("updated deleted records", info=True) %}

{% endmacro %}