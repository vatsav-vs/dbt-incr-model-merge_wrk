{% macro TRY_UPDT(tbl) %}
{% set sql %}
    CREATE PROCEDURE VATSAV_DB.hist_schema.UPDT_INCR_COL(tbl varchar) 
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_cmd = 'update '+ TBL +' SET update_check = 0;'
    try{
    snowflake.execute(
        {sqlText: sql_cmd}
    );
    return "success";
    }
    catch (err) {
    return "failed: " + err;
    }
    $$;
{% endset %}

{% do run_query(sql) %}
{% do log("procedure created", info=True) %}
{% endmacro %}