/*hist_schema*/
{% macro qs_dcunit(ENV_NAME,PROJECT_NAME,user_or_role) %}

{% set sql1 %}
    create table IF NOT EXISTS {{ ENV_NAME }}_{{ PROJECT_NAME }}.custom.access_mgnt (
    user_or_role string,
    view_name string,
    active char(1)
    );
{% endset %}
{% do run_query(sql1) %}
{% do log("table created", info=True) %}

{% set sql2%}
    INSERT INTO {{ ENV_NAME }}_{{ PROJECT_NAME }}.custom.access_mgnt VALUES ('{{ user_or_role }}','"{{ENV_NAME}}_DISTRIBUTE"."{{PROJECT_NAME}}"."V_DCUNITEST"','X');
{% endset %}
{% do run_query(sql2) %}
{% do log("inserted data", info=True) %}


{% set sql4 %}
    call common.distribute.prc_distribute_view_processing_{{ ENV_NAME }}_{{ PROJECT_NAME }}();
{% endset %}
{% do run_query(sql4) %}
{% do log("Triggered Procedure 2 for Distribution", info=True) %}

{% endmacro %}