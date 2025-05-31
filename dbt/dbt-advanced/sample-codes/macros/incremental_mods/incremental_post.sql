{% macro incremental_post() %}

{% if execute %}

  {% set model_name = this.name|upper %}
  {% set target_name = target.name|upper %}

  {% set incremental_record_insert %}

    {% do log("Inserting new Incremental Log record for Model: " ~ model_name, info=True) %}

    INSERT INTO CONTROL_TABLE
    VALUES ('{{var('share_dbt_project_name')}}', '{{target_name}}', '{{model_name}}', sysdate(), convert_timezone('UTC', 'Australia/Sydney', sysdate()));

  {% endset %}

  {% do run_query(incremental_record_insert) %}

{% endif %}
  
{% endmacro %}