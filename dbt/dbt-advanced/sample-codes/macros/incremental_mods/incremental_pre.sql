{% macro incremental_pre() %}

{% if execute %}

    {% if is_incremental() %}

        {% set model_name = this.name|upper %}
        {% set target_name = target.name|upper %}
        {% do log("Model Name: " ~ model_name, info=True) %}
        {% do log("Target Name: " ~ target_name, info=True) %}

        {% set check_sql %}
            select 1 chck from CONTROL_TABLE
            where TABLE_NAME='{{model_name}}' and ENVIRONMENT='{{target_name}}' and PROJECT_NAME='{{var('share_dbt_project_name')}}'
        {% endset %}

        {% set check_results = run_query(check_sql).columns[0][0] %}

        {% if check_results %}
            {% do log("Table exists in Incremental Log.", info=True) %}
        {% else %}
            {% do log("Table does not exist in Incremental Log. Creating...", info=True) %}

            {% set incremental_record %}
                INSERT INTO CONTROL_TABLE
                VALUES ('{{var('share_dbt_project_name')}}', '{{target_name}}', '{{model_name}}', sysdate(), convert_timezone('UTC', 'Australia/Sydney', sysdate()));
            {% endset %}

            {% do run_query(incremental_record) %}
        {% endif %}

    {% else %}
        {% do log("Full Load Executed. No Pre-Hook Log Record Inserted", info=True) %}
    {% endif %}

{% endif %}
  
{% endmacro %}