{% macro incremental_purge() %}

{% if execute %}

    {% set check_day %}
        SELECT extract(day from sysdate());
    {% endset %}

    {% set check_day_results = run_query(check_day).columns[0][0] %}

    {% if check_day_results == 3 %} --Purge on 3rd day of the current month (3 days of logs retained after purge)

        {% do log("Purging previous months records from Incremental Log.", info=True) %}

        {% set incremental_delete %}
        delete from CONTROL_TABLE
        where DATE(LATEST_UPDATE_UTC_TIME) < DATE_TRUNC('month', DATE(sysdate()));
        {% endset %}

        {% do run_query(incremental_delete) %}

    {% else %}

        {% do log("Incremental Log not purged.", info=True) %}

    {% endif %}

{% endif %}  

{% endmacro %}