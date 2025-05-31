{% macro full_refresh_get_wh(overnight_wh, day_wh) %}
    {% set current_hour = get_current_sydney_hour() %}

    {% if current_hour >= 23 or current_hour < 3 %}
        {% do return(overnight_wh) %}
    {% else %}
        {% do return(day_wh) %}
    {% endif %}
{% endmacro %}
