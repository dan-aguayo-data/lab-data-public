{% macro wh_selector_models(dev_wh,sit_wh,uat_wh,prod_wh) %}

    {% if target.name.upper() == 'PROD' %}
       {% set wh = prod_wh %}
    {% elif target.name.upper() == 'SIT' %}
        {% set wh = sit_wh %}
    {% elif target.name.upper() == 'UAT' %}
        {% set wh = uat_wh %}
    {% else %}
        {% set wh = dev_wh %}
    {% endif %}
    {% do return(wh) %}
{% endmacro %}