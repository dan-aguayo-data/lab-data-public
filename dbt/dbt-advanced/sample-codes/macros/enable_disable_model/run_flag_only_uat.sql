{% macro run_flag_uat() %}
  {% if target.name.upper() == 'UAT' %}
    {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}