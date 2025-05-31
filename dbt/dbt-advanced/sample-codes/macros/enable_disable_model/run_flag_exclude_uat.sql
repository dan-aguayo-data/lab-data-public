{% macro run_flag_excl_uat() %}
  {% if target.name.upper() == 'UAT' %}
    {% do return(False) %}
  {% else %}
    {% do return(True) %}
  {% endif %}
{% endmacro %}