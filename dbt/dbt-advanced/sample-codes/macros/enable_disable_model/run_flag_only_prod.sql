{% macro run_flag_prod() %}
  {% if target.name.upper() == 'PROD' %}
    {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}