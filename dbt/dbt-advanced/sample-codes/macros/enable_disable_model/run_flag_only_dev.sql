{% macro run_flag_dev() %}
  {% if target.name.upper() == 'DEV' %}
    {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}