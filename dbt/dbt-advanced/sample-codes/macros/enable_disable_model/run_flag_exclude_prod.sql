{% macro run_flag_excl_prod() %}
  {% if target.name.upper() == 'PROD' %}
    {% do return(False) %}
  {% else %}
    {% do return(True) %}
  {% endif %}
{% endmacro %}