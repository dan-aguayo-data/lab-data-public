{% macro run_flag_exclude_all() %}
  {% if target.name.upper() == 'PROD' or target.name.upper() == 'DEV' or target.name.upper() == 'UAT' %}
    {{ return(False) }}
  {% else %}
    {{ return(True) }}
  {% endif %}
{% endmacro %}