{% macro run_flag_prod_dev() %}
  {% if target.name.upper() == 'PROD' or target.name.upper() == 'DEV' %}
    {{ return(True) }}
  {% else %}
    {{ return(False) }}
  {% endif %}
{% endmacro %}


