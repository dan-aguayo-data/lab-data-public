{% macro get_current_sydney_hour() %}
  {%- set now = modules.datetime.datetime.now(modules.pytz.timezone("Australia/Sydney")) -%}
  {{ log("Current Sydney hour (compile time): " ~ now.hour, info=True) }}
  {{ return(now.hour) }}
{% endmacro %}
