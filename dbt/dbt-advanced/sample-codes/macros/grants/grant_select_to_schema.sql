{% macro grant_select_to_schema(schemas, roles, branch_name) %}
{%- if feature_branch(branch_name)  and target.name not in ["local"] -%}
  {% for schema in schemas %}
    {% for role in roles %}
      {%- if "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- else -%}     
        GRANT USAGE ON schema {{ schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL TABLES IN schema {{ schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL VIEWS IN schema {{ schema }} TO ROLE {{ role }};
      {%- endif -%}       
    {% endfor %}
  {% endfor %}
{%- endif -%}
{% endmacro %}
