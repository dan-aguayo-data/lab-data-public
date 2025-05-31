{% macro grant_permission_to_schema(schemas, roles, branch_name) %}
{%- if feature_branch(branch_name)  and target.name not in ["local"] -%}
  {% for schema in schemas %}
    {% for role in roles %}
      {%- if "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- elif "CONDITION_HERE" in schema -%}
      {%- else -%}    
        GRANT ALL ON schema {{ schema }} TO ROLE {{ role }};
        GRANT ALL ON ALL TABLES IN schema {{ schema }} TO ROLE {{ role }};
        GRANT ALL ON ALL VIEWS IN schema {{ schema }} TO ROLE {{ role }};
      {%- endif -%}      
    {% endfor %}
  {% endfor %}
{%- endif -%}
{% endmacro %}
