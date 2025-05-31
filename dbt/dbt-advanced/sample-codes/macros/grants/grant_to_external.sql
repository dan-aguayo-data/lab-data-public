{% macro grant_to_external(external_databases, external_schemas, roles, branch_name) %}
{%- if feature_branch(branch_name)  and target.name not in ["local"] -%}
  {% for database in external_databases %} 
    {% for schema in external_schemas %}
      {% for role in roles %}
        GRANT USAGE ON schema {{ database -}}.{{- schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL TABLES IN schema {{ database -}}.{{- schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL VIEWS IN schema {{ database -}}.{{- schema }} TO ROLE {{ role }};
      {% endfor %}
    {% endfor %}
  {% endfor %}
{%- endif -%}
{% endmacro %}
