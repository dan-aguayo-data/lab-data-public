{% macro resize_warehouse(warehouse_name, new_size) %}
    {{ log("Resizing warehouse " ~ warehouse_name ~ " to " ~ new_size, info=True) }}
    {% set query %}
        ALTER WAREHOUSE {{ warehouse_name }} SET WAREHOUSE_SIZE = '{{ new_size }}';
    {% endset %}
    {% do run_query(query) %}
    {{ log("Warehouse " ~ warehouse_name ~ " resized to " ~ new_size, info=True) }}
{% endmacro %}


