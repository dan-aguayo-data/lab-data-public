{% macro resize_warehouses(warehouses) %}
    {% for wh in warehouses %}
        {% set parts = wh.split(":") %}
        {% set warehouse_name = parts[0] %}
        {% set new_size = parts[1] %}
        
        {{ log("Resizing warehouse " ~ warehouse_name ~ " to size " ~ new_size, info=True) }}
        
        {% set query %}
            ALTER WAREHOUSE {{ warehouse_name }} SET WAREHOUSE_SIZE = '{{ new_size }}';
        {% endset %}
        
        {% do run_query(query) %}
        
        {{ log("Warehouse " ~ warehouse_name ~ " successfully resized to " ~ new_size, info=True) }}
    {% endfor %}
{% endmacro %}



dbt run-operation resize_warehouses 
