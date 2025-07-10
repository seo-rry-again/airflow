{% macro generate_schema_name(custom_schema_name, node_schema) -%}

    {{ custom_schema_name | trim }}

{%- endmacro %}
