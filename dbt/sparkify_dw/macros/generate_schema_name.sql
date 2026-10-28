{# Use the folder schema as-is (staging / marts / kpi / ref) instead of dbt's default
   "<target_schema>_<custom_schema>" naming, so Metabase sees clean schema names. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
