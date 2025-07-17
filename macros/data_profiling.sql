{% macro generate_profiling_metrics(table_name, columns) %}
    {% set results = [] %}
    {% for col in columns %}
        select
            '{{ table_name }}' as table_name,
            '{{ col }}' as column_name,
            count(*) as total_rows,
            count(*) - count({{ col }}) as null_count,
            round((count(*) - count({{ col }})) * 100.0 / count(*), 2) as null_pct,
            count(distinct {{ col }}) as distinct_count
        from {{ ref(table_name) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
{% endmacro %}
