-- macros/apply_crime_dq_checks.sql

{% macro apply_crime_dq_checks(column_name, rule) %}
    {% if rule == 'geo_id_check' %}
        case
            when {{ column_name }} like '%/%' then 'PASS'
            else 'FAIL'
        end

    {% elif rule == 'clean_string' %}
        regexp_replace(trim({{ column_name }}), '[()<>?]', '')

    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}

-- macros/dq_check_geo_column.sql
{% macro dq_check_geo_column(col, type) %}
    {% if type == 'geo_name' %}
        case when regexp_like({{ col }}, '^[a-zA-Z ]+$') then 'PASS' else 'FAIL' end
    {% else %}
        'PASS'
    {% endif %}
{% endmacro %}

