-- Not Null Check
{% macro dq_check_not_null(column) %}
    case when {{ column }} is null then 'FAIL_NULL' else 'PASS' end
{% endmacro %}

-- Valid Email Format
{% macro dq_check_email_format(column) %}
    case 
        when {{ column }} is null then 'FAIL_NULL'
        when not regexp_like({{ column }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') then 'FAIL_INVALID_EMAIL'
        else 'PASS' 
    end
{% endmacro %}

-- Numeric Range Check
{% macro dq_check_numeric_range(column_name, min_value, max_value) %}
    case 
        when try_to_number({{ column_name }}) is null then 'FAIL_NON_NUMERIC'
        when try_to_number({{ column_name }}) < {{ min_value }} or try_to_number({{ column_name }}) > {{ max_value }} then 'FAIL_RANGE'
        else 'PASS'
    end
{% endmacro %}


-- Duplicate Check (based on name + email)
{% macro dq_check_duplicates(name_col, email_col) %}
    {{ return(
        "count(*) over (partition by " ~ name_col ~ ", " ~ email_col ~ ") > 1"
    ) }}
{% endmacro %}

