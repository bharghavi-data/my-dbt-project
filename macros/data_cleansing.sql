{% macro clean_column(col_name, to_lower=false, cast_type="", remove_special_chars=false) %}
    {%- set cleaned = "trim(" ~ col_name ~ "::string)" -%}

    {%- if remove_special_chars %}
        {%- set cleaned = "regexp_replace(" ~ cleaned ~ ", '[^a-zA-Z0-9@._]', '')" -%}
    {%- endif %}

    {%- if to_lower %}
        {%- set cleaned = "lower(" ~ cleaned ~ ")" -%}
    {%- endif %}

    -- Clean NULL strings and blanks
    {%- set cleaned = "nullif(" ~ cleaned ~ ", '')" -%}
    {%- set cleaned = "nullif(" ~ cleaned ~ ", 'NULL')" -%}
    {%- set cleaned = "nullif(" ~ cleaned ~ ", 'null')" -%}
    {%- set cleaned = "nullif(" ~ cleaned ~ ", 'N/A')" -%}

    {%- if cast_type != "" %}
        {%- set cleaned = "try_cast(" ~ cleaned ~ " as " ~ cast_type ~ ")" -%}
    {%- endif %}

    {{ cleaned }}
{% endmacro %}


{% macro clean_email(col_name) %}
    lower(
        regexp_replace(
            regexp_replace(trim({{ col_name }}), '[^a-zA-Z0-9@._]', ''),  -- keep only safe characters
            '@{2,}', '@'  -- replace multiple @ with one
        )
    )
{% endmacro %}

{% macro clean_date_column(col_name) %}
    case
        -- Pattern 1: 'YYYY-MM-DD'
        when try_to_date({{ col_name }}, 'YYYY-MM-DD') is not null 
            then to_char(try_to_date({{ col_name }}, 'YYYY-MM-DD'), 'DD-MM-YYYY')

        -- Pattern 2: 'DD/MM/YYYY'
        when try_to_date({{ col_name }}, 'DD/MM/YYYY') is not null 
            then to_char(try_to_date({{ col_name }}, 'DD/MM/YYYY'), 'DD-MM-YYYY')

        -- Pattern 3: 'YYYY/MM/DD'
        when try_to_date({{ col_name }}, 'YYYY/MM/DD') is not null 
            then to_char(try_to_date({{ col_name }}, 'YYYY/MM/DD'), 'DD-MM-YYYY')

        -- Pattern 4: 'DD-MM-YYYY'
        when try_to_date({{ col_name }}, 'DD-MM-YYYY') is not null 
            then to_char(try_to_date({{ col_name }}, 'DD-MM-YYYY'), 'DD-MM-YYYY')

        -- Pattern 5: 'YYYY.MM.DD'
        when try_to_date({{ col_name }}, 'YYYY.MM.DD') is not null 
            then to_char(try_to_date({{ col_name }}, 'YYYY.MM.DD'), 'DD-MM-YYYY')

        -- Pattern 6: 'DD-Mon-YYYY'
        when try_to_date({{ col_name }}, 'DD-Mon-YYYY') is not null 
            then to_char(try_to_date({{ col_name }}, 'DD-Mon-YYYY'), 'DD-MM-YYYY')

        -- Pattern 7: 'Mon DD, YYYY'
        when try_to_date({{ col_name }}, 'Mon DD, YYYY') is not null 
            then to_char(try_to_date({{ col_name }}, 'Mon DD, YYYY'), 'DD-MM-YYYY')

        -- Pattern 8: 'MM/DD/YYYY'
        when try_to_date({{ col_name }}, 'MM/DD/YYYY') is not null 
            then to_char(try_to_date({{ col_name }}, 'MM/DD/YYYY'), 'DD-MM-YYYY')

        -- Pattern 9: Compact numeric (e.g., 20240612)
        when try_to_date({{ col_name }}, 'YYYYMMDD') is not null 
            then to_char(try_to_date({{ col_name }}, 'YYYYMMDD'), 'DD-MM-YYYY')

        else null
    end
{% endmacro %}

{% macro remove_duplicates(relation, key_columns, order_column='last_updated', prefer_column='name') %}
(
    select *
    from (
        select *,
            row_number() over (
                partition by {{ key_columns | join(', ') }}
                order by 
                    case when {{ prefer_column }} is not null then 1 else 0 end desc,
                    {{ order_column }} desc
            ) as row_num
        from {{ relation }}
    ) as ranked
    where row_num = 1
)
{% endmacro %}


{% macro dq_check_geo_iso_pattern(model) %}
with rule_check as (
    select
        LEVEL,
        ISO_NUMERIC_CODE,
        case 
            when LEVEL = 'country' and ISO_NUMERIC_CODE != '840' then 'country_mismatch'
            when LEVEL = 'state' and ISO_NUMERIC_CODE is not null then 'state_mismatch'
            else null
        end as violation
    from {{ model }}
),
summary as (
    select 
        violation,
        count(*) as violation_count
    from rule_check
    where violation is not null
    group by violation
)
select *
from summary
{% endmacro %}


{% macro clean_crime_column(column_name) %}
    regexp_replace(trim({{ column_name }}), '[()<>?]', '')
{% endmacro %}


-- macros/dq_check_is_number.sql

{% macro dq_check_is_number(column_name) %}
    case
        when {{ column_name }} is not null then 'PASS'
        else 'FAIL_NOT_NUMERIC'
    end
{% endmacro %}

-- macros/clean_geo_column.sql
{% macro clean_geo_column(col, type) %}
    {% if type == 'geo_name' %}
        regexp_replace(trim({{ col }}), '[^a-zA-Z ]', '')
    {% else %}
        trim({{ col }})
    {% endif %}
{% endmacro %}




