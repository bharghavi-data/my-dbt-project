{% macro scd2_merge_auto_bkp(target, source, unique_key, tracked_fields=none) %}

-- Step 1: Normalize unique key(s)
{% if unique_key is string %}
    {% set unique_keys = [unique_key] %}
{% else %}
    {% set unique_keys = unique_key %}
{% endif %}

-- Step 2: Get all columns from source
{% set src_relation = adapter.get_relation(
    database=None,
    schema=source.schema if source.schema is defined else 'public',
    identifier=source.identifier
) %}
{% set all_cols = adapter.get_columns_in_relation(src_relation) | map(attribute='name') | list %}

-- Step 3: Derive tracked fields if not provided
{% if tracked_fields is none %}
    {% set tracked_fields = [] %}
    {% for col in all_cols %}
        {% if col not in unique_keys %}
            {% do tracked_fields.append(col) %}
        {% endif %}
    {% endfor %}
{% endif %}

-- Step 4: Define final columns (BK + tracked + scd fields)
{% set final_columns = unique_keys + tracked_fields + ['scd_hash', 'dbt_valid_from', 'dbt_valid_to', 'dbt_is_current'] %}

-- Step 5: Create source hash
with source_data as (
    select *,
        md5(
            {% for field in tracked_fields %}
                coalesce(trim(cast({{ field }} as string)), '')
                {% if not loop.last %} || {% endif %}
            {% endfor %}
        ) as scd_hash
    from {{ source }}
),

-- Step 6: Get current active rows from target
current_target as (
    select * from {{ target }}
    where dbt_is_current = true
),

-- Step 7: Identify changed or new rows
new_changes as (
    select s.*
    from source_data s
    left join current_target t
        on {% for key in unique_keys %}
            s.{{ key }} = t.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    where t.{{ unique_keys[0] }} is null or s.scd_hash != t.scd_hash
),

-- Step 8: Expire existing rows
expired_rows as (
    select 
        {% for col in final_columns %}
            {% if col == 'dbt_valid_to' %}
                current_timestamp() as dbt_valid_to,
            {% elif col == 'dbt_is_current' %}
                false as dbt_is_current
            {% else %}
                t.{{ col }},
            {% endif %}
        {% endfor %}
    from current_target t
    inner join new_changes s
        on {% for key in unique_keys %}
            t.{{ key }} = s.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
),

-- Step 9: Insert new rows
new_rows as (
    select 
        {% for col in final_columns %}
            {% if col == 'dbt_valid_from' %}
                current_timestamp() as dbt_valid_from,
            {% elif col == 'dbt_valid_to' %}
                cast(null as timestamp) as dbt_valid_to,
            {% elif col == 'dbt_is_current' %}
                true as dbt_is_current
            {% else %}
                s.{{ col }},
            {% endif %}
        {% endfor %}
    from new_changes s
),

-- Step 10: Retain unchanged current rows
unchanged_rows as (
    select 
        {% for col in final_columns %}
            t.{{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from current_target t
    left join new_changes s
        on {% for key in unique_keys %}
            t.{{ key }} = s.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    where s.{{ unique_keys[0] }} is null
),

-- Step 11: Bring forward all previously expired rows
already_expired as (
    select 
        {% for col in final_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ target }}
    where dbt_is_current = false
)

-- Step 12: Final unioned output (columns aligned)
select 
    {% for col in final_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from already_expired

union all

select 
    {% for col in final_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from expired_rows

union all

select 
    {% for col in final_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from new_rows

union all

select 
    {% for col in final_columns %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from unchanged_rows

{% endmacro %}
