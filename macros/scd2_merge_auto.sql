{% macro scd2_merge_auto(target, source, unique_key, tracked_fields=none) %}
-- Normalize unique keys
{% if unique_key is string %}
    {% set unique_keys = [unique_key] %}
{% else %}
    {% set unique_keys = unique_key %}
{% endif %}

-- Get source columns
{% set src_relation = source %}
{% set all_cols = adapter.get_columns_in_relation(src_relation) | map(attribute='name') | list %}

-- Determine tracked fields
{% if tracked_fields is none %}
    {% set tracked_fields = [] %}
    {% for col in all_cols %}
        {% if col not in unique_keys and col.lower() not in ['scd_hash', 'dbt_valid_from', 'dbt_valid_to', 'dbt_is_current'] %}
            {% do tracked_fields.append(col) %}
        {% endif %}
    {% endfor %}
{% endif %}

-- Final columns
{% set final_columns = unique_keys + tracked_fields + ['scd_hash', 'dbt_valid_from', 'dbt_valid_to', 'dbt_is_current'] %}

with source_data as (
    select 
        *,
        md5(concat(
            {% for field in tracked_fields %}
            coalesce(cast({{ field }} as string), '')
            {%- if not loop.last -%}, {% endif %}
            {% endfor %}
        )) as scd_hash
    from {{ source }}
),
existing_data as (
    {% if is_incremental() or adapter.get_relation(this.database, this.schema, this.identifier) %}
    select * from {{ this }}
    {% else %}
    select * from (select 1 where false)
    {% endif %}
),
existing_current as (
    select * from existing_data
    where dbt_is_current = true
),
changes as (
    select 
        s.*,
        e.scd_hash as existing_scd_hash,
        case 
            when e.{{ unique_keys[0] }} is null then 'new'
            when s.scd_hash != e.scd_hash then 'changed'
            else 'unchanged'
        end as change_type
    from source_data s
    left join existing_current e 
        on {% for key in unique_keys %}
            s.{{ key }} = e.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
),
expired_records as (
    select 
        {% for col in unique_keys %}
        e.{{ col }},
        {% endfor %}
        {% for col in tracked_fields %}
        e.{{ col }},
        {% endfor %}
        e.scd_hash,
        e.dbt_valid_from,
        current_timestamp() as dbt_valid_to,
        false as dbt_is_current
    from existing_current e
    inner join changes c 
        on {% for key in unique_keys %}
            e.{{ key }} = c.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    where c.change_type = 'changed'
),
unchanged_records as (
    select e.*
    from existing_current e
    inner join changes c 
        on {% for key in unique_keys %}
            e.{{ key }} = c.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    where c.change_type = 'unchanged'
),
new_records as (
    select 
        {% for col in unique_keys %}
        c.{{ col }},
        {% endfor %}
        {% for col in tracked_fields %}
        c.{{ col }},
        {% endfor %}
        c.scd_hash,
        current_timestamp() as dbt_valid_from,
        cast(null as timestamp) as dbt_valid_to,
        true as dbt_is_current
    from changes c
    where c.change_type in ('new', 'changed')
),
historical_records as (
    select * from existing_data
    where dbt_is_current = false
)

select * from historical_records
union all
select * from unchanged_records
union all
select * from expired_records
union all
select * from new_records

{% endmacro %}