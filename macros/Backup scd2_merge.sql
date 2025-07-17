{% macro scd2_merge(target, source, unique_key, tracked_fields=none) %}

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

-- Final columns in consistent order
{% set final_columns = unique_keys + tracked_fields + ['scd_hash', 'dbt_valid_from', 'dbt_valid_to', 'dbt_is_current'] %}

with source_data as (
    select 
        {% for col in unique_keys + tracked_fields %}
        {{ col }},
        {% endfor %}
        md5(concat(
            {% for field in tracked_fields %}
            coalesce(cast({{ field }} as string), '')
            {%- if not loop.last -%}, {% endif %}
            {% endfor %}
        )) as scd_hash
    from {{ source }}
),

existing_data as (
    {% if is_incremental() and adapter.get_relation(this.database, this.schema, this.identifier) %}
    select * from {{ this }}
    {% else %}
    -- Return empty set with correct schema for initial load
    select 
        {% for col in final_columns %}
        {% if col in unique_keys or col in tracked_fields or col == 'scd_hash' %}
        cast(null as string) as {{ col }}
        {% elif col == 'dbt_valid_from' or col == 'dbt_valid_to' %}
        cast(null as timestamp) as {{ col }}
        {% elif col == 'dbt_is_current' %}
        cast(null as boolean) as {{ col }}
        {% endif %}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from (select 1 where false)
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

-- ✅ Deduplicate to one change per key using row_number()
records_to_process as (
    select *
    from (
        select *,
            row_number() over (
                partition by {{ unique_keys | join(', ') }}
                order by current_timestamp()
            ) as row_num
        from changes
        where change_type in ('new', 'changed')
    ) ranked
    where row_num = 1
),

-- Keep all historical records (already expired) - only if we have changes to process
historical_records as (
    select * from existing_data
    where dbt_is_current = false
    and exists (select 1 from records_to_process)
),

-- Keep unchanged current records - only if we have changes to process  
unchanged_records as (
    select e.*
    from existing_current e
    where exists (select 1 from records_to_process)
    and not exists (
        select 1 from records_to_process r
        where {% for key in unique_keys %}
            e.{{ key }} = r.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    )
),

-- Expire changed records (set dbt_is_current to false and dbt_valid_to to current timestamp)
-- FIXED: Use existing record's original values with updated SCD metadata
expired_records as (
    select 
        {% for col in final_columns %}
        {% if col == 'dbt_valid_to' %}
        current_timestamp() as dbt_valid_to,
        {% elif col == 'dbt_is_current' %}
        false as dbt_is_current,
        {% else %}
        e.{{ col }}
        {% endif %}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from existing_current e
    inner join records_to_process r
        on {% for key in unique_keys %}
            e.{{ key }} = r.{{ key }}{% if not loop.last %} and {% endif %}
        {% endfor %}
    where r.change_type = 'changed'
),

-- Insert new current records (both new and changed)
new_current_records as (
    select 
        {% for col in final_columns %}
        {% if col == 'dbt_valid_from' %}
        current_timestamp() as dbt_valid_from,
        {% elif col == 'dbt_valid_to' %}
        cast(null as timestamp) as dbt_valid_to,
        {% elif col == 'dbt_is_current' %}
        true as dbt_is_current,
        {% else %}
        r.{{ col }}
        {% endif %}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from records_to_process r
)

-- Combine all records for the final result
{% if is_incremental() %}
select * from expired_records
union all
select * from new_current_records
{% else %}
-- For full refresh, return all records with SCD2 structure
select 
    {% for col in final_columns %}
    {% if col == 'dbt_valid_from' %}
    current_timestamp() as dbt_valid_from,
    {% elif col == 'dbt_valid_to' %}
    cast(null as timestamp) as dbt_valid_to,
    {% elif col == 'dbt_is_current' %}
    true as dbt_is_current,
    {% else %}
    {{ col }},
    {% endif %}
    {% if not loop.last %},{% endif %}
    {% endfor %}
from source_data
{% endif %}

{% endmacro %}