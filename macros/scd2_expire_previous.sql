{% macro scd2_expire_previous(model_ref, unique_key) %}
{% if unique_key is string %}
    {% set keys = [unique_key] %}
{% else %}
    {% set keys = unique_key %}
{% endif %}

{% set join_condition %}
    {% for key in keys %}
        newer.{{ key }} = tgt.{{ key }}{% if not loop.last %} and {% endif %}
    {% endfor %}
{% endset %}

update {{ model_ref }} as tgt
set 
    dbt_is_current = false,
    dbt_valid_to = current_timestamp()
where exists (
    select 1
    from {{ model_ref }} as newer
    where 
        {{ join_condition }}
        and newer.dbt_is_current = true
        and newer.dbt_valid_from > tgt.dbt_valid_from
)
and tgt.dbt_is_current = true;
{% endmacro %}
