{% macro apply_remediation(ref_model) %}
    {# Get the relation object from the ref #}
    {% set relation = ref_model if ref_model is mapping else ref(ref_model) %}

    {# Get all column names from the table #}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {# Filter out dq_status column #}
    {% set selected_columns = columns 
        | map(attribute='name') 
        | reject('equalto', 'dq_status') 
        | list 
    %}

    (
        select 
            {{ selected_columns | join(', ') }}
        from {{ relation }}
        where dq_status = 'PASSED'
    )
{% endmacro %}
