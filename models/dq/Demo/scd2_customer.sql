{{ config(materialized='table') }}

{{ scd2_merge_auto(
    target=this,
    source=ref('remediated'),
    unique_key='customer_id',
    tracked_fields=[
        'name',
        'age',
        'email',
        'salary',
        'joining_date',
        'last_updated'
    ]
) }}
