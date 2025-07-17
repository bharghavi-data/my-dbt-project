{{ config(materialized='table') }}

with source as (
    select * from {{ ref('mock_data') }}
),

cleansed as (
    select
        id,
        {{ clean_column('name', remove_special_chars=true) }} as name,
        {{ clean_column('age', cast_type='integer') }} as age,
        {{ clean_column('email', to_lower=true, remove_special_chars=true) }} as email
    from source
)

select * from cleansed
