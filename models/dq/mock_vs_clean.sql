{{ config(materialized='view') }}

with raw as (
    select * from {{ ref('mock_data') }}
),

clean as (
    select
        id,
        {{ clean_column('name', remove_special_chars=true) }} as name,
        {{ clean_column('age', cast_type='integer') }} as age,
        {{ clean_column('email', to_lower=true, remove_special_chars=true) }} as email
    from {{ ref('mock_data') }}
)

select
    raw.id as raw_id,
    trim(raw.name) as raw_name,
    clean.name as clean_name,

    trim(raw.age) as raw_age,
    clean.age as clean_age,

    trim(raw.email) as raw_email,
    clean.email as clean_email

from raw
left join clean
    on raw.id = clean.id
