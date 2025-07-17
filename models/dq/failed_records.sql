{{ config(materialized='table') }}

with dq as (
    select * from {{ ref('stg_dq_mock_data') }}
),

failed as (
    select 
        id, name, age, email, salary, joining_date,
        case 
            when name_check != 'PASS' then name_check
            when age_check != 'PASS' then age_check
            when email_check != 'PASS' then email_check
            when salary_check != 'PASS' then salary_check
            when joining_date_check != 'PASS' then joining_date_check
            when duplicate_check != 'PASS' then duplicate_check
        end as failure_reason
    from dq
    where dq_status = 'FAILED'
)

select * from failed
