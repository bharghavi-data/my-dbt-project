{{ config(materialized='table') }}

with scd2_cleaned as (

    select 
        customer_id,
        {{ clean_column('name', to_lower=true, remove_special_chars=true) }} as name,
        {{ clean_column('age', cast_type='number') }} as age,
        {{ clean_email('email') }} as email,
        {{ clean_column('salary', cast_type='number') }} as salary,
        {{ clean_date_column('joining_date') }} as joining_date,
        last_updated
    from {{ ref('raw_data') }}
    where customer_id is not null and email is not null 

),

--  This step removes true duplicates and keeps the best record
deduped as (
    {{ remove_duplicates('scd2_cleaned', ['customer_id', 'email'], 'last_updated', 'name') }}
),

--  Apply data quality checks
dq_applied as (
    select 
        customer_id,
        name,
        age,
        email,
        salary,
        joining_date,
        last_updated,

        {{ dq_check_not_null('name') }} as name_check,
        {{ dq_check_numeric_range('age', 18, 99) }} as age_check,
        {{ dq_check_email_format('email') }} as email_check,
        {{ dq_check_numeric_range('salary', 0, 1000000) }} as salary_check,
        case 
            when joining_date is null then 'FAIL_INVALID_DATE'
            else 'PASS'
        end as joining_date_check
    from deduped
),

--  Final output with DQ status
final as (
    select 
        *,
        case 
            when name_check = 'PASS'
             and age_check = 'PASS'
             and email_check = 'PASS'
             and salary_check = 'PASS'
             and joining_date_check = 'PASS'
            then 'PASSED'
            else 'FAILED'
        end as dq_status
    from dq_applied
)

select * from final
