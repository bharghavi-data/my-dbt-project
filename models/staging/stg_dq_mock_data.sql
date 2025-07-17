{{ config(materialized='table') }}

with source as (
    select 
        id,
        {{ clean_column('name', to_lower=true, remove_special_chars=true) }} as name,
        {{ clean_column('age', cast_type='number') }} as age,
        {{ clean_email('email') }} as email,
        {{ clean_column('salary', cast_type='number') }} as salary,
        {{ clean_date_column('joining_date') }} as joining_date
    from {{ ref('dq_mock_data') }}
),

deduped as (
    select * ,
           row_number() over (partition by name, email order by id) as row_num
    from source
),

dq_applied as (
    select 
        *,
        {{ dq_check_not_null('name') }} as name_check,
        {{ dq_check_numeric_range('age', 18, 99) }} as age_check,
        {{ dq_check_email_format('email') }} as email_check,
        {{ dq_check_numeric_range('salary', 0, 1000000) }} as salary_check,
        case when joining_date is null then 'FAIL_INVALID_DATE' else 'PASS' end as joining_date_check,
        case when row_num > 1 then 'FAIL_DUPLICATE' else 'PASS' end as duplicate_check
    from deduped
),

clean_records as (
    select *
    from dq_applied
    where name_check = 'PASS'
      and age_check = 'PASS'
      and email_check = 'PASS'
      and salary_check = 'PASS'
      and joining_date_check = 'PASS'
      and duplicate_check = 'PASS'
),

final as (
    select 
        dq.*,
        case when c.id is not null then 'PASSED' else 'FAILED' end as dq_status
    from dq_applied dq
    left join clean_records c using(id)
)

select * from final
