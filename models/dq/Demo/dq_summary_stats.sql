{{ config(materialized='table') }}

with raw as (
    select * from {{ ref('raw_data') }}
),

cleaned as (
    select * from {{ ref('dq_status') }}
),

-- STEP 1: Strict Validation on Raw Data (before cleansing)
before_valid as (
    select customer_id, name
    from raw
    where
        name is not null and regexp_like(name, '^[A-Za-z ]+$')
        and try_cast(age as integer) between 18 and 99
        and try_cast(salary as number) between 0 and 1000000
        and regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
        and (
            try_to_date(joining_date, 'YYYY-MM-DD') is not null or
            try_to_date(joining_date, 'DD/MM/YYYY') is not null or
            try_to_date(joining_date, 'YYYY/MM/DD') is not null or
            try_to_date(joining_date, 'DD-MM-YYYY') is not null or
            try_to_date(joining_date, 'YYYY.MM.DD') is not null or
            try_to_date(joining_date, 'Month DD, YYYY') is not null or
            try_to_date(joining_date, 'DD-Mon-YYYY') is not null or
            try_to_date(joining_date, 'Mon DD, YYYY') is not null
        )
),

-- STEP 2: After Cleansing DQ Status
after_valid as (
    select customer_id, name
    from cleaned
    where dq_status = 'PASSED'
),

summary as (
    select
        (select count(*) from raw) as total_records,
        (select count(*) from before_valid) as passed_before,
        (select count(*) from after_valid) as passed_after,
        (select lower(listagg(distinct name, ', ')) from before_valid) as names_before_passed,
        (select lower(listagg(distinct name, ', ')) from after_valid) as names_after_passed
),

final as (
    select
        *,
        round((passed_before * 100.0) / total_records, 2) as dq_valid_percent_before,
        round((passed_after * 100.0) / total_records, 2) as dq_valid_percent_after,
        round(((passed_after - passed_before) * 100.0) / total_records, 2) as dq_improvement_percent
    from summary
)

select * from final

