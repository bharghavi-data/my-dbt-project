{{ config(materialized='table') }}

with dq_validated as (

    
    select * 
    from {{ ref('dq_status') }}

), 

final_remediated as (

    
    select 
        customer_id,
        name,
        age,
        email,
        salary,
        joining_date,
        last_updated
    from dq_validated
    where dq_status = 'PASSED'

)

select * from final_remediated
