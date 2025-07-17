{{ config(materialized='table') }}

select distinct
    to_char(cast(customer_id as integer)) as customer_id,
    name,
    age,
    email,
    salary,
    try_to_date({{ clean_date_column("joining_date") }}, 'DD-MM-YYYY') as joining_date,
    last_updated
from {{ ref('remediated_customers') }}
