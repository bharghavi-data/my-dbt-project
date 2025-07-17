{{ config(materialized='table') }}

select * 
from {{ apply_remediation('dq_status') }}
