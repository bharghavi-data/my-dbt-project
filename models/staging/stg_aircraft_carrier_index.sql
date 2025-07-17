{{ config(materialized='table') }}

select *
from {{ source('cybersyn', 'AIRCRAFT_CARRIER_INDEX') }}
where AIRCRAFT_CARRIER_ID is not null
