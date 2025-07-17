{{ config(materialized='view') }}

select 
    AIRPORT_DOT_CODE,
    AIRPORT_NAME,
    AIRPORT_ALPHA_CODE,
    AIRPORT_WORLD_AREA_CODE,
    AIRPORT_CITY_NAME,
    STATE_GEO_ID,
    COUNTRY_GEO_ID
from {{ ref('airport') }}
