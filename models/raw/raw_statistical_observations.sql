{{ config(
    materialized = 'incremental',
    unique_key = ['GEO_ID']
) }}

select 
    DATE,
    GEO_ID,
    UNIT,
    VALUE,
    VARIABLE,
    VARIABLE_NAME
from {{ source('cybersyn', 'FBI_CRIME_TIMESERIES') }}
where VARIABLE is not null 
  and GEO_ID is not null
  and DATE >= '2021-01-01'

  {% if is_incremental() %}
    and GEO_ID not in (select distinct GEO_ID from {{ this }})
  {% endif %}
group by 
    DATE,
    GEO_ID,
    UNIT,
    VALUE,
    VARIABLE,
    VARIABLE_NAME





