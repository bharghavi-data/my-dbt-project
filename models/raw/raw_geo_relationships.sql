{{ config(materialized = 'table') }}

select *
from {{ source('cybersyn', 'GEOGRAPHY_RELATIONSHIPS') }} gr
where gr.GEO_ID is not null
  and gr.GEO_ID in (
      select distinct GEO_ID
      from {{ ref('raw_statistical_observations') }}
      where GEO_ID is not null
  )

