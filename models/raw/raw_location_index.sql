{{ config(
    materialized = 'incremental',
    unique_key = ['GEO_ID']
) }}

select *
from {{ source('cybersyn', 'GEOGRAPHY_INDEX') }}
where GEO_ID is not null

{% if is_incremental() %}
  and GEO_ID not in (select distinct GEO_ID from {{ this }})
{% endif %}
