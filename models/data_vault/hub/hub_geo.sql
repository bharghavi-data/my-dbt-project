{{ config(
    materialized='incremental',
    unique_key='geo_hk'
) }}

select distinct
    geo_hk,
    current_timestamp() as load_date,
    'HUB_GEO' as record_source
from {{ ref('stg_geo_cleansed') }}
where geo_hk is not null

{% if is_incremental() %}
    and geo_hk not in (select geo_hk from {{ this }})
{% endif %} 


