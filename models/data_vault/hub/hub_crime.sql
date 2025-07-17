{{ config(
    materialized='incremental',
    unique_key='crime_hk'
) }}

select distinct
    crime_hk,
    current_timestamp() as load_date,
    'HUB_CRIME' as record_source
from {{ ref('stg_crime_cleansed') }}
where crime_hk is not null

{% if is_incremental() %}
    and crime_hk not in (select crime_hk from {{ this }})
{% endif %}




