{{ config(
    materialized = 'incremental',
    unique_key = 'link_crime_geo_hk'
) }}

select distinct
    md5(
        coalesce(trim(cast(sc.crime_hk as string)), '') || '-' ||
        coalesce(trim(cast(sg.geo_hk as string)), '')
    ) as link_crime_geo_hk,

    sc.crime_hk,
    sg.geo_hk,

    current_timestamp() as load_date,
    'LINK_CRIME_GEO_LOGIC' as record_source

from {{ ref('sat_crime_cleansed') }} sc
join {{ ref('sat_geo_cleansed') }} sg
    on sc.geo_id = sg.geo_id

where sc.dbt_is_current = true
  and sg.dbt_is_current = true

  {% if is_incremental() %}
    and md5(
        coalesce(trim(cast(sc.crime_hk as string)), '') || '-' ||
        coalesce(trim(cast(sg.geo_hk as string)), '')
    ) not in (select distinct link_crime_geo_hk from {{ this }})
  {% endif %}





