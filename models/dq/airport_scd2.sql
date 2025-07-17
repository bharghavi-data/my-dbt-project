{{ config(materialized='table') }}

{{ scd2_merge(
    target='airport_scd2',
    source=ref('stg_airport_source'),
    unique_key='AIRPORT_DOT_CODE',
    tracked_fields=[
        'AIRPORT_NAME',
        'AIRPORT_ALPHA_CODE',
        'AIRPORT_WORLD_AREA_CODE',
        'AIRPORT_CITY_NAME',
        'STATE_GEO_ID',
        'COUNTRY_GEO_ID'
    ]
) }}
