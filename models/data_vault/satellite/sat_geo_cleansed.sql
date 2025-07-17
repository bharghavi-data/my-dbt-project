{{ config(
    materialized='table'
) }}

{{ scd2_merge_auto(
    target=this,
    source=ref('stg_geo_cleansed'),
    unique_key='geo_hk',
    tracked_fields=[
        'geo_id', 'geo_name', 'iso_3166_2_code',
        'iso_alpha2', 'iso_alpha3', 'iso_name',
        'iso_numeric_code', 'level', 'record_source'
    ]
) }}
