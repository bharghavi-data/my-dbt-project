

{{ config(materialized='table') }}

{{ scd2_merge_auto_bkp(
    target='airport_scd2_auto',
    source=ref('stg_airport_source'),
    unique_key=['AIRPORT_DOT_CODE', 'AIRPORT_ALPHA_CODE']
) }}
