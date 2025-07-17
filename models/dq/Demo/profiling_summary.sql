{{ config(materialized='table') }}

{% set table = 'stg_airport_source' %}
{% set columns = ['AIRPORT_NAME', 'AIRPORT_ALPHA_CODE', 'AIRPORT_CITY_NAME', 'STATE_GEO_ID'] %}

{{ generate_profiling_metrics(table, columns) }}
