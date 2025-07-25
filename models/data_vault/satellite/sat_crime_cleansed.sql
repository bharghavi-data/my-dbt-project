{{ 
  config(
    materialized='table'
  ) 
}}

{{ 
  scd2_merge_auto(
    target=this,
    source=ref('stg_crime_cleansed'),
    unique_key='crime_hk',
    tracked_fields=[
      'variable',
      'variable_name', 
      'offense_category',
      'measure',
      'frequency',
      'unit',
      'observation_date',
      'observation_value',
      'geo_id',
      'record_source'
    ]
  ) 
}}
