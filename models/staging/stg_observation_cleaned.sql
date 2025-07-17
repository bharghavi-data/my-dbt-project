{{ config(materialized='table') }}

select
    md5(
        {{ clean_column('GEO_ID', to_lower=true, remove_special_chars=true) }} || '-' ||
        {{ clean_column('VARIABLE', to_lower=true, remove_special_chars=true) }} || '-' ||
        to_char(try_to_date(DATE), 'YYYYMMDD')
    ) as location_indicator_hk,
    try_to_date(DATE) as observation_date,
    {{ clean_column('VALUE', cast_type='float') }} as observation_value,
    {{ clean_column('UNIT', to_lower=true) }} as unit,
    current_timestamp() as load_datetime,
    'FBI_CRIME_TIMESERIES' as record_source
from {{ ref('raw_statistical_observations') }}
where GEO_ID is not null 
  and VARIABLE is not null 
  and DATE is not null
