{{ config(
    materialized = 'incremental',
    unique_key = 'crime_hk'
) }}

with base_data as (

    select
        md5(
            coalesce(trim(cast(rso.VARIABLE as string)), '') || '-' ||
            coalesce(trim(cast(rso.GEO_ID as string)), '') || '-' ||
            to_char(try_to_date(rso.DATE), 'YYYYMMDD')
        ) as crime_hk,

        rso.VARIABLE as variable,
        {{ clean_column('rim.VARIABLE_NAME', to_lower=true) }} as variable_name,
        {{ clean_column('rim.OFFENSE_CATEGORY', to_lower=true) }} as offense_category,
        {{ clean_column('rim.MEASURE', to_lower=true) }} as measure,
        {{ clean_column('rim.FREQUENCY', to_lower=true) }} as frequency,
        {{ clean_column('rso.UNIT', to_lower=true) }} as unit,
        try_to_date(rso.DATE) as observation_date,
        {{ clean_column('rso.VALUE', cast_type='float') }} as observation_value,
        rso.GEO_ID as geo_id,

        current_timestamp() as load_date,
        'RAW_CRIME' as record_source,

        {{ clean_column('rim.VARIABLE_NAME', to_lower=true) }} as name,
        current_timestamp() as last_updated

    from {{ ref('raw_statistical_observations') }} rso
    left join {{ ref('raw_indicator_metadata') }} rim
        on rso.VARIABLE = rim.VARIABLE

    where rso.VARIABLE is not null
      and rso.GEO_ID is not null
      and rso.DATE is not null

    {% if is_incremental() %}
      and md5(
          coalesce(trim(cast(rso.VARIABLE as string)), '') || '-' ||
          coalesce(trim(cast(rso.GEO_ID as string)), '') || '-' ||
          to_char(try_to_date(rso.DATE), 'YYYYMMDD')
      ) not in (
          select crime_hk from {{ this }}
      )
    {% endif %}

),

deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by crime_hk
                order by last_updated desc
            ) as row_num
        from base_data
    )
    where row_num = 1
)

select * from deduplicated



/*{{ config(materialized='table') }}

with base_data as (

    select
        md5(
            coalesce(trim(cast(rso.VARIABLE as string)), '') || '-' ||
            coalesce(trim(cast(rso.GEO_ID as string)), '') || '-' ||
            to_char(try_to_date(rso.DATE), 'YYYYMMDD')
        ) as crime_hk,

        {{ clean_column('rso.VARIABLE', to_lower=true, remove_special_chars=true) }} as variable,
        {{ clean_column('rim.VARIABLE_NAME', to_lower=true) }} as variable_name,
        {{ clean_column('rim.OFFENSE_CATEGORY', to_lower=true) }} as offense_category,
        {{ clean_column('rim.MEASURE', to_lower=true) }} as measure,
        {{ clean_column('rim.FREQUENCY', to_lower=true) }} as frequency,
        {{ clean_column('rso.UNIT', to_lower=true) }} as unit,
        try_to_date(rso.DATE) as observation_date,
        {{ clean_column('rso.VALUE', cast_type='float') }} as observation_value,
        rso.GEO_ID as geo_id,

        current_timestamp() as load_date,
        'RAW_CRIME' as record_source,

        {{ clean_column('rim.VARIABLE_NAME', to_lower=true) }} as name,  
        current_timestamp() as last_updated
    from {{ ref('raw_statistical_observations') }} rso
    left join {{ ref('raw_indicator_metadata') }} rim
        on rso.VARIABLE = rim.VARIABLE
    where rso.VARIABLE is not null
      and rso.GEO_ID is not null
      and rso.DATE is not null
)

select *
from {{ remove_duplicates(
    relation='base_data',
    key_columns=['crime_hk']
) }} */
