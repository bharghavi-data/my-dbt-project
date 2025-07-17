{{ config(materialized='table') }}

with base_data as (

    select
        md5(
            coalesce(trim(cast(rso.VARIABLE as string)), '') || '-' ||
            coalesce(trim(cast(rso.GEO_ID as string)), '') || '-' ||
            to_char(try_to_date(rso.DATE), 'YYYYMMDD')
        ) as crime_hk,

        rso.VARIABLE as variable,
        rim.VARIABLE_NAME as variable_name,
        rim.OFFENSE_CATEGORY as offense_category,
        rim.MEASURE as measure,
        rim.FREQUENCY as frequency,
        rso.UNIT as unit,
        try_to_date(rso.DATE) as observation_date,
        cast(rso.VALUE as float) as observation_value,
        rso.GEO_ID as geo_id,

        current_timestamp() as load_date,
        'RAW_CRIME' as record_source

    from {{ ref('raw_statistical_observations') }} rso
    left join {{ ref('raw_indicator_metadata') }} rim
        on rso.VARIABLE = rim.VARIABLE
    where rso.VARIABLE is not null
      and rso.GEO_ID is not null
      and rso.DATE is not null
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by crime_hk
                order by load_date desc
            ) as rn
        from base_data
    )
    where rn = 1
)

select * from deduped

