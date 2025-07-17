{{ config(
    materialized = 'incremental',
    unique_key = 'geo_hk'
) }}

with base_data as (

    select
        md5(
            coalesce(trim(cast(rli.GEO_ID as string)), '') || '-' ||
            coalesce(trim(cast(gr.RELATED_GEO_ID as string)), '')
        ) as geo_hk,

        rli.GEO_ID,
        rli.GEO_NAME,
        rli.ISO_3166_2_CODE,
        rli.ISO_ALPHA2,
        rli.ISO_ALPHA3,
        rli.ISO_NAME,
        rli.ISO_NUMERIC_CODE,
        rli.LEVEL,

        current_timestamp() as load_date,
        'JOINED_GEO_REL' as record_source

    from {{ ref('raw_location_index') }} rli
    inner join {{ ref('raw_geo_relationships') }} gr
        on rli.GEO_ID = gr.GEO_ID
    where rli.GEO_ID is not null

    {% if is_incremental() %}
      -- only get rows not already present in the target table
      and md5(
            coalesce(trim(cast(rli.GEO_ID as string)), '') || '-' ||
            coalesce(trim(cast(gr.RELATED_GEO_ID as string)), '')
          ) not in (
            select geo_hk from {{ this }}
          )
    {% endif %}
),

deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by geo_hk
                order by load_date desc
            ) as rn
        from base_data
    )
    where rn = 1
)

select * from deduped





