-- models/data_vault/staging/stg_geo_cleansed.sql
{{ config(materialized='table') }}

with base_data as (

    select
        md5(
            coalesce(trim(cast(rli.GEO_ID as string)), '') || '-' ||
            coalesce(trim(cast(gr.RELATED_GEO_ID as string)), '')
        ) as GEO_HK,

        rli.GEO_ID as GEO_ID,
        rli.GEO_NAME as GEO_NAME,
        rli.GEO_NAME as NAME,  -- ensures `NAME` exists
        rli.ISO_3166_2_CODE as ISO_3166_2_CODE,
        rli.ISO_ALPHA2 as ISO_ALPHA2,
        rli.ISO_ALPHA3 as ISO_ALPHA3,
        rli.ISO_NAME as ISO_NAME,
        rli.ISO_NUMERIC_CODE as ISO_NUMERIC_CODE,
        rli.LEVEL as LEVEL,

        -- DQ flag for ISO_3166_2_CODE
        case 
            when rli.ISO_3166_2_CODE is null then 'OK'
            when rli.ISO_3166_2_CODE in ('ISO 3166-2:US', 'US') then 'OK'
            else 'UNEXPECTED'
        end as ISO_3166_2_CODE_DQ_FLAG,

        'JOINED_GEO_REL' as RECORD_SOURCE,

        current_timestamp() as LOAD_DATE,
        current_timestamp() as LAST_UPDATED

    from {{ ref('raw_location_index') }} rli
    inner join {{ ref('raw_geo_relationships') }} gr
        on rli.GEO_ID = gr.GEO_ID
    where rli.GEO_ID is not null

),

deduped as (
    select *
    from {{ remove_duplicates(
        relation='base_data',
        key_columns=['GEO_HK']
    ) }}
)

select *
from deduped
