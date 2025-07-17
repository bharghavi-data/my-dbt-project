with base as (
    select
        AIRCRAFT_CARRIER_ID,
        {{ clean_column('CARRIER_NAME', to_lower=true, remove_special_chars=true) }} as carrier_name,
        CARRIER_WORLD_AREA_CODE,
        {{ clean_column('CARRIER_TYPE', to_lower=true, remove_special_chars=true) }} as carrier_type,
        {{ clean_column('OAI_CARRIER_TYPE', to_lower=true, remove_special_chars=true) }} as oai_carrier_type
    from {{ ref('stg_aircraft_carrier_index') }}
    where AIRCRAFT_CARRIER_ID is not null
)

select * from base