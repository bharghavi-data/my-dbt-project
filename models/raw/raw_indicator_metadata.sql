{{ config(
    materialized = 'incremental',
    unique_key = ['VARIABLE']
) }}

select
    VARIABLE,
    VARIABLE_NAME,
    OFFENSE_CATEGORY,
    MEASURE,
    FREQUENCY,
    UNIT
from {{ source('cybersyn', 'FBI_CRIME_ATTRIBUTES') }}
where VARIABLE is not null

{% if is_incremental() %}
  and VARIABLE not in (select distinct VARIABLE from {{ this }})
{% endif %}

group by
    VARIABLE,
    VARIABLE_NAME,
    OFFENSE_CATEGORY,
    MEASURE,
    FREQUENCY,
    UNIT
