{{ config(materialized='table') }}

with raw_data as (

    select ' John ' as name, ' 25 ' as age
    union all
    select 'Jane', null
    union all
    select null, '30'

),

cleansed_data as (

    select
        trim(name) as name,
        try_cast(trim(age) as integer) as age
    from raw_data

)

select *
from cleansed_data
where name is not null
