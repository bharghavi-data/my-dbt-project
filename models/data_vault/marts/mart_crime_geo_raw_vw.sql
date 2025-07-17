select distinct
    sg.geo_name,
    sc.variable_name,
    sc.offense_category,
    year(sc.observation_date) as date,
    sc.observation_value as value
from {{ ref('sat_geo_raw') }} sg
inner join {{ ref('sat_crime_raw') }} sc 
    on sg.geo_id = sc.geo_id
group by
    sg.geo_name,
    sc.variable_name,
    sc.offense_category,
    year(sc.observation_date),
    sc.observation_value
order by
    sg.geo_name,
    sc.offense_category,
    year(sc.observation_date),
    sc.observation_value

