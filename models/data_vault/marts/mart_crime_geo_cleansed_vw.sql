select distinct
    sg.geo_name,
    sc.variable_name,
    sc.offense_category,
    year(sc.observation_date) as date,
    sc.observation_value as value
from {{ ref('sat_geo_cleansed') }} sg
inner join {{ ref('sat_crime_cleansed') }} sc 
    on sg.geo_id = sc.geo_id
where sg.dbt_is_current = true
  and sc.dbt_is_current = true
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
