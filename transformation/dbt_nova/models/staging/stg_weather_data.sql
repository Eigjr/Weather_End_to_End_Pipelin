with src as (
  select *
  from {{ source('raw_data', 'weather_data') }}
)

select
  city,
  country,
  day::date as day,
  temp_max::double precision as temp_max,
  temp_min::double precision as temp_min,
  wind_speed::double precision as wind_speed
  
from src
