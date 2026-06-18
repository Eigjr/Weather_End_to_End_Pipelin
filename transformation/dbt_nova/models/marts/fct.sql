select
  city,
  day,
  avg(temp_max) as avg_temp_max,
  avg(temp_min) as avg_temp_min,
  avg(wind_speed) as avg_wind_speed
from {{ ref('stg_weather_data') }}
group by 1,2