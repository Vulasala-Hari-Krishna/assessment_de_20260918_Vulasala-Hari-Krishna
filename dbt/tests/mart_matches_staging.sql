select
    coalesce(staged.city_id, mart.city_id) as city_id,
    coalesce(staged.weather_date, mart.weather_date) as weather_date
from {{ ref('stg_weather') }} as staged
full outer join {{ ref('fct_city_daily') }} as mart
    on staged.city_id = mart.city_id and staged.weather_date = mart.weather_date
where staged.city_id is null
   or mart.city_id is null
   or mart.temperature_min_c is distinct from staged.temperature_min_c
   or mart.temperature_max_c is distinct from staged.temperature_max_c
   or mart.temperature_mean_c is distinct from staged.temperature_mean_c
   or mart.precipitation_mm is distinct from staged.precipitation_mm
   or mart.wind_speed_max_kmh is distinct from staged.wind_speed_max_kmh
   or mart.temperature_range_c is distinct from (staged.temperature_max_c - staged.temperature_min_c)
   or mart.is_wet_day is distinct from (staged.precipitation_mm >= 1)