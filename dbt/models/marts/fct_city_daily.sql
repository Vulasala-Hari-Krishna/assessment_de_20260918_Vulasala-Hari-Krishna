select
    city_id,
    city_name,
    weather_date,
    timezone,
    temperature_min_c,
    temperature_max_c,
    temperature_mean_c,
    temperature_max_c - temperature_min_c as temperature_range_c,
    precipitation_mm,
    precipitation_mm >= 1 as is_wet_day,
    wind_speed_max_kmh,
    ingested_at
from {{ ref('stg_weather') }}