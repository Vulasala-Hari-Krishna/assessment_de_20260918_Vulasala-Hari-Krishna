select
    nullif(trim(city_id), '') as city_id,
    nullif(trim(city_name), '') as city_name,
    (payload #>> '{daily,time,0}')::date as weather_date,
    nullif(trim(payload ->> 'timezone'), '') as timezone,
    (payload #>> '{daily,temperature_2m_min,0}')::numeric as temperature_min_c,
    (payload #>> '{daily,temperature_2m_max,0}')::numeric as temperature_max_c,
    (payload #>> '{daily,temperature_2m_mean,0}')::numeric as temperature_mean_c,
    (payload #>> '{daily,precipitation_sum,0}')::numeric as precipitation_mm,
    (payload #>> '{daily,wind_speed_10m_max,0}')::numeric as wind_speed_max_kmh,
    ingested_at
from {{ source('raw', 'weather_daily') }}