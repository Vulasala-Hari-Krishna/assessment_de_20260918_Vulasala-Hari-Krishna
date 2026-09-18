select city_id, logical_date
from {{ source('raw', 'weather_daily') }}
where jsonb_array_length(payload #> '{daily,time}') is distinct from 1
   or (payload #>> '{daily,time,0}')::date is distinct from logical_date