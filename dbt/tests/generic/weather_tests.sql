{% test unique_city_date(model, date_column) %}
select city_id, {{ date_column }}, count(*) as row_count
from {{ model }}
group by city_id, {{ date_column }}
having count(*) > 1
{% endtest %}

{% test between_values(model, column_name, minimum, maximum) %}
select *
from {{ model }}
where {{ column_name }} < {{ minimum }} or {{ column_name }} > {{ maximum }}
{% endtest %}

{% test ordered_temperatures(model) %}
select *
from {{ model }}
where temperature_min_c > temperature_mean_c
   or temperature_mean_c > temperature_max_c
{% endtest %}