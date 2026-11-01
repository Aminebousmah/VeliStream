select
    station_id,
    any(name) as name,
    toStartOfHour(last_reported) as hour_utc,
    avg(num_bikes_available) as avg_bikes,
    min(num_bikes_available) as min_bikes,
    max(num_bikes_available) as max_bikes,
    count() as reports
from {{ ref('int_station_status_enriched') }}
group by station_id, hour_utc
