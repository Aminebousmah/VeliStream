-- Replaying a message must never create a second row for a logical key.
select station_id, last_reported, count() as rows
from {{ ref('stg_station_status') }}
group by station_id, last_reported
having rows > 1
