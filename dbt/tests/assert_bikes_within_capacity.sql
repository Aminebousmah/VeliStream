-- The feed sometimes counts a few bikes above the declared capacity. Tolerate a small overflow.
{{ config(severity='warn') }}

select station_id, last_reported, num_bikes_available, capacity
from {{ ref('int_station_status_enriched') }}
where capacity > 0 and num_bikes_available > capacity + 5
