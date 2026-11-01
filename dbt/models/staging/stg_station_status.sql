-- One row per logical key, whatever the state of the background merges.
select
    station_id,
    last_reported,
    argMax(station_code, ingested_at) as station_code,
    argMax(num_bikes_available, ingested_at) as num_bikes_available,
    argMax(num_mechanical, ingested_at) as num_mechanical,
    argMax(num_ebike, ingested_at) as num_ebike,
    argMax(num_docks_available, ingested_at) as num_docks_available,
    argMax(is_installed, ingested_at) as is_installed,
    argMax(is_renting, ingested_at) as is_renting
from {{ source('velib', 'raw_station_status') }}
group by station_id, last_reported
