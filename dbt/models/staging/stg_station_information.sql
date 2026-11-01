select
    station_id,
    argMax(station_code, loaded_at) as station_code,
    argMax(name, loaded_at) as name,
    argMax(lat, loaded_at) as lat,
    argMax(lon, loaded_at) as lon,
    argMax(capacity, loaded_at) as capacity
from {{ source('velib', 'raw_station_information') }}
group by station_id
