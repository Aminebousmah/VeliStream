with statuses as (
    select * from {{ ref('stg_station_status') }}
),

stations as (
    select * from {{ ref('stg_station_information') }}
)

select
    s.station_id,
    s.last_reported,
    i.name,
    i.lat,
    i.lon,
    i.capacity,
    s.num_bikes_available,
    s.num_mechanical,
    s.num_ebike,
    s.num_docks_available,
    s.is_renting,
    s.num_bikes_available = 0 as is_empty,
    s.num_docks_available = 0 as is_full
from statuses as s
inner join stations as i on s.station_id = i.station_id
where s.is_installed
