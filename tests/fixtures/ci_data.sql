-- Small fictitious network for dbt build in CI: one station goes empty, then full, then normal.
-- Station 2 reports the same logical key twice: a replay must not create a second row in the marts.
insert into velib.raw_station_information (station_id, station_code, name, lat, lon, capacity) values
    (1, '1001', 'Station test A', 48.85, 2.35, 20),
    (2, '1002', 'Station test B', 48.86, 2.36, 10);

insert into velib.raw_station_status
    (station_id, station_code, num_bikes_available, num_mechanical, num_ebike, num_docks_available,
     is_installed, is_renting, is_returning, last_reported, fetched_at)
values
    (1, '1001', 5, 3, 2, 15, true, true, true, '2035-01-01 08:00:00', '2035-01-01 08:01:00'),
    (1, '1001', 0, 0, 0, 20, true, true, true, '2035-01-01 08:30:00', '2035-01-01 08:31:00'),
    (1, '1001', 20, 12, 8, 0, true, true, true, '2035-01-01 09:00:00', '2035-01-01 09:01:00'),
    (1, '1001', 6, 3, 3, 14, true, true, true, '2035-01-01 09:30:00', '2035-01-01 09:31:00'),
    (2, '1002', 4, 4, 0, 6, true, true, true, '2035-01-01 08:00:00', '2035-01-01 08:01:00'),
    (2, '1002', 4, 4, 0, 6, true, true, true, '2035-01-01 08:00:00', '2035-01-01 08:02:00');

insert into velib.ingest_log (fetched_at, ingested_at, messages) values
    ('2035-01-01 08:01:00', '2035-01-01 08:01:01', 2),
    ('2035-01-01 09:31:00', '2035-01-01 09:31:01', 2);
