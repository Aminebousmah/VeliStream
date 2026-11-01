-- State of the whole network every 5 minutes: each station counts with its last known status,
-- even if it did not report during the bucket.
with bounds as (
    select
        toStartOfFiveMinutes(min(fetched_at)) as first_bucket,
        toStartOfFiveMinutes(max(fetched_at)) as last_bucket
    from {{ source('velib', 'ingest_log') }}
),

buckets as (
    select arrayJoin(
        arrayMap(
            step -> first_bucket + toIntervalMinute(5 * step),
            range(toUInt32(dateDiff('minute', first_bucket, last_bucket) / 5) + 1)
        )
    ) as bucket_utc
    from bounds
),

grid as (
    select b.bucket_utc, s.station_id
    from buckets as b
    cross join {{ ref('stg_station_information') }} as s
),

states as (
    select
        g.bucket_utc,
        g.station_id,
        e.last_reported,
        e.is_renting,
        e.num_bikes_available,
        e.num_mechanical,
        e.num_ebike,
        e.is_empty,
        e.is_full
    from grid as g
    asof left join {{ ref('int_station_status_enriched') }} as e
        on g.station_id = e.station_id
        and g.bucket_utc + toIntervalMinute(5) > e.last_reported
)

select
    bucket_utc,
    count() as stations_in_service,
    sum(num_bikes_available) as bikes_available,
    sum(num_mechanical) as mechanical_bikes,
    sum(num_ebike) as electric_bikes,
    avg(is_empty) as empty_rate,
    avg(is_full) as full_rate
from states
where last_reported > toDateTime(0, 'UTC') and is_renting
group by bucket_utc
