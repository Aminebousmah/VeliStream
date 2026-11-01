-- Gaps and islands: an episode lasts as long as a station in service keeps the same state.
with collection as (
    select max(fetched_at) as last_fetch from {{ source('velib', 'ingest_log') }}
),

states as (
    select
        station_id,
        name,
        last_reported,
        multiIf(not is_renting, 'normal', is_empty, 'empty', is_full, 'full', 'normal')
            as station_state
    from {{ ref('int_station_status_enriched') }}
),

with_previous as (
    select
        *,
        lagInFrame(station_state, 1, '') over w as previous_state,
        leadInFrame(last_reported, 1, toDateTime(0, 'UTC')) over w as next_report
    from states
    window w as (
        partition by station_id
        order by last_reported
        rows between unbounded preceding and unbounded following
    )
),

numbered as (
    select
        *,
        sum(if(station_state != previous_state, 1, 0)) over (
            partition by station_id
            order by last_reported
            rows between unbounded preceding and current row
        ) as episode_id
    from with_previous
)

select
    station_id,
    any(name) as name,
    episode_id,
    any(station_state) as state,
    min(last_reported) as started_at,
    -- An episode still running at the last collection stays open until then.
    max(if(next_report = toDateTime(0, 'UTC'), (select last_fetch from collection), next_report))
        as ended_at,
    countIf(next_report = toDateTime(0, 'UTC')) > 0 as is_open,
    dateDiff('minute', started_at, ended_at) as duration_minutes
from numbered
where station_state != 'normal'
group by station_id, episode_id
