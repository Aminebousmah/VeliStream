-- From the API fetch to ClickHouse, weighted by messages. Precision: one second.
select
    toStartOfFiveMinutes(fetched_at) as bucket_utc,
    sum(messages) as total_messages,
    uniqExact(fetched_at) as collection_cycles,
    quantileExactWeighted(0.5)(dateDiff('second', fetched_at, ingested_at), messages) as latency_s_p50,
    quantileExactWeighted(0.95)(dateDiff('second', fetched_at, ingested_at), messages) as latency_s_p95
from {{ source('velib', 'ingest_log') }}
group by bucket_utc
