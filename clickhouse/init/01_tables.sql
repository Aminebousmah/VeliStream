create database if not exists velib;

create table if not exists velib.raw_station_status
(
    station_id          UInt64,
    station_code        String,
    num_bikes_available UInt16,
    num_mechanical      UInt16,
    num_ebike           UInt16,
    num_docks_available UInt16,
    is_installed        Bool,
    is_renting          Bool,
    is_returning        Bool,
    last_reported       DateTime('UTC'),
    fetched_at          DateTime('UTC'),
    ingested_at         DateTime64(3, 'UTC') default now64(3)
)
engine = ReplacingMergeTree(ingested_at)
partition by toDate(last_reported)
order by (station_id, last_reported)
ttl toDateTime(ingested_at) + interval 90 day;

create table if not exists velib.raw_station_information
(
    station_id   UInt64,
    station_code String,
    name         String,
    lat          Float64,
    lon          Float64,
    capacity     UInt16,
    loaded_at    DateTime('UTC') default now()
)
engine = ReplacingMergeTree(loaded_at)
order by station_id;

create table if not exists velib.dlq_messages
(
    raw_message String,
    received_at DateTime('UTC') default now()
)
engine = MergeTree
order by received_at
ttl received_at + interval 30 day;

-- Kafka engine tables only consume: they hold no data of their own.
create table if not exists velib.kafka_station_status
(
    station_id          UInt64,
    station_code        String,
    num_bikes_available UInt16,
    num_mechanical      UInt16,
    num_ebike           UInt16,
    num_docks_available UInt16,
    is_installed        Bool,
    is_renting          Bool,
    is_returning        Bool,
    last_reported       DateTime('UTC'),
    fetched_at          DateTime('UTC')
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'velib.station_status',
    kafka_group_name = 'clickhouse_station_status',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_num_consumers = 1;

-- now() and now64() stay frozen for a whole Kafka stream: only nowInBlock() tells the real time.
create materialized view if not exists velib.mv_station_status
to velib.raw_station_status
as select *, toDateTime64(nowInBlock(), 3, 'UTC') as ingested_at from velib.kafka_station_status;

-- raw_station_status merges repeated statuses on purpose, so it cannot count what was received.
-- This journal keeps one row per block: it proves that nothing is lost, and measures latency.
create table if not exists velib.ingest_log
(
    fetched_at  DateTime('UTC'),
    ingested_at DateTime('UTC'),
    messages    UInt64
)
engine = MergeTree
partition by toDate(fetched_at)
order by (fetched_at, ingested_at)
ttl fetched_at + interval 90 day;

create materialized view if not exists velib.mv_ingest_log
to velib.ingest_log
as
select fetched_at, nowInBlock() as ingested_at, count() as messages
from velib.kafka_station_status
group by fetched_at;

create table if not exists velib.kafka_dlq
(
    raw_message String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'velib.dlq',
    kafka_group_name = 'clickhouse_dlq',
    kafka_format = 'JSONAsString';

create materialized view if not exists velib.mv_dlq
to velib.dlq_messages
as select raw_message from velib.kafka_dlq;
