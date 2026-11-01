"""Assets, checks and schedules. Streaming ingestion stays outside: Dagster supervises it."""

import os
import subprocess
from pathlib import Path

import clickhouse_connect
import httpx
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from producer.client import BASE_URL
from producer.models import StationInformation

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"
FRESHNESS_LIMIT_SECONDS = 180
MISSING_STATIONS_TOLERANCE = 0.05


def clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=8123,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


@asset(group_name="reference")
def station_information(context: AssetExecutionContext) -> None:
    """Reference table of the stations, nearly static: refreshed once a day."""
    response = httpx.get(f"{BASE_URL}/station_information.json", timeout=20.0)
    response.raise_for_status()
    stations = [
        StationInformation.model_validate(row) for row in response.json()["data"]["stations"]
    ]
    rows = [[s.station_id, s.station_code, s.name, s.lat, s.lon, s.capacity] for s in stations]
    columns = ["station_id", "station_code", "name", "lat", "lon", "capacity"]
    clickhouse().insert("velib.raw_station_information", rows, column_names=columns)
    context.add_output_metadata({"stations": len(rows)})


@asset(group_name="marts", deps=[station_information])
def dbt_marts(context: AssetExecutionContext) -> None:
    """dbt build: staging, intermediate, marts and their tests."""
    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", "."],
        cwd=DBT_DIR, capture_output=True, text=True, check=False,
    )
    context.log.info(result.stdout[-4000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt build a échoué :\n{result.stdout[-2000:]}")


@asset_check(asset=dbt_marts)
def freshness() -> AssetCheckResult:
    """The last message must be less than 3 minutes old."""
    age = clickhouse().query(
        "select dateDiff('second', max(ingested_at), now()) from velib.ingest_log"
    ).result_rows[0][0]
    return AssetCheckResult(
        passed=age <= FRESHNESS_LIMIT_SECONDS,
        severity=AssetCheckSeverity.ERROR,
        metadata={"age_seconds": int(age)},
    )


@asset_check(asset=dbt_marts)
def missing_stations() -> AssetCheckResult:
    """Stations from the reference that did not appear in the last hour."""
    known, reporting = clickhouse().query(
        """
        select
            (select uniqExact(station_id) from velib.raw_station_information),
            (select uniqExact(station_id) from velib.raw_station_status
             where fetched_at > now() - interval 1 hour)
        """
    ).result_rows[0]
    share = 1 - reporting / known if known else 1.0
    return AssetCheckResult(
        passed=share <= MISSING_STATIONS_TOLERANCE,
        severity=AssetCheckSeverity.WARN,
        metadata={"known": int(known), "reporting": int(reporting)},
    )


@asset_check(asset=dbt_marts)
def bikes_within_capacity() -> AssetCheckResult:
    """Available bikes must not exceed the capacity, within a small tolerance."""
    count = clickhouse().query(
        """
        select count()
        from marts.int_station_status_enriched
        where capacity > 0
          and num_bikes_available > capacity + 5
          and last_reported > now() - interval 1 day
        """
    ).result_rows[0][0]
    return AssetCheckResult(
        passed=count == 0, severity=AssetCheckSeverity.WARN, metadata={"violations": int(count)}
    )


reference_job = define_asset_job("reference_job", selection=["station_information"])
marts_job = define_asset_job("marts_job", selection=["dbt_marts"])

defs = Definitions(
    assets=[station_information, dbt_marts],
    asset_checks=[freshness, missing_stations, bikes_within_capacity],
    jobs=[reference_job, marts_job],
    schedules=[
        ScheduleDefinition(
            job=reference_job,
            cron_schedule="0 4 * * *",
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=marts_job,
            cron_schedule="*/5 * * * *",
            execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
