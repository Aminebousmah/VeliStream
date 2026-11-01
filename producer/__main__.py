"""Collection loop: `python -m producer --interval 60`."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx
from confluent_kafka import Producer

from producer.client import BASE_URL, FeedError, fetch_json
from producer.metrics import Metrics
from producer.models import parse_statuses

logger = logging.getLogger("velistream.producer")

STATUS_TOPIC = "velib.station_status"
DLQ_TOPIC = "velib.dlq"
MIN_INTERVAL_SECONDS = 60  # the open data licence asks for at most one call per minute and feed


class MessageBus(Protocol):
    def produce(self, topic: str, value: str, key: str | None = None, **kwargs: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def publish_payload(
    bus: MessageBus, payload: dict[str, Any], metrics: Metrics, fetched_at: datetime | None = None
) -> None:
    """Valid statuses go to the main topic, invalid rows to the dead letter topic."""

    def on_delivery(error: Any, _message: Any) -> None:
        if error is not None:
            metrics.delivery_errors += 1
            logger.error("échec de livraison : %s", error)

    valid, rejected = parse_statuses(payload, fetched_at)
    for status in valid:
        bus.produce(STATUS_TOPIC, status.to_message(), key=status.key(), on_delivery=on_delivery)
        bus.poll(0)
    for row in rejected:
        bus.produce(DLQ_TOPIC, json.dumps(row, default=str), on_delivery=on_delivery)
    metrics.published += len(valid)
    metrics.dead_lettered += len(rejected)


async def run(
    bus: MessageBus, interval: float, metrics: Metrics, cycles: int | None = None
) -> None:
    url = f"{BASE_URL}/station_status.json"
    async with httpx.AsyncClient(timeout=20.0) as client:
        while cycles is None or metrics.cycles < cycles:
            started = time.monotonic()
            metrics.cycles += 1
            try:
                payload = await fetch_json(client, url)
                publish_payload(bus, payload, metrics, datetime.now(UTC))
                bus.flush(10.0)
            except FeedError as error:
                metrics.fetch_errors += 1
                logger.error("cycle ignoré : %s", error)
            metrics.last_cycle_seconds = round(time.monotonic() - started, 3)
            logger.info("cycle terminé %s", metrics.as_dict())
            if cycles is not None and metrics.cycles >= cycles:
                break
            await asyncio.sleep(max(interval - (time.monotonic() - started), 0))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=MIN_INTERVAL_SECONDS)
    parser.add_argument("--cycles", type=int, default=None, help="Stop after N cycles.")
    args = parser.parse_args()
    if args.interval < MIN_INTERVAL_SECONDS:
        parser.error(f"--interval doit valoir au moins {MIN_INTERVAL_SECONDS} s")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bus = Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            "enable.idempotence": True,
            "linger.ms": 200,
            "compression.type": "zstd",
        }
    )
    asyncio.run(run(bus, args.interval, Metrics(), args.cycles))


if __name__ == "__main__":
    main()
