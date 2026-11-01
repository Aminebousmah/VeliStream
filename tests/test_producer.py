import json
import random
from datetime import UTC, datetime

import httpx
import pytest

from producer.__main__ import DLQ_TOPIC, STATUS_TOPIC, publish_payload
from producer.client import FeedError, backoff_delay, fetch_json
from producer.metrics import Metrics
from producer.models import StationInformation, StationStatus, parse_statuses

ROW = {
    "station_id": 213688169,
    "stationCode": "16107",
    "num_bikes_available": 13,
    "num_bikes_available_types": [{"mechanical": 11}, {"ebike": 2}],
    "num_docks_available": 22,
    "is_installed": 1,
    "is_returning": 1,
    "is_renting": 1,
    "last_reported": 1793554216,
}


class FakeBus:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value, key=None, **kwargs):
        self.sent.append((topic, key, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


class TestValidation:
    def test_real_feed_shape_is_flattened(self):
        status = StationStatus.model_validate(ROW)
        assert status.num_mechanical == 11
        assert status.num_ebike == 2
        assert status.station_code == "16107"
        assert status.is_renting is True

    def test_message_keeps_the_logical_key_fields(self):
        message = json.loads(StationStatus.model_validate(ROW).to_message())
        assert message["station_id"] == 213688169
        assert message["last_reported"] == 1793554216
        assert message["station_code"] == "16107"

    @pytest.mark.parametrize(
        "broken",
        [
            {"num_bikes_available": -1},
            {"num_docks_available": -3},
            {"last_reported": 0},
            {"station_id": "pas-un-nombre"},
        ],
    )
    def test_invalid_rows_are_rejected(self, broken):
        valid, rejected = parse_statuses({"data": {"stations": [{**ROW, **broken}]}})
        assert valid == []
        assert len(rejected) == 1
        assert rejected[0]["errors"]

    def test_degraded_cycle_really_sent_by_the_feed_is_rejected(self):
        """Observed on 2026-11-01: null station codes and empty bike types on every station."""
        degraded = {**ROW, "stationCode": None, "num_bikes_available_types": [{}, {}]}
        valid, rejected = parse_statuses({"data": {"stations": [degraded]}})
        assert valid == []
        assert len(rejected) == 1

    def test_empty_bike_types_are_rejected_even_with_a_station_code(self):
        degraded = {**ROW, "num_bikes_available_types": [{}, {}]}
        valid, rejected = parse_statuses({"data": {"stations": [degraded]}})
        assert valid == []
        assert len(rejected) == 1

    def test_missing_field_is_rejected_without_losing_the_others(self):
        incomplete = {k: v for k, v in ROW.items() if k != "stationCode"}
        valid, rejected = parse_statuses({"data": {"stations": [ROW, incomplete]}})
        assert len(valid) == 1
        assert len(rejected) == 1

    def test_fetch_time_is_recorded(self):
        moment = datetime(2026, 1, 1, tzinfo=UTC)
        valid, _ = parse_statuses({"data": {"stations": [ROW]}}, moment)
        assert valid[0].fetched_at == int(moment.timestamp())

    def test_station_information_rejects_bad_coordinates(self):
        with pytest.raises(ValueError):
            StationInformation.model_validate(
                {"station_id": 1, "stationCode": "1", "name": "x", "lat": 200, "lon": 2,
                 "capacity": 10}
            )


class TestPublishing:
    def test_valid_rows_go_to_the_topic_keyed_by_station(self):
        bus, metrics = FakeBus(), Metrics()
        publish_payload(bus, {"data": {"stations": [ROW]}}, metrics)
        topic, key, _ = bus.sent[0]
        assert topic == STATUS_TOPIC
        assert key == "213688169"
        assert metrics.published == 1

    def test_invalid_rows_go_to_the_dead_letter_topic(self):
        bus, metrics = FakeBus(), Metrics()
        publish_payload(
            bus, {"data": {"stations": [ROW, {**ROW, "num_bikes_available": -1}]}}, metrics
        )
        assert [topic for topic, _, _ in bus.sent] == [STATUS_TOPIC, DLQ_TOPIC]
        assert metrics.dead_lettered == 1

    def test_empty_feed_publishes_nothing(self):
        bus, metrics = FakeBus(), Metrics()
        publish_payload(bus, {"data": {"stations": []}}, metrics)
        assert bus.sent == []


class TestRetries:
    def test_backoff_stays_within_the_capped_exponential(self):
        rng = random.Random(1)
        for attempt in range(8):
            assert 0 <= backoff_delay(attempt, rng=rng) <= min(30.0, 2.0 * 2**attempt)

    def test_backoff_has_jitter(self):
        rng = random.Random(1)
        assert len({backoff_delay(5, rng=rng) for _ in range(5)}) > 1

    @staticmethod
    async def no_sleep(_delay):
        return None

    async def test_retries_then_succeeds(self):
        responses = iter(
            [httpx.Response(503), httpx.Response(200, json={"data": {"stations": []}})]
        )
        transport = httpx.MockTransport(lambda request: next(responses))
        async with httpx.AsyncClient(transport=transport) as client:
            payload = await fetch_json(client, "https://feed.test/x", sleep=self.no_sleep)
        assert payload == {"data": {"stations": []}}

    async def test_gives_up_after_all_retries(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(503)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(FeedError):
                await fetch_json(client, "https://feed.test/x", max_retries=2, sleep=self.no_sleep)
        assert len(calls) == 3

    async def test_client_errors_are_not_retried(self):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(FeedError):
                await fetch_json(client, "https://feed.test/x", sleep=self.no_sleep)
        assert len(calls) == 1

    async def test_invalid_json_is_retried(self):
        responses = iter(
            [httpx.Response(200, content=b"<html>"), httpx.Response(200, json={"ok": True})]
        )
        transport = httpx.MockTransport(lambda request: next(responses))
        async with httpx.AsyncClient(transport=transport) as client:
            assert await fetch_json(client, "https://feed.test/x", sleep=self.no_sleep) == {
                "ok": True
            }
