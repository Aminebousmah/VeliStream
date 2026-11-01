"""Feed client, with retries, exponential backoff and jitter."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

logger = logging.getLogger("velistream.client")

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FeedError(Exception):
    """The feed stayed unavailable after every retry."""


def backoff_delay(
    attempt: int, base: float = 2.0, cap: float = 30.0, rng: random.Random | None = None
) -> float:
    """Full jitter: uniform between 0 and the capped exponential delay."""
    source = rng or random
    return source.uniform(0, min(cap, base * 2**attempt))


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    max_retries: int = 4,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """GET a JSON document. Retries cover the outage, but stay inside one collection cycle."""
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url)
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
                return response.json()
            last_error = httpx.HTTPStatusError(
                f"status {response.status_code}", request=response.request, response=response
            )
        except (httpx.TransportError, ValueError) as error:
            last_error = error
        except httpx.HTTPStatusError as error:  # a 4xx will not get better by retrying
            raise FeedError(str(error)) from error
        if attempt < max_retries:
            delay = backoff_delay(attempt)
            logger.warning("tentative %d échouée, nouvel essai dans %.1f s", attempt + 1, delay)
            await sleep(delay)
    raise FeedError(f"flux indisponible : {last_error}")
