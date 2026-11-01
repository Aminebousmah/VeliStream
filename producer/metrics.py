"""Counters of the producer, exposed in its logs at each cycle."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class Metrics:
    cycles: int = 0
    fetch_errors: int = 0
    published: int = 0
    dead_lettered: int = 0
    delivery_errors: int = 0
    last_cycle_seconds: float = 0.0

    def as_dict(self) -> dict[str, float]:
        return asdict(self)
