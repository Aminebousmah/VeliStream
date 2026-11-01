"""Message schema. Every status is validated here before being sent."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class StationStatus(BaseModel):
    """One station at one reporting time. Logical key: (station_id, last_reported)."""

    model_config = ConfigDict(extra="ignore")

    station_id: int = Field(gt=0)
    station_code: str = Field(alias="stationCode", min_length=1)
    num_bikes_available: int = Field(ge=0)
    num_mechanical: int = Field(default=0, ge=0)
    num_ebike: int = Field(default=0, ge=0)
    num_docks_available: int = Field(ge=0)
    is_installed: bool
    is_renting: bool
    is_returning: bool
    last_reported: int = Field(gt=0)
    fetched_at: int = Field(default=0, ge=0)

    @model_validator(mode="before")
    @classmethod
    def flatten_bike_types(cls, raw: Any) -> Any:
        if isinstance(raw, dict) and "num_bikes_available_types" in raw:
            merged: dict[str, int] = {}
            for entry in raw["num_bikes_available_types"]:
                merged.update(entry)
            raw = {
                **raw,
                "num_mechanical": merged.get("mechanical", 0),
                "num_ebike": merged.get("ebike", 0),
            }
        return raw

    @model_validator(mode="after")
    def bike_types_add_up(self) -> StationStatus:
        """The feed has already sent empty type lists with non-zero bikes: never trust that."""
        if self.num_mechanical + self.num_ebike != self.num_bikes_available:
            raise ValueError("les types de vélos ne somment pas au total disponible")
        return self

    def key(self) -> str:
        return str(self.station_id)

    def to_message(self) -> str:
        return self.model_dump_json(by_alias=False)


class StationInformation(BaseModel):
    model_config = ConfigDict(extra="ignore")

    station_id: int = Field(gt=0)
    station_code: str = Field(alias="stationCode")
    name: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    capacity: int = Field(ge=0)


def parse_statuses(
    payload: dict[str, Any], fetched_at: datetime | None = None
) -> tuple[list[StationStatus], list[dict[str, Any]]]:
    """Split a feed into valid statuses and rejected rows for the dead letter topic."""
    moment = int((fetched_at or datetime.now(UTC)).timestamp())
    valid: list[StationStatus] = []
    rejected: list[dict[str, Any]] = []
    for row in payload.get("data", {}).get("stations", []):
        try:
            valid.append(StationStatus.model_validate({**row, "fetched_at": moment}))
        except ValidationError as error:
            errors = error.errors(include_url=False, include_input=False)
            rejected.append({"row": row, "errors": errors})
    return valid, rejected
