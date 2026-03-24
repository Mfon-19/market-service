"""Request and response schemas used by price and polling endpoints."""

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PriceLatestResponse(BaseModel):
    """Serialized latest-price payload returned by the API."""

    symbol: str
    provider: str
    price: Decimal
    timestamp: datetime
    moving_average_5: Decimal | None = None
    cached: bool = False


class PollJobCreateRequest(BaseModel):
    """Payload for creating a recurring symbol polling job."""

    symbols: list[str]
    interval_seconds: int = Field(default=60, alias="interval")
    provider: str = "yahoo"

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        """Normalize symbols and require at least one unique entry.

        Args:
            value: Raw symbols supplied by the client.

        Returns:
            list[str]: Uppercased, deduplicated symbols.
        """
        normalized = [symbol.strip().upper() for symbol in value if symbol and symbol.strip()]
        unique = sorted(set(normalized))
        if not unique:
            raise ValueError("at least one symbol is required")
        return unique

    @field_validator("provider")
    @classmethod
    def normalize_provider(cls, value: str) -> str:
        """Normalize provider names into the canonical lowercase form.

        Args:
            value: Raw provider name supplied by the client.

        Returns:
            str: Normalized provider identifier.
        """
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("provider must not be empty")
        return normalized

    @field_validator("interval_seconds")
    @classmethod
    def validate_interval(cls, value: int) -> int:
        """Require a positive polling interval.

        Args:
            value: Requested polling interval in seconds.

        Returns:
            int: Validated polling interval.
        """
        if value <= 0:
            raise ValueError("interval must be greater than 0")
        return value


class PollJobAcceptedResponse(BaseModel):
    """Response returned when a polling job has been accepted."""

    job_id: uuid.UUID
    status: str = "accepted"


class PollJobStatusResponse(BaseModel):
    """Response describing the persisted state of a polling job."""

    job_id: uuid.UUID
    provider: str
    symbols: list[str]
    interval_seconds: int
    is_active: bool
    created_at: datetime
    last_run_at: datetime | None = None
