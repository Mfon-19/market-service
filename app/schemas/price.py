import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PriceLatestResponse(BaseModel):
    symbol: str
    provider: str
    price: Decimal
    timestamp: datetime
    moving_average_5: Decimal | None = None
    cached: bool = False


class PollJobCreateRequest(BaseModel):
    symbols: list[str]
    interval_seconds: int = Field(default=60, alias="interval")
    provider: str = "yahoo"

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[str]) -> list[str]:
        normalized = [symbol.strip().upper() for symbol in value if symbol and symbol.strip()]
        unique = sorted(set(normalized))
        if not unique:
            raise ValueError("at least one symbol is required")
        return unique

    @field_validator("provider")
    @classmethod
    def normalize_provider(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("provider must not be empty")
        return normalized

    @field_validator("interval_seconds")
    @classmethod
    def validate_interval(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("interval must be greater than 0")
        return value


class PollJobAcceptedResponse(BaseModel):
    job_id: uuid.UUID
    status: str = "accepted"


class PollJobStatusResponse(BaseModel):
    job_id: uuid.UUID
    provider: str
    symbols: list[str]
    interval_seconds: int
    is_active: bool
    created_at: datetime
    last_run_at: datetime | None = None
