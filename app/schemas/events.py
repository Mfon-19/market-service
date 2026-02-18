import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class PriceEvent(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    symbol: str
    provider: str
    price: Decimal
    timestamp: datetime
    raw_response_id: uuid.UUID
    price_point_id: uuid.UUID
