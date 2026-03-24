"""Raw provider response model used for auditing and replay support."""

import uuid

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class RawMarketData(Base):
    """Stores the original provider payload for a fetched market quote."""

    __tablename__ = "raw_market_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    http_status: Mapped[int] = mapped_column(Integer, nullable=False, default=200)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    fetched_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_raw_market_data_symbol_fetched_at", "symbol", "fetched_at"),
        Index("ix_raw_market_data_provider_fetched_at", "provider", "fetched_at"),
    )
