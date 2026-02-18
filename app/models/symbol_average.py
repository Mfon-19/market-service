import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Integer, Numeric, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class SymbolAverage(Base):
    __tablename__ = "symbol_averages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    window_size: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    moving_average: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    last_price_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("symbol", "provider", "window_size", name="uq_symbol_avg_window"),
        Index("ix_symbol_averages_symbol_provider", "symbol", "provider"),
    )
