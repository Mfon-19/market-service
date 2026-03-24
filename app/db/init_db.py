"""Database bootstrap helpers for table creation and performance indexes."""

from sqlalchemy import text

from app.db.session import engine
from app.models.base import Base
from app.models import (  # noqa: F401
    latest_price,
    polling_job,
    price_point,
    processed_price_event,
    raw_market_data,
    symbol_average,
)


def _ensure_perf_indexes() -> None:
    """Create non-blocking performance indexes required by hot query paths."""
    ddl_statements = [
        (
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_points_ps_ts_desc_inc "
            "ON price_points (provider, symbol, as_of DESC) INCLUDE (price)"
        ),
    ]

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))


def init_db() -> None:
    """Create mapped tables and ensure supplemental indexes exist."""
    Base.metadata.create_all(bind=engine)
    _ensure_perf_indexes()
