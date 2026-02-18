from decimal import Decimal

from sqlalchemy import desc, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.price_point import PricePoint
from app.models.symbol_average import SymbolAverage
from app.schemas.events import PriceEvent
from app.services.metrics import DB_QUERY_LATENCY_SECONDS


def calculate_moving_average(prices: list[Decimal], window_size: int = 5) -> Decimal | None:
    if len(prices) < window_size:
        return None
    window = prices[:window_size]
    return sum(window) / Decimal(window_size)


class MovingAverageService:
    def __init__(self, window_size: int = 5) -> None:
        self.window_size = window_size

    def process_event(self, db: Session, event: PriceEvent) -> Decimal | None:
        with DB_QUERY_LATENCY_SECONDS.labels(operation="fetch_last_price_points").time():
            rows = db.execute(
                select(PricePoint.price, PricePoint.as_of)
                .where(
                    PricePoint.symbol == event.symbol,
                    PricePoint.provider == event.provider,
                )
                .order_by(desc(PricePoint.as_of))
                .limit(self.window_size)
            ).all()

        sample_size = len(rows)
        prices = [Decimal(str(row.price)) for row in rows]
        average = calculate_moving_average(prices, window_size=self.window_size)
        if average is None:
            return None

        latest_ts = rows[0].as_of

        stmt = insert(SymbolAverage).values(
            symbol=event.symbol,
            provider=event.provider,
            window_size=self.window_size,
            sample_size=sample_size,
            moving_average=average,
            last_price_timestamp=latest_ts,
        )

        stmt = stmt.on_conflict_do_update(
            constraint="uq_symbol_avg_window",
            set_={
                "sample_size": sample_size,
                "moving_average": average,
                "last_price_timestamp": latest_ts,
            },
        )

        with DB_QUERY_LATENCY_SECONDS.labels(operation="upsert_symbol_average").time():
            db.execute(stmt)
            db.commit()

        return average
