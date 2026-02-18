from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.latest_price import LatestPrice
from app.models.processed_price_event import ProcessedPriceEvent
from app.models.symbol_average import SymbolAverage
from app.schemas.events import PriceEvent
from app.services.metrics import (
    CONSUMER_PROCESS_DURATION_SECONDS,
    DB_WRITE_DURATION_SECONDS,
    EVENTS_PROCESSED_TOTAL,
    PRICE_PIPELINE_END_TO_END_SECONDS,
)


def calculate_moving_average(prices: list[Decimal], window_size: int = 5) -> Decimal | None:
    if len(prices) < window_size:
        return None
    window = prices[:window_size]
    return sum(window) / Decimal(window_size)


@dataclass
class RollingWindowState:
    window_size: int
    prices: deque[Decimal] = field(init=False)
    rolling_sum: Decimal = field(default=Decimal("0"))

    def __post_init__(self) -> None:
        self.prices = deque(maxlen=self.window_size)

    def add_price(self, price: Decimal) -> tuple[int, Decimal | None]:
        if len(self.prices) == self.window_size:
            self.rolling_sum -= self.prices.popleft()

        self.prices.append(price)
        self.rolling_sum += price

        if len(self.prices) < self.window_size:
            return len(self.prices), None

        return len(self.prices), self.rolling_sum / Decimal(self.window_size)


class MovingAverageService:
    def __init__(self, window_size: int = 5) -> None:
        self.window_size = window_size
        # In-memory rolling windows are O(1) per event but reset on consumer restart.
        self._state: dict[str, RollingWindowState] = {}

    def process_batch(self, db: Session, events: list[PriceEvent]) -> int:
        processed = 0

        with DB_WRITE_DURATION_SECONDS.time():
            for event in events:
                with CONSUMER_PROCESS_DURATION_SECONDS.time():
                    inserted = self._mark_processed(db, event)
                    if not inserted:
                        continue

                    sample_size, moving_average = self._update_state(event)
                    self._upsert_latest_price(db, event, moving_average)

                    if moving_average is not None:
                        self._upsert_symbol_average(db, event, sample_size, moving_average)

                    self._observe_pipeline_latency(event)
                    EVENTS_PROCESSED_TOTAL.inc()
                    processed += 1

            db.commit()

        return processed

    def _event_timestamp(self, event: PriceEvent) -> datetime:
        ts = event.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts

    def _mark_processed(self, db: Session, event: PriceEvent) -> bool:
        stmt = (
            insert(ProcessedPriceEvent)
            .values(
                price_point_id=event.price_point_id,
                event_id=event.event_id,
                provider=event.provider,
                symbol=event.symbol,
                event_timestamp=self._event_timestamp(event),
            )
            .on_conflict_do_nothing(index_elements=[ProcessedPriceEvent.price_point_id])
            .returning(ProcessedPriceEvent.price_point_id)
        )
        inserted_id = db.execute(stmt).scalar_one_or_none()
        return inserted_id is not None

    def _state_key(self, event: PriceEvent) -> str:
        return f"{event.provider}:{event.symbol}"

    def _update_state(self, event: PriceEvent) -> tuple[int, Decimal | None]:
        key = self._state_key(event)
        state = self._state.get(key)
        if state is None:
            state = RollingWindowState(window_size=self.window_size)
            self._state[key] = state

        price = Decimal(str(event.price))
        return state.add_price(price)

    def _upsert_symbol_average(
        self,
        db: Session,
        event: PriceEvent,
        sample_size: int,
        moving_average: Decimal,
    ) -> None:
        event_ts = self._event_timestamp(event)
        stmt = insert(SymbolAverage).values(
            symbol=event.symbol,
            provider=event.provider,
            window_size=self.window_size,
            sample_size=sample_size,
            moving_average=moving_average,
            last_price_timestamp=event_ts,
        )

        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            constraint="uq_symbol_avg_window",
            set_={
                "sample_size": sample_size,
                "moving_average": moving_average,
                "last_price_timestamp": event_ts,
            },
            where=excluded.last_price_timestamp >= SymbolAverage.last_price_timestamp,
        )
        db.execute(stmt)

    def _upsert_latest_price(self, db: Session, event: PriceEvent, moving_average: Decimal | None) -> None:
        event_ts = self._event_timestamp(event)
        stmt = insert(LatestPrice).values(
            provider=event.provider,
            symbol=event.symbol,
            price=Decimal(str(event.price)),
            timestamp=event_ts,
            moving_average_5=moving_average,
            price_point_id=event.price_point_id,
        )

        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=[LatestPrice.provider, LatestPrice.symbol],
            set_={
                "price": Decimal(str(event.price)),
                "timestamp": event_ts,
                "moving_average_5": moving_average,
                "price_point_id": event.price_point_id,
            },
            where=excluded.timestamp >= LatestPrice.timestamp,
        )
        db.execute(stmt)

    def _observe_pipeline_latency(self, event: PriceEvent) -> None:
        event_ts = self._event_timestamp(event)
        latency_seconds = max(0.0, (datetime.now(timezone.utc) - event_ts).total_seconds())
        PRICE_PIPELINE_END_TO_END_SECONDS.observe(latency_seconds)
