import logging
from datetime import timezone
from typing import Callable

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.models.price_point import PricePoint
from app.models.raw_market_data import RawMarketData
from app.models.symbol_average import SymbolAverage
from app.schemas.events import PriceEvent
from app.schemas.price import PriceLatestResponse
from app.services.cache import PriceCache
from app.services.kafka_producer import PriceEventProducer
from app.services.metrics import DB_QUERY_LATENCY_SECONDS, PROVIDER_CALL_ERRORS_TOTAL, PROVIDER_CALL_LATENCY_SECONDS
from app.services.providers.factory import ProviderFactory

logger = logging.getLogger(__name__)


class PriceIngestionService:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        provider_factory: ProviderFactory,
        producer: PriceEventProducer,
        cache: PriceCache,
    ) -> None:
        self._session_factory = session_factory
        self._provider_factory = provider_factory
        self._producer = producer
        self._cache = cache

    def fetch_store_publish(self, symbol: str, provider: str | None = None) -> PriceLatestResponse:
        symbol_normalized = symbol.strip().upper()
        provider_client = self._provider_factory.get_client(provider)
        provider_name = provider_client.name

        with PROVIDER_CALL_LATENCY_SECONDS.labels(provider=provider_name).time():
            try:
                quote = provider_client.get_latest_price(symbol_normalized)
            except Exception:
                PROVIDER_CALL_ERRORS_TOTAL.labels(provider=provider_name).inc()
                raise

        quote_timestamp = quote.timestamp
        if quote_timestamp.tzinfo is None:
            quote_timestamp = quote_timestamp.replace(tzinfo=timezone.utc)

        with self._session_factory() as db:
            with DB_QUERY_LATENCY_SECONDS.labels(operation="insert_raw_and_price").time():
                raw_row = RawMarketData(
                    symbol=symbol_normalized,
                    provider=provider_name,
                    payload=quote.raw_payload,
                    http_status=quote.http_status,
                )
                db.add(raw_row)
                db.flush()

                price_point = PricePoint(
                    symbol=symbol_normalized,
                    provider=provider_name,
                    price=quote.price,
                    as_of=quote_timestamp,
                    raw_response_id=raw_row.id,
                )
                db.add(price_point)
                db.flush()

                moving_average = db.execute(
                    select(SymbolAverage.moving_average).where(
                        SymbolAverage.symbol == symbol_normalized,
                        SymbolAverage.provider == provider_name,
                        SymbolAverage.window_size == 5,
                    )
                ).scalar_one_or_none()

                db.commit()

        event = PriceEvent(
            symbol=symbol_normalized,
            provider=provider_name,
            price=quote.price,
            timestamp=quote_timestamp,
            raw_response_id=raw_row.id,
            price_point_id=price_point.id,
        )
        self._producer.send_event(event)

        response = PriceLatestResponse(
            symbol=symbol_normalized,
            provider=provider_name,
            price=quote.price,
            timestamp=quote_timestamp,
            moving_average_5=moving_average,
            cached=False,
        )

        self._cache.set_latest(
            symbol=symbol_normalized,
            provider=provider_name,
            payload=response.model_dump(mode="json"),
        )

        return response

    def read_cached(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        cached = self._cache.get_latest(symbol=symbol, provider=provider)
        if not cached:
            return None

        try:
            return PriceLatestResponse.model_validate({**cached, "cached": True})
        except Exception:
            logger.warning("Invalid cache payload ignored")
            return None

    def read_latest_persisted(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        symbol_normalized = symbol.strip().upper()
        provider_normalized = provider.strip().lower()

        with self._session_factory() as db:
            row = db.execute(
                select(PricePoint.price, PricePoint.as_of)
                .where(
                    PricePoint.symbol == symbol_normalized,
                    PricePoint.provider == provider_normalized,
                )
                .order_by(desc(PricePoint.as_of))
                .limit(1)
            ).first()
            if row is None:
                return None

            moving_average = db.execute(
                select(SymbolAverage.moving_average).where(
                    SymbolAverage.symbol == symbol_normalized,
                    SymbolAverage.provider == provider_normalized,
                    SymbolAverage.window_size == 5,
                )
            ).scalar_one_or_none()

            return PriceLatestResponse(
                symbol=symbol_normalized,
                provider=provider_normalized,
                price=row.price,
                timestamp=row.as_of,
                moving_average_5=moving_average,
                cached=True,
            )
