import logging
from datetime import timezone
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.latest_price import LatestPrice
from app.models.price_point import PricePoint
from app.models.raw_market_data import RawMarketData
from app.models.symbol_average import SymbolAverage
from app.schemas.events import PriceEvent
from app.schemas.price import PriceLatestResponse
from app.services.cache import PriceCache
from app.services.kafka_producer import PriceEventProducer
from app.services.metrics import DB_QUERY_LATENCY_SECONDS, PROVIDER_CALL_ERRORS_TOTAL, PROVIDER_CALL_LATENCY_SECONDS
from app.services.providers.factory import ProviderFactory
from app.services.providers.base import ProviderQuote
from app.services.providers.exceptions import ProviderRateLimitError

logger = logging.getLogger(__name__)


class PriceIngestionService:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        provider_factory: ProviderFactory,
        producer: PriceEventProducer,
        cache: PriceCache,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._provider_factory = provider_factory
        self._producer = producer
        self._cache = cache
        self._settings = settings

    def _call_provider(self, symbol: str, provider: str) -> tuple[ProviderQuote, str]:
        provider_client = self._provider_factory.get_client(provider)
        provider_name = provider_client.name

        with PROVIDER_CALL_LATENCY_SECONDS.labels(provider=provider_name).time():
            try:
                quote = provider_client.get_latest_price(symbol)
            except Exception:
                PROVIDER_CALL_ERRORS_TOTAL.labels(provider=provider_name).inc()
                raise
        return quote, provider_name

    def _fetch_quote_with_fallback(self, symbol: str, requested_provider: str) -> tuple[ProviderQuote, str]:
        try:
            return self._call_provider(symbol=symbol, provider=requested_provider)
        except ProviderRateLimitError as primary_exc:
            fallback_enabled = (
                self._settings.enable_yahoo_rate_limit_fallback
                and requested_provider == "yahoo"
            )
            if not fallback_enabled:
                raise

            fallback_provider = self._settings.yahoo_rate_limit_fallback_provider.strip().lower()
            if not fallback_provider or fallback_provider == requested_provider:
                raise

            logger.warning(
                "Primary provider %s rate-limited for %s, attempting fallback provider %s",
                requested_provider,
                symbol,
                fallback_provider,
            )

            try:
                return self._call_provider(symbol=symbol, provider=fallback_provider)
            except Exception:
                logger.exception(
                    "Fallback provider %s failed after %s rate limit for %s",
                    fallback_provider,
                    requested_provider,
                    symbol,
                )
                raise primary_exc

    def fetch_store_publish(self, symbol: str, provider: str | None = None) -> PriceLatestResponse:
        symbol_normalized = symbol.strip().upper()
        requested_provider_name = self._provider_factory.get_client(provider).name
        quote, provider_name = self._fetch_quote_with_fallback(
            symbol=symbol_normalized,
            requested_provider=requested_provider_name,
        )

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

        payload = response.model_dump(mode="json")
        cache_provider_keys = {requested_provider_name, provider_name}
        for cache_provider in cache_provider_keys:
            self._cache.set_latest(
                symbol=symbol_normalized,
                provider=cache_provider,
                payload=payload,
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

    def _latest_provider_candidates(self, provider: str) -> list[str]:
        provider_normalized = provider.strip().lower()
        candidates = [provider_normalized]

        if self._settings.enable_yahoo_rate_limit_fallback and provider_normalized == "yahoo":
            fallback_provider = self._settings.yahoo_rate_limit_fallback_provider.strip().lower()
            if fallback_provider and fallback_provider not in candidates:
                candidates.append(fallback_provider)

        return candidates

    def read_latest_from_store(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        symbol_normalized = symbol.strip().upper()
        candidates = self._latest_provider_candidates(provider)

        with self._session_factory() as db:
            latest_row: LatestPrice | None = None
            for candidate in candidates:
                row = db.get(LatestPrice, (candidate, symbol_normalized))
                if row is None:
                    continue
                if latest_row is None or row.timestamp > latest_row.timestamp:
                    latest_row = row

            if latest_row is None:
                return None

            return PriceLatestResponse(
                symbol=latest_row.symbol,
                provider=latest_row.provider,
                price=latest_row.price,
                timestamp=latest_row.timestamp,
                moving_average_5=latest_row.moving_average_5,
                cached=False,
            )

    def warm_cache(self, requested_provider: str, response: PriceLatestResponse) -> None:
        payload = response.model_dump(mode="json")
        cache_keys = {requested_provider.strip().lower(), response.provider.strip().lower()}
        for cache_provider in cache_keys:
            self._cache.set_latest(symbol=response.symbol, provider=cache_provider, payload=payload)

    def read_latest_persisted(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        return self.read_latest_from_store(symbol=symbol, provider=provider)
