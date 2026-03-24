"""Core ingestion workflow for fetching, storing, caching, and publishing prices."""

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
    """Coordinates provider reads, database writes, caching, and Kafka publishing."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        provider_factory: ProviderFactory,
        producer: PriceEventProducer,
        cache: PriceCache,
        settings: Settings,
    ) -> None:
        """Build an ingestion service from the process-wide collaborators.

        Args:
            session_factory: Factory that creates database sessions on demand.
            provider_factory: Factory that resolves provider clients.
            producer: Kafka producer used to publish normalized price events.
            cache: Cache adapter for latest-price responses.
            settings: Runtime settings controlling fallback behavior.
        """
        self._session_factory = session_factory
        self._provider_factory = provider_factory
        self._producer = producer
        self._cache = cache
        self._settings = settings

    def _call_provider(self, symbol: str, provider: str) -> tuple[ProviderQuote, str]:
        """Fetch a quote from one provider while recording provider metrics.

        Args:
            symbol: Market symbol to request.
            provider: Provider identifier to resolve.

        Returns:
            tuple[ProviderQuote, str]: Retrieved quote and the provider name used.
        """
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
        """Fetch a quote, optionally retrying with a fallback provider on rate limit.

        Args:
            symbol: Market symbol to request.
            requested_provider: Preferred provider identifier.

        Returns:
            tuple[ProviderQuote, str]: Retrieved quote and the provider name that served it.

        Raises:
            ProviderRateLimitError: If the primary provider is rate-limited and no fallback succeeds.
        """
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
        """Fetch a fresh quote, persist it, publish an event, and warm the cache.

        Args:
            symbol: Market symbol to fetch.
            provider: Preferred provider identifier, or `None` for the default.

        Returns:
            PriceLatestResponse: Latest price response built from the persisted quote.
        """
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
        """Read the latest-price response from cache when available.

        Args:
            symbol: Market symbol to look up.
            provider: Requested provider identifier.

        Returns:
            PriceLatestResponse | None: Cached response payload, or `None` on miss.
        """
        cached = self._cache.get_latest(symbol=symbol, provider=provider)
        if not cached:
            return None

        try:
            return PriceLatestResponse.model_validate({**cached, "cached": True})
        except Exception:
            logger.warning("Invalid cache payload ignored")
            return None

    def _latest_provider_candidates(self, provider: str) -> list[str]:
        """Return provider names that may contain the freshest persisted price.

        Args:
            provider: Requested provider identifier.

        Returns:
            list[str]: Candidate providers checked in priority order.
        """
        provider_normalized = provider.strip().lower()
        candidates = [provider_normalized]

        if self._settings.enable_yahoo_rate_limit_fallback and provider_normalized == "yahoo":
            fallback_provider = self._settings.yahoo_rate_limit_fallback_provider.strip().lower()
            if fallback_provider and fallback_provider not in candidates:
                candidates.append(fallback_provider)

        return candidates

    def read_latest_from_store(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        """Read the latest persisted price projection from the database.

        Args:
            symbol: Market symbol to look up.
            provider: Requested provider identifier.

        Returns:
            PriceLatestResponse | None: Persisted latest-price projection, or `None`.
        """
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
        """Populate cache entries for both the requested and resolved providers.

        Args:
            requested_provider: Provider name requested by the caller.
            response: Response payload to cache.
        """
        payload = response.model_dump(mode="json")
        cache_keys = {requested_provider.strip().lower(), response.provider.strip().lower()}
        for cache_provider in cache_keys:
            self._cache.set_latest(symbol=response.symbol, provider=cache_provider, payload=payload)

    def read_latest_persisted(self, symbol: str, provider: str) -> PriceLatestResponse | None:
        """Return the latest persisted value as a rate-limit fallback read.

        Args:
            symbol: Market symbol to look up.
            provider: Requested provider identifier.

        Returns:
            PriceLatestResponse | None: Persisted latest-price projection, or `None`.
        """
        return self.read_latest_from_store(symbol=symbol, provider=provider)
