"""Redis-backed cache helpers for the latest-price read path."""

import json
import logging
from datetime import datetime
from decimal import Decimal

import redis

from app.core.config import Settings
from app.services.metrics import CACHE_HITS_TOTAL, CACHE_MISSES_TOTAL

logger = logging.getLogger(__name__)


class PriceCache:
    """Provides a best-effort Redis cache for latest price responses."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the cache client when Redis caching is enabled.

        Args:
            settings: Runtime settings containing Redis and TTL configuration.
        """
        self.enabled = bool(settings.use_redis_cache and settings.redis_url)
        self.ttl = settings.cache_ttl_seconds
        self._client: redis.Redis | None = None

        if not self.enabled:
            return

        try:
            self._client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
            self._client.ping()
        except redis.RedisError:
            logger.warning("Redis not available at startup, cache disabled")
            self._client = None
            self.enabled = False

    @staticmethod
    def _key(symbol: str, provider: str) -> str:
        """Build the Redis key for a symbol/provider pair.

        Args:
            symbol: Market symbol.
            provider: Provider identifier.

        Returns:
            str: Cache key used for the latest-price entry.
        """
        return f"latest:{provider.lower()}:{symbol.upper()}"

    def get_latest(self, symbol: str, provider: str) -> dict | None:
        """Read a cached latest-price payload from Redis.

        Args:
            symbol: Market symbol to look up.
            provider: Provider identifier to look up.

        Returns:
            dict | None: Cached JSON payload when available, otherwise `None`.
        """
        if not self.enabled or self._client is None:
            return None

        try:
            raw = self._client.get(self._key(symbol, provider))
            if raw is None:
                CACHE_MISSES_TOTAL.inc()
                return None
            CACHE_HITS_TOTAL.inc()
            return json.loads(raw)
        except redis.RedisError:
            logger.exception("Redis get failed")
            return None

    def set_latest(self, symbol: str, provider: str, payload: dict) -> None:
        """Store a latest-price payload in Redis with normalized JSON types.

        Args:
            symbol: Market symbol used in the cache key.
            provider: Provider identifier used in the cache key.
            payload: Response payload to serialize into Redis.
        """
        if not self.enabled or self._client is None:
            return

        normalized: dict[str, str | bool | None] = {}
        for key, value in payload.items():
            if isinstance(value, Decimal):
                normalized[key] = str(value)
            elif isinstance(value, datetime):
                normalized[key] = value.isoformat()
            else:
                normalized[key] = value

        try:
            self._client.setex(
                self._key(symbol, provider),
                self.ttl,
                json.dumps(normalized),
            )
        except redis.RedisError:
            logger.exception("Redis set failed")

    def close(self) -> None:
        """Close the Redis client if one was created."""
        if self._client is not None:
            self._client.close()
