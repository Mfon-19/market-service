import json
import logging
from datetime import datetime
from decimal import Decimal

import redis

from app.core.config import Settings
from app.services.metrics import CACHE_HITS_TOTAL, CACHE_MISSES_TOTAL

logger = logging.getLogger(__name__)


class PriceCache:
    def __init__(self, settings: Settings) -> None:
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
        return f"latest:{provider.lower()}:{symbol.upper()}"

    def get_latest(self, symbol: str, provider: str) -> dict | None:
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
        if self._client is not None:
            self._client.close()
