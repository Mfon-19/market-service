from datetime import datetime, timezone
from decimal import Decimal

from app.services.metrics import PROVIDER_RATE_LIMIT_HITS_TOTAL
from app.services.providers.base import ProviderClient, ProviderQuote
from app.services.providers.exceptions import ProviderError, ProviderRateLimitError
from app.services.providers.http import request_with_backoff
from app.services.rate_limiter import MinuteRateLimiter


class AlphaVantageProvider(ProviderClient):
    name = "alpha_vantage"

    def __init__(
        self,
        api_key: str,
        timeout_seconds: int,
        rate_limiter: MinuteRateLimiter,
        http_max_retries: int,
        http_backoff_seconds: float,
    ) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter
        self.http_max_retries = max(0, http_max_retries)
        self.http_backoff_seconds = max(0.1, http_backoff_seconds)

    def get_latest_price(self, symbol: str) -> ProviderQuote:
        if not self.api_key:
            raise ProviderError("alpha vantage API key is not configured")

        waited = self.rate_limiter.acquire()
        if waited > 0:
            PROVIDER_RATE_LIMIT_HITS_TOTAL.labels(provider=self.name).inc()

        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        response = request_with_backoff(
            self.name,
            symbol,
            "https://www.alphavantage.co/query",
            params=params,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.http_max_retries,
            base_backoff_seconds=self.http_backoff_seconds,
        )

        payload = response.json()
        if "Note" in payload:
            raise ProviderRateLimitError(self.name, symbol, message=payload.get("Note"))

        quote = payload.get("Global Quote")
        if not quote:
            raise ProviderError(f"alpha vantage returned no quote for symbol {symbol}")

        price_value = quote.get("05. price")
        latest_day = quote.get("07. latest trading day")
        if price_value is None:
            raise ProviderError(f"alpha vantage returned no price for symbol {symbol}")

        if latest_day:
            quote_ts = datetime.fromisoformat(latest_day).replace(tzinfo=timezone.utc)
        else:
            quote_ts = datetime.now(timezone.utc)

        return ProviderQuote(
            symbol=symbol,
            provider=self.name,
            price=Decimal(str(price_value)),
            timestamp=quote_ts,
            raw_payload=payload,
            http_status=response.status_code,
        )
