from datetime import datetime, timezone
from decimal import Decimal

from app.services.metrics import PROVIDER_RATE_LIMIT_HITS_TOTAL
from app.services.providers.base import ProviderClient, ProviderQuote
from app.services.providers.exceptions import ProviderError, ProviderRateLimitError
from app.services.providers.http import request_with_backoff
from app.services.rate_limiter import MinuteRateLimiter


class FinnhubProvider(ProviderClient):
    name = "finnhub"

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
            raise ProviderError("finnhub API key is not configured")

        waited = self.rate_limiter.acquire()
        if waited > 0:
            PROVIDER_RATE_LIMIT_HITS_TOTAL.labels(provider=self.name).inc()

        params = {
            "symbol": symbol,
            "token": self.api_key,
        }
        response = request_with_backoff(
            self.name,
            symbol,
            "https://finnhub.io/api/v1/quote",
            params=params,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.http_max_retries,
            base_backoff_seconds=self.http_backoff_seconds,
        )

        payload = response.json()
        api_error = payload.get("error")
        if api_error:
            if "limit" in str(api_error).lower():
                raise ProviderRateLimitError(self.name, symbol, message=str(api_error))
            raise ProviderError(f"finnhub returned error for {symbol}: {api_error}")

        current_price = payload.get("c")
        if current_price in (None, 0):
            raise ProviderError(f"finnhub returned invalid price for symbol {symbol}")

        timestamp = payload.get("t")
        if timestamp:
            quote_ts = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
        else:
            quote_ts = datetime.now(timezone.utc)

        return ProviderQuote(
            symbol=symbol,
            provider=self.name,
            price=Decimal(str(current_price)),
            timestamp=quote_ts,
            raw_payload=payload,
            http_status=response.status_code,
        )
