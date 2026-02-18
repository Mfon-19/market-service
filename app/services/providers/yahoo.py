from datetime import datetime, timezone
from decimal import Decimal

from app.services.metrics import PROVIDER_RATE_LIMIT_HITS_TOTAL
from app.services.providers.base import ProviderClient, ProviderQuote
from app.services.providers.http import request_with_backoff
from app.services.rate_limiter import MinuteRateLimiter


class YahooFinanceProvider(ProviderClient):
    name = "yahoo"

    def __init__(
        self,
        timeout_seconds: int,
        rate_limiter: MinuteRateLimiter,
        http_max_retries: int,
        http_backoff_seconds: float,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter
        self.http_max_retries = max(0, http_max_retries)
        self.http_backoff_seconds = max(0.1, http_backoff_seconds)

    def get_latest_price(self, symbol: str) -> ProviderQuote:
        waited = self.rate_limiter.acquire()
        if waited > 0:
            PROVIDER_RATE_LIMIT_HITS_TOTAL.labels(provider=self.name).inc()

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {"interval": "1m", "range": "1d"}
        response = request_with_backoff(
            self.name,
            symbol,
            url,
            params=params,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.http_max_retries,
            base_backoff_seconds=self.http_backoff_seconds,
        )

        payload = response.json()
        result = payload.get("chart", {}).get("result")
        if not result:
            raise RuntimeError(f"yahoo returned no result for symbol {symbol}")

        meta = result[0].get("meta", {})
        price_value = meta.get("regularMarketPrice")
        timestamp_value = meta.get("regularMarketTime")

        if price_value is None:
            quote_points = result[0].get("indicators", {}).get("quote", [])
            closes = quote_points[0].get("close", []) if quote_points else []
            closes = [value for value in closes if value is not None]
            if closes:
                price_value = closes[-1]

        if price_value is None:
            raise RuntimeError(f"could not parse price for symbol {symbol} from yahoo response")

        if timestamp_value is not None:
            quote_ts = datetime.fromtimestamp(int(timestamp_value), tz=timezone.utc)
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
