from app.core.config import Settings
from app.services.providers.alpha_vantage import AlphaVantageProvider
from app.services.providers.base import ProviderClient
from app.services.providers.finnhub import FinnhubProvider
from app.services.providers.yahoo import YahooFinanceProvider
from app.services.rate_limiter import MinuteRateLimiter


class ProviderFactory:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._clients: dict[str, ProviderClient] = {
            "yahoo": YahooFinanceProvider(
                timeout_seconds=settings.provider_timeout_seconds,
                rate_limiter=MinuteRateLimiter(settings.provider_rate_limit_per_minute),
                http_max_retries=settings.provider_http_max_retries,
                http_backoff_seconds=settings.provider_http_backoff_seconds,
            ),
            "alpha_vantage": AlphaVantageProvider(
                api_key=settings.alpha_vantage_api_key,
                timeout_seconds=settings.provider_timeout_seconds,
                rate_limiter=MinuteRateLimiter(settings.provider_rate_limit_per_minute),
                http_max_retries=settings.provider_http_max_retries,
                http_backoff_seconds=settings.provider_http_backoff_seconds,
            ),
            "finnhub": FinnhubProvider(
                api_key=settings.finnhub_api_key,
                timeout_seconds=settings.provider_timeout_seconds,
                rate_limiter=MinuteRateLimiter(settings.provider_rate_limit_per_minute),
                http_max_retries=settings.provider_http_max_retries,
                http_backoff_seconds=settings.provider_http_backoff_seconds,
            ),
        }

    def get_client(self, provider: str | None) -> ProviderClient:
        provider_name = (provider or self._settings.default_provider).strip().lower()
        client = self._clients.get(provider_name)
        if client is None:
            supported = ", ".join(sorted(self._clients.keys()))
            raise ValueError(f"unsupported provider '{provider_name}'. Supported: {supported}")
        return client

    def supported_providers(self) -> list[str]:
        return sorted(self._clients.keys())
