"""Factory for constructing and retrieving configured provider clients."""

from app.core.config import Settings
from app.services.providers.alpha_vantage import AlphaVantageProvider
from app.services.providers.base import ProviderClient
from app.services.providers.finnhub import FinnhubProvider
from app.services.providers.yahoo import YahooFinanceProvider
from app.services.rate_limiter import MinuteRateLimiter


class ProviderFactory:
    """Owns the configured provider client instances for the process."""

    def __init__(self, settings: Settings) -> None:
        """Create provider clients using the shared runtime configuration.

        Args:
            settings: Runtime settings with provider credentials and limits.
        """
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
        """Return a provider client by name, falling back to the default provider.

        Args:
            provider: Requested provider identifier or `None`.

        Returns:
            ProviderClient: Configured provider client instance.

        Raises:
            ValueError: If the requested provider is not supported.
        """
        provider_name = (provider or self._settings.default_provider).strip().lower()
        client = self._clients.get(provider_name)
        if client is None:
            supported = ", ".join(sorted(self._clients.keys()))
            raise ValueError(f"unsupported provider '{provider_name}'. Supported: {supported}")
        return client

    def supported_providers(self) -> list[str]:
        """Return the sorted list of supported provider identifiers.

        Returns:
            list[str]: Supported provider names.
        """
        return sorted(self._clients.keys())
