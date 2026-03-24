"""Provider-specific exceptions raised by market data client integrations."""

class ProviderError(RuntimeError):
    """Base exception for provider integration errors."""


class ProviderRateLimitError(ProviderError):
    """Raised when a provider request is rejected due to rate limiting."""

    def __init__(
        self,
        provider: str,
        symbol: str,
        retry_after_seconds: float | None = None,
        message: str | None = None,
    ) -> None:
        """Build a provider rate-limit error with retry metadata.

        Args:
            provider: Provider that rejected the request.
            symbol: Symbol that was being requested.
            retry_after_seconds: Suggested delay before retrying, if known.
            message: Optional provider-specific error message.
        """
        self.provider = provider
        self.symbol = symbol
        self.retry_after_seconds = retry_after_seconds

        resolved_message = message or f"{provider} rate limit hit for symbol {symbol}"
        if retry_after_seconds is not None:
            resolved_message = f"{resolved_message}; retry after {retry_after_seconds:.1f}s"

        super().__init__(resolved_message)
