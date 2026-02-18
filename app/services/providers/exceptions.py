class ProviderError(RuntimeError):
    """Base exception for provider integration errors."""


class ProviderRateLimitError(ProviderError):
    def __init__(
        self,
        provider: str,
        symbol: str,
        retry_after_seconds: float | None = None,
        message: str | None = None,
    ) -> None:
        self.provider = provider
        self.symbol = symbol
        self.retry_after_seconds = retry_after_seconds

        resolved_message = message or f"{provider} rate limit hit for symbol {symbol}"
        if retry_after_seconds is not None:
            resolved_message = f"{resolved_message}; retry after {retry_after_seconds:.1f}s"

        super().__init__(resolved_message)
