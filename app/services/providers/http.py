"""Shared HTTP retry and backoff helpers used by provider clients."""

import random
import time

import requests

from app.services.metrics import PROVIDER_RATE_LIMIT_HITS_TOTAL
from app.services.providers.exceptions import ProviderError, ProviderRateLimitError


def _retry_after_seconds(response: requests.Response) -> float | None:
    """Parse a numeric `Retry-After` response header when present.

    Args:
        response: HTTP response returned by the upstream provider.

    Returns:
        float | None: Parsed retry delay in seconds, or `None` if unavailable.
    """
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return None

    try:
        parsed = float(retry_after)
        if parsed > 0:
            return parsed
    except ValueError:
        return None

    return None


def request_with_backoff(
    provider_name: str,
    symbol: str,
    url: str,
    *,
    params: dict,
    timeout_seconds: int,
    max_retries: int,
    base_backoff_seconds: float,
) -> requests.Response:
    """Execute an HTTP GET with retry/backoff handling for providers.

    Args:
        provider_name: Provider identifier used in metrics and errors.
        symbol: Market symbol being requested.
        url: Provider endpoint URL.
        params: Query-string parameters for the request.
        timeout_seconds: Request timeout in seconds.
        max_retries: Maximum number of retry attempts for retryable failures.
        base_backoff_seconds: Base delay used for exponential backoff.

    Returns:
        requests.Response: Successful HTTP response.

    Raises:
        ProviderRateLimitError: If rate-limited retries are exhausted.
        ProviderError: If a non-retryable request failure occurs.
    """
    attempt = 0
    while True:
        response = requests.get(url, params=params, timeout=timeout_seconds)

        if response.status_code == 429:
            retry_after = _retry_after_seconds(response)
            backoff = retry_after or (base_backoff_seconds * (2**attempt))
            # Add low jitter to avoid synchronized retries across jobs.
            backoff += random.uniform(0, 0.5)
            PROVIDER_RATE_LIMIT_HITS_TOTAL.labels(provider=provider_name).inc()

            if attempt >= max_retries:
                raise ProviderRateLimitError(
                    provider=provider_name,
                    symbol=symbol,
                    retry_after_seconds=backoff,
                )

            time.sleep(backoff)
            attempt += 1
            continue

        if 500 <= response.status_code <= 599 and attempt < max_retries:
            backoff = base_backoff_seconds * (2**attempt)
            time.sleep(backoff)
            attempt += 1
            continue

        try:
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            raise ProviderError(f"{provider_name} request failed for {symbol}: {exc}") from exc
