import random
import time

import requests

from app.services.metrics import PROVIDER_RATE_LIMIT_HITS_TOTAL
from app.services.providers.exceptions import ProviderError, ProviderRateLimitError


def _retry_after_seconds(response: requests.Response) -> float | None:
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
