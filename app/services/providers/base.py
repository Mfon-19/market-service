"""Provider abstractions shared by all market data client implementations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class ProviderQuote:
    """Normalized quote returned by one upstream market data provider."""

    symbol: str
    provider: str
    price: Decimal
    timestamp: datetime
    raw_payload: dict
    http_status: int


class ProviderClient(ABC):
    """Abstract interface implemented by each upstream market data provider."""

    name: str

    @abstractmethod
    def get_latest_price(self, symbol: str) -> ProviderQuote:
        """Fetch the latest quote for a market symbol.

        Args:
            symbol: Market symbol to fetch.

        Returns:
            ProviderQuote: Normalized provider quote payload.
        """
        raise NotImplementedError
