from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class ProviderQuote:
    symbol: str
    provider: str
    price: Decimal
    timestamp: datetime
    raw_payload: dict
    http_status: int


class ProviderClient(ABC):
    name: str

    @abstractmethod
    def get_latest_price(self, symbol: str) -> ProviderQuote:
        raise NotImplementedError
