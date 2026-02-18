from fastapi import Request

from app.services.cache import PriceCache
from app.services.ingestion import PriceIngestionService
from app.services.polling import PollingJobManager
from app.services.providers.factory import ProviderFactory


def get_ingestion_service(request: Request) -> PriceIngestionService:
    return request.app.state.ingestion_service


def get_polling_manager(request: Request) -> PollingJobManager:
    return request.app.state.polling_manager


def get_provider_factory(request: Request) -> ProviderFactory:
    return request.app.state.provider_factory


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache
