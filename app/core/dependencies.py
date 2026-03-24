"""FastAPI dependency helpers for resolving shared application services."""

from fastapi import Request

from app.services.cache import PriceCache
from app.services.ingestion import PriceIngestionService
from app.services.polling import PollingJobManager
from app.services.providers.factory import ProviderFactory


def get_ingestion_service(request: Request) -> PriceIngestionService:
    """Return the ingestion service stored on application state.

    Args:
        request: Incoming FastAPI request.

    Returns:
        PriceIngestionService: Shared ingestion service instance.
    """
    return request.app.state.ingestion_service


def get_polling_manager(request: Request) -> PollingJobManager:
    """Return the polling manager stored on application state.

    Args:
        request: Incoming FastAPI request.

    Returns:
        PollingJobManager: Shared polling job manager instance.
    """
    return request.app.state.polling_manager


def get_provider_factory(request: Request) -> ProviderFactory:
    """Return the provider factory stored on application state.

    Args:
        request: Incoming FastAPI request.

    Returns:
        ProviderFactory: Shared provider factory instance.
    """
    return request.app.state.provider_factory


def get_price_cache(request: Request) -> PriceCache:
    """Return the cache adapter stored on application state.

    Args:
        request: Incoming FastAPI request.

    Returns:
        PriceCache: Shared cache instance.
    """
    return request.app.state.price_cache
