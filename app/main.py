"""Application entrypoint that wires routes, shared services, and metrics."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.api.routes.health import router as health_router
from app.api.routes.prices import router as prices_router
from app.core.config import settings
from app.db.init_db import init_db
from app.db.session import SessionLocal
from app.services.cache import PriceCache
from app.services.http_metrics import HTTPMetricsMiddleware
from app.services.ingestion import PriceIngestionService
from app.services.kafka_producer import PriceEventProducer
from app.services.polling import PollingJobManager
from app.services.providers.factory import ProviderFactory


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize shared services for the FastAPI application lifecycle.

    Args:
        app: FastAPI application instance receiving initialized shared state.

    Yields:
        None: Control back to FastAPI while the application is running.
    """
    init_db()

    provider_factory = ProviderFactory(settings)
    price_cache = PriceCache(settings)
    producer = PriceEventProducer(settings)
    ingestion_service = PriceIngestionService(
        session_factory=SessionLocal,
        provider_factory=provider_factory,
        producer=producer,
        cache=price_cache,
        settings=settings,
    )
    polling_manager = PollingJobManager(
        session_factory=SessionLocal,
        ingestion_service=ingestion_service,
    )
    polling_manager.start()

    app.state.provider_factory = provider_factory
    app.state.price_cache = price_cache
    app.state.producer = producer
    app.state.ingestion_service = ingestion_service
    app.state.polling_manager = polling_manager

    yield

    polling_manager.stop()
    producer.close()
    price_cache.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(HTTPMetricsMiddleware)

app.include_router(health_router)
app.include_router(prices_router)
app.mount("/metrics", make_asgi_app())
