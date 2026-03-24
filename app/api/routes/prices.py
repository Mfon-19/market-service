"""Price API routes for latest reads and persistent polling jobs."""

import uuid
from math import ceil
import time

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.dependencies import get_ingestion_service, get_polling_manager, get_provider_factory
from app.db.session import get_db
from app.models.polling_job import PollingJob
from app.schemas.price import (
    PollJobAcceptedResponse,
    PollJobCreateRequest,
    PollJobStatusResponse,
    PriceLatestResponse,
)
from app.services.ingestion import PriceIngestionService
from app.services.metrics import LATEST_PRICE_REQUEST_DURATION_SECONDS
from app.services.polling import PollingJobManager
from app.services.providers.exceptions import ProviderRateLimitError
from app.services.providers.factory import ProviderFactory

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/latest", response_model=PriceLatestResponse)
def get_latest_price(
    symbol: str = Query(..., min_length=1),
    provider: str = Query(default=settings.default_provider, min_length=1),
    ingestion: PriceIngestionService = Depends(get_ingestion_service),
) -> PriceLatestResponse:
    """Return the latest known price for a symbol, fetching on cache miss.

    Args:
        symbol: Requested market symbol.
        provider: Requested provider name.
        ingestion: Service that reads, fetches, stores, and publishes prices.

    Returns:
        PriceLatestResponse: Latest price payload, optionally marked as cached.

    Raises:
        HTTPException: If validation fails or upstream providers cannot satisfy the request.
    """
    symbol_normalized = symbol.strip().upper()
    provider_normalized = provider.strip().lower()
    started = time.perf_counter()
    cached_label = "false"
    status_label = "200"

    try:
        cached = ingestion.read_cached(symbol=symbol_normalized, provider=provider_normalized)
        if cached is not None:
            cached_label = "true"
            return cached

        latest = ingestion.read_latest_from_store(symbol=symbol_normalized, provider=provider_normalized)
        if latest is not None:
            ingestion.warm_cache(provider_normalized, latest)
            return latest

        return ingestion.fetch_store_publish(symbol=symbol_normalized, provider=provider_normalized)
    except ProviderRateLimitError as exc:
        persisted = ingestion.read_latest_persisted(symbol=symbol_normalized, provider=provider_normalized)
        if persisted is not None:
            ingestion.warm_cache(provider_normalized, persisted)
            return persisted

        headers = None
        if exc.retry_after_seconds is not None:
            headers = {"Retry-After": str(max(1, ceil(exc.retry_after_seconds)))}
        status_label = str(status.HTTP_429_TOO_MANY_REQUESTS)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
            headers=headers,
        ) from exc
    except ValueError as exc:
        status_label = str(status.HTTP_400_BAD_REQUEST)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except requests.RequestException as exc:
        status_label = str(status.HTTP_502_BAD_GATEWAY)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="provider request failed") from exc
    except RuntimeError as exc:
        status_label = str(status.HTTP_502_BAD_GATEWAY)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    finally:
        duration = time.perf_counter() - started
        LATEST_PRICE_REQUEST_DURATION_SECONDS.labels(cached=cached_label, status=status_label).observe(duration)


@router.post("/poll", response_model=PollJobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def create_polling_job(
    payload: PollJobCreateRequest,
    db: Session = Depends(get_db),
    manager: PollingJobManager = Depends(get_polling_manager),
    provider_factory: ProviderFactory = Depends(get_provider_factory),
) -> PollJobAcceptedResponse:
    """Create and schedule a persistent polling job for one provider.

    Args:
        payload: Requested polling job configuration.
        db: Database session used to persist the job.
        manager: Scheduler-backed polling manager.
        provider_factory: Factory used to validate provider availability.

    Returns:
        PollJobAcceptedResponse: Accepted job identifier.

    Raises:
        HTTPException: If the request violates validation or rate-limit constraints.
    """
    if payload.interval_seconds < settings.poll_min_interval_seconds:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"interval must be >= {settings.poll_min_interval_seconds} seconds",
        )

    if len(payload.symbols) > settings.poll_max_symbols_per_job:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"maximum symbols per job is {settings.poll_max_symbols_per_job}",
        )

    rate_limit = max(1, settings.provider_rate_limit_per_minute)
    rate_safe_interval = ceil((len(payload.symbols) * 60) / rate_limit)
    required_interval = max(settings.poll_min_interval_seconds, rate_safe_interval)
    if payload.interval_seconds < required_interval:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"interval too low for configured provider rate limit; "
                f"use at least {required_interval} seconds for {len(payload.symbols)} symbol(s)"
            ),
        )

    try:
        provider_factory.get_client(payload.provider)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    try:
        job = manager.create_job(db=db, request=payload)
        return PollJobAcceptedResponse(job_id=job.id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/poll/{job_id}", response_model=PollJobStatusResponse)
def get_polling_job(
    job_id: uuid.UUID,
    db: Session = Depends(get_db),
) -> PollJobStatusResponse:
    """Return the current persisted status for a polling job.

    Args:
        job_id: Polling job identifier.
        db: Database session used to read job state.

    Returns:
        PollJobStatusResponse: Current job metadata and execution timestamps.

    Raises:
        HTTPException: If the polling job does not exist.
    """
    job = db.get(PollingJob, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")

    return PollJobStatusResponse(
        job_id=job.id,
        provider=job.provider,
        symbols=job.symbols,
        interval_seconds=job.interval_seconds,
        is_active=job.is_active,
        created_at=job.created_at,
        last_run_at=job.last_run_at,
    )
