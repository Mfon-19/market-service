import uuid
from math import ceil

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
    symbol_normalized = symbol.strip().upper()
    provider_normalized = provider.strip().lower()

    cached = ingestion.read_cached(symbol=symbol_normalized, provider=provider_normalized)
    if cached is not None:
        return cached

    try:
        return ingestion.fetch_store_publish(symbol=symbol_normalized, provider=provider_normalized)
    except ProviderRateLimitError as exc:
        persisted = ingestion.read_latest_persisted(symbol=symbol_normalized, provider=provider_normalized)
        if persisted is not None:
            return persisted

        headers = None
        if exc.retry_after_seconds is not None:
            headers = {"Retry-After": str(max(1, ceil(exc.retry_after_seconds)))}
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
            headers=headers,
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="provider request failed") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


@router.post("/poll", response_model=PollJobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def create_polling_job(
    payload: PollJobCreateRequest,
    db: Session = Depends(get_db),
    manager: PollingJobManager = Depends(get_polling_manager),
    provider_factory: ProviderFactory = Depends(get_provider_factory),
) -> PollJobAcceptedResponse:
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
