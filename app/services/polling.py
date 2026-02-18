import logging
import uuid
from datetime import datetime, timezone
from math import ceil
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.polling_job import PollingJob
from app.schemas.price import PollJobCreateRequest
from app.services.ingestion import PriceIngestionService
from app.services.providers.exceptions import ProviderRateLimitError

logger = logging.getLogger(__name__)


class PollingJobManager:
    def __init__(self, session_factory: Callable[[], Session], ingestion_service: PriceIngestionService) -> None:
        self._session_factory = session_factory
        self._ingestion_service = ingestion_service
        self._scheduler = BackgroundScheduler(timezone=timezone.utc)
        self._started = False

    def start(self) -> None:
        if self._started:
            return

        self._scheduler.start()
        self._restore_active_jobs()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return

        self._scheduler.shutdown(wait=False)
        self._started = False

    def create_job(self, db: Session, request: PollJobCreateRequest) -> PollingJob:
        job = PollingJob(
            provider=request.provider,
            symbols=request.symbols,
            interval_seconds=request.interval_seconds,
            is_active=True,
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        self.register_job(job)
        return job

    def register_job(self, job: PollingJob) -> None:
        self._scheduler.add_job(
            self._run_job,
            trigger="interval",
            id=str(job.id),
            seconds=job.interval_seconds,
            args=[str(job.id)],
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            next_run_time=datetime.now(timezone.utc),
        )

    def _restore_active_jobs(self) -> None:
        with self._session_factory() as db:
            jobs = db.query(PollingJob).filter(PollingJob.is_active.is_(True)).all()
            for job in jobs:
                self.register_job(job)

    def _run_job(self, job_id: str) -> None:
        try:
            job_uuid = uuid.UUID(job_id)
        except ValueError:
            logger.error("Invalid polling job id %s", job_id)
            return

        with self._session_factory() as db:
            job = db.get(PollingJob, job_uuid)
            if job is None or not job.is_active:
                return

            rate_limit = max(1, settings.provider_rate_limit_per_minute)
            rate_safe_interval = ceil((len(job.symbols) * 60) / rate_limit)
            required_interval = max(settings.poll_min_interval_seconds, rate_safe_interval)
            if job.interval_seconds < required_interval:
                logger.warning(
                    "Polling job %s interval (%ss) is below safe threshold (%ss); skipping run",
                    job.id,
                    job.interval_seconds,
                    required_interval,
                )
                job.last_run_at = datetime.now(timezone.utc)
                db.commit()
                return

            for symbol in job.symbols:
                try:
                    self._ingestion_service.fetch_store_publish(symbol=symbol, provider=job.provider)
                except ProviderRateLimitError as exc:
                    logger.warning(
                        "Polling job %s rate-limited by %s for symbol %s: %s",
                        job.id,
                        job.provider,
                        symbol,
                        exc,
                    )
                    break
                except Exception:
                    logger.exception("Polling job %s failed for symbol %s", job.id, symbol)

            job.last_run_at = datetime.now(timezone.utc)
            db.commit()
