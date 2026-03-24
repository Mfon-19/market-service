"""Health endpoint definitions for simple service liveness checks."""

from fastapi import APIRouter

from app.core.config import settings

router = APIRouter(tags=["health"])


@router.get("/health")
def healthcheck() -> dict:
    """Return a minimal health payload for monitoring checks.

    Returns:
        dict: Service status metadata for liveness probes.
    """
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.environment,
    }
