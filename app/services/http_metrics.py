"""HTTP middleware that records Prometheus latency metrics for API requests."""

import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.services.metrics import HTTP_REQUEST_DURATION_SECONDS


class HTTPMetricsMiddleware(BaseHTTPMiddleware):
    """Captures request duration metrics labeled by route, method, and status."""

    async def dispatch(self, request: Request, call_next):
        """Record request latency around the downstream ASGI handler.

        Args:
            request: Incoming Starlette request.
            call_next: Callable that forwards the request to the next handler.

        Returns:
            Response: Downstream response after metrics have been recorded.
        """
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        HTTP_REQUEST_DURATION_SECONDS.labels(
            route=route_path,
            method=request.method,
            status=str(response.status_code),
        ).observe(duration)

        return response
