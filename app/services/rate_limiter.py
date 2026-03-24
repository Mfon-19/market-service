"""In-process rate limiting primitives for external provider calls."""

import threading
import time
from collections import deque


class MinuteRateLimiter:
    """Enforces a simple sliding-window limit on calls per minute."""

    def __init__(self, max_calls_per_minute: int) -> None:
        """Create a rate limiter for a provider client.

        Args:
            max_calls_per_minute: Maximum allowed calls in any 60-second window.
        """
        self.max_calls = max(1, max_calls_per_minute)
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a call slot is available and return time spent waiting.

        Returns:
            float: Total number of seconds spent waiting for capacity.
        """
        waited_seconds = 0.0

        while True:
            now = time.monotonic()
            with self._lock:
                while self._timestamps and now - self._timestamps[0] >= 60:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return waited_seconds

                wait_for = max(0.01, 60 - (now - self._timestamps[0]))

            time.sleep(wait_for)
            waited_seconds += wait_for
