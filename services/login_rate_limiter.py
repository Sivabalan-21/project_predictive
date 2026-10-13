"""Small process-local login throttle. Deployments with several API replicas also need a shared proxy/WAF limit."""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Callable


class LoginRateLimiter:
    def __init__(self, limit: int, window_seconds: int, clock: Callable[[], float] = time.monotonic):
        self.limit, self.window_seconds, self._clock = limit, window_seconds, clock
        self._attempts: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, client_key: str) -> bool:
        now = self._clock()
        with self._lock:
            attempts = self._attempts[client_key]
            while attempts and attempts[0] <= now - self.window_seconds:
                attempts.popleft()
            if len(attempts) >= self.limit:
                return False
            attempts.append(now)
            return True
