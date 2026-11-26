"""Token-bucket limiter for embedding APIs that cap requests and tokens per minute."""

from __future__ import annotations

import time
from collections.abc import Callable

from rag_ingest.domain import RateLimitedError


class TokenBucket:
    """Two buckets refilled continuously: one for requests, one for tokens.

    The limiter is per process. When several workers share one API quota, give each a
    proportional share (quota / max concurrency); the Terraform caps Lambda concurrency
    for exactly this reason.
    """

    def __init__(
        self,
        requests_per_minute: float,
        tokens_per_minute: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if requests_per_minute <= 0 or tokens_per_minute <= 0:
            raise ValueError("limits must be positive")
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self._clock = clock
        self._sleep = sleep
        self._requests = float(requests_per_minute)
        self._tokens = float(tokens_per_minute)
        self._updated_at = clock()

    def acquire(self, tokens: int, max_wait_seconds: float) -> None:
        """Take one request and ``tokens`` tokens, sleeping until they are available.

        Raises:
            ValueError: if ``tokens`` exceeds the bucket size, so it could never succeed.
            RateLimitedError: if the wait would exceed ``max_wait_seconds``.
        """
        if tokens > self.tokens_per_minute:
            raise ValueError(
                f"request needs {tokens} tokens but the bucket holds {self.tokens_per_minute}"
            )
        self._refill()
        wait = self._seconds_until_available(tokens)
        if wait > max_wait_seconds:
            raise RateLimitedError(f"embedding budget frees up in {wait:.1f}s")
        if wait > 0:
            self._sleep(wait)
            self._refill()
        self._requests -= 1
        self._tokens -= tokens

    def _seconds_until_available(self, tokens: int) -> float:
        request_deficit = max(0.0, 1 - self._requests)
        token_deficit = max(0.0, tokens - self._tokens)
        return max(
            request_deficit * 60 / self.requests_per_minute,
            token_deficit * 60 / self.tokens_per_minute,
        )

    def _refill(self) -> None:
        now = self._clock()
        elapsed_minutes = (now - self._updated_at) / 60
        self._updated_at = now
        self._requests = min(
            self.requests_per_minute, self._requests + elapsed_minutes * self.requests_per_minute
        )
        self._tokens = min(
            self.tokens_per_minute, self._tokens + elapsed_minutes * self.tokens_per_minute
        )
