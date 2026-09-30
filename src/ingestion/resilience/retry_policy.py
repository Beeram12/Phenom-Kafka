"""Exponential backoff with full jitter for async operations."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

ResultT = TypeVar("ResultT")

Sleeper = Callable[[float], Awaitable[None]]


class RetryError(Exception):
    """Raised when an operation ultimately fails.

    ``exhausted`` is True when every attempt was used on retriable errors, and False when a
    non-retriable error stopped the loop early. ``last_error`` is the final underlying error.
    """

    def __init__(self, operation_name: str, last_error: Exception, attempts: int, exhausted: bool):
        self.operation_name = operation_name
        self.last_error = last_error
        self.attempts = attempts
        self.exhausted = exhausted
        reason = "retries exhausted" if exhausted else "non-retriable error"
        super().__init__(
            f"{operation_name} failed after {attempts} attempt(s) ({reason}): "
            f"{type(last_error).__name__}: {last_error}"
        )


class ExponentialBackoffPolicy:
    """Runs a coroutine factory up to ``max_attempts`` times with full-jitter backoff.

    delay(attempt) = uniform(0, min(max_delay, base_delay * multiplier ** attempt)),
    where ``attempt`` is the 0-based index of the attempt that just failed.
    """

    def __init__(
        self,
        *,
        base_delay_ms: int,
        multiplier: float,
        max_delay_ms: int,
        max_attempts: int,
        logger: structlog.stdlib.BoundLogger,
        random_source: random.Random | None = None,
        sleep: Sleeper = asyncio.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._base_delay_seconds = base_delay_ms / 1000
        self._multiplier = multiplier
        self._max_delay_seconds = max_delay_ms / 1000
        self.max_attempts = max_attempts
        self._logger = logger
        self._random_source = random_source or random.Random()
        self._sleep = sleep

    def delay_ceiling(self, attempt: int) -> float:
        """Upper bound (seconds) of the jittered delay after the 0-based ``attempt``."""
        exponential = self._base_delay_seconds * self._multiplier**attempt
        return min(self._max_delay_seconds, exponential)

    def delay_for_attempt(self, attempt: int) -> float:
        """Full-jitter delay (seconds) after the 0-based ``attempt`` failed."""
        return self._random_source.uniform(0, self.delay_ceiling(attempt))

    async def run(
        self,
        operation_factory: Callable[[], Awaitable[ResultT]],
        *,
        should_retry: Callable[[Exception], bool],
        operation_name: str,
    ) -> ResultT:
        """Await a fresh ``operation_factory()`` per attempt until success or give-up.

        Raises RetryError on failure. Cancellation is never swallowed.
        """
        for attempt in range(self.max_attempts):
            try:
                return await operation_factory()
            except Exception as error:
                attempt_number = attempt + 1
                retriable = should_retry(error)
                if not retriable or attempt_number >= self.max_attempts:
                    self._logger.warning(
                        "retry_giving_up",
                        operation=operation_name,
                        attempt=attempt_number,
                        max_attempts=self.max_attempts,
                        error_class=type(error).__name__,
                        error=str(error),
                        retriable=retriable,
                    )
                    raise RetryError(
                        operation_name, error, attempt_number, exhausted=retriable
                    ) from error
                delay_seconds = self.delay_for_attempt(attempt)
                self._logger.warning(
                    "retry_scheduled",
                    operation=operation_name,
                    attempt=attempt_number,
                    max_attempts=self.max_attempts,
                    delay_ms=round(delay_seconds * 1000, 1),
                    error_class=type(error).__name__,
                    error=str(error),
                )
                await self._sleep(delay_seconds)
        raise AssertionError("unreachable: retry loop always returns or raises")
