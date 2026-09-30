"""ExponentialBackoffPolicy: full-jitter bounds and retry semantics."""

from __future__ import annotations

import random

import pytest

from ingestion.observability.logging import get_logger
from ingestion.resilience.retry_policy import ExponentialBackoffPolicy, RetryError


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay_seconds: float) -> None:
        self.delays.append(delay_seconds)


def make_policy(
    sleep: RecordingSleep | None = None, max_attempts: int = 5
) -> ExponentialBackoffPolicy:
    return ExponentialBackoffPolicy(
        base_delay_ms=100,
        multiplier=2.0,
        max_delay_ms=5_000,
        max_attempts=max_attempts,
        logger=get_logger("test"),
        random_source=random.Random(42),
        sleep=sleep or RecordingSleep(),
    )


def test_delay_ceiling_grows_exponentially_and_is_capped() -> None:
    policy = make_policy()
    ceilings = [policy.delay_ceiling(attempt) for attempt in range(10)]
    assert ceilings[:6] == pytest.approx([0.1, 0.2, 0.4, 0.8, 1.6, 3.2])
    assert all(ceiling == pytest.approx(5.0) for ceiling in ceilings[6:])


@pytest.mark.parametrize("attempt", range(12))
def test_full_jitter_delay_stays_within_bounds(attempt: int) -> None:
    policy = make_policy()
    for _ in range(500):
        delay = policy.delay_for_attempt(attempt)
        assert 0.0 <= delay <= policy.delay_ceiling(attempt)


def test_jitter_actually_varies() -> None:
    policy = make_policy()
    delays = {policy.delay_for_attempt(3) for _ in range(50)}
    assert len(delays) > 40


async def test_retries_until_success() -> None:
    sleep = RecordingSleep()
    policy = make_policy(sleep)
    calls = 0

    async def flaky_operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("transient")
        return "ok"

    result = await policy.run(flaky_operation, should_retry=lambda _: True, operation_name="op")
    assert result == "ok"
    assert calls == 3
    assert len(sleep.delays) == 2
    assert sleep.delays[0] <= 0.1
    assert sleep.delays[1] <= 0.2


async def test_gives_up_after_max_attempts_with_exhausted_flag() -> None:
    sleep = RecordingSleep()
    policy = make_policy(sleep, max_attempts=4)

    async def always_fails() -> None:
        raise ConnectionError("down")

    with pytest.raises(RetryError) as error_info:
        await policy.run(always_fails, should_retry=lambda _: True, operation_name="op")
    assert error_info.value.attempts == 4
    assert error_info.value.exhausted is True
    assert isinstance(error_info.value.last_error, ConnectionError)
    assert len(sleep.delays) == 3  # no sleep after the final attempt


async def test_non_retriable_error_stops_immediately() -> None:
    sleep = RecordingSleep()
    policy = make_policy(sleep)

    async def bad_data() -> None:
        raise ValueError("poison")

    with pytest.raises(RetryError) as error_info:
        await policy.run(
            bad_data,
            should_retry=lambda error: not isinstance(error, ValueError),
            operation_name="op",
        )
    assert error_info.value.attempts == 1
    assert error_info.value.exhausted is False
    assert sleep.delays == []


def test_rejects_zero_attempts() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        make_policy(max_attempts=0)
