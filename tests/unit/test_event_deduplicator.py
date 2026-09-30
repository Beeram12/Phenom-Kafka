"""RedisEventDeduplicator two-phase semantics against fakeredis."""

from __future__ import annotations

from collections.abc import AsyncIterator

import fakeredis
import pytest

from ingestion.dedupe.event_deduplicator import RedisEventDeduplicator
from ingestion.domain.models import DedupeKey
from ingestion.domain.protocols import ReservationStatus

COMMITTED_TTL = 7 * 24 * 3600
PENDING_TTL = 300


@pytest.fixture
async def redis_client() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    client = fakeredis.FakeAsyncRedis()
    yield client
    await client.aclose()


@pytest.fixture
def deduplicator(redis_client: fakeredis.FakeAsyncRedis) -> RedisEventDeduplicator:
    return RedisEventDeduplicator(
        redis_client, committed_ttl_seconds=COMMITTED_TTL, pending_ttl_seconds=PENDING_TTL
    )


KEY_A = DedupeKey("tenant_1", "event-a")
KEY_B = DedupeKey("tenant_1", "event-b")


async def test_first_reservation_wins_with_pending_ttl(
    deduplicator: RedisEventDeduplicator, redis_client: fakeredis.FakeAsyncRedis
) -> None:
    assert await deduplicator.reserve([KEY_A]) == [ReservationStatus.RESERVED]
    assert await redis_client.get("dedupe:tenant_1:event-a") == b"pending"
    assert 0 < await redis_client.ttl("dedupe:tenant_1:event-a") <= PENDING_TTL


async def test_concurrent_reservation_is_in_flight(deduplicator: RedisEventDeduplicator) -> None:
    await deduplicator.reserve([KEY_A])
    assert await deduplicator.reserve([KEY_A]) == [ReservationStatus.IN_FLIGHT]


async def test_committed_key_is_duplicate_with_seven_day_ttl(
    deduplicator: RedisEventDeduplicator, redis_client: fakeredis.FakeAsyncRedis
) -> None:
    await deduplicator.reserve([KEY_A])
    await deduplicator.mark_committed([KEY_A])
    assert await deduplicator.reserve([KEY_A]) == [ReservationStatus.DUPLICATE]
    ttl = await redis_client.ttl("dedupe:tenant_1:event-a")
    assert PENDING_TTL < ttl <= COMMITTED_TTL


async def test_release_allows_client_retry(deduplicator: RedisEventDeduplicator) -> None:
    await deduplicator.reserve([KEY_A])
    await deduplicator.release([KEY_A])
    assert await deduplicator.reserve([KEY_A]) == [ReservationStatus.RESERVED]


async def test_release_never_deletes_committed_marker(
    deduplicator: RedisEventDeduplicator,
) -> None:
    await deduplicator.reserve([KEY_A, KEY_B])
    await deduplicator.mark_committed([KEY_A])
    await deduplicator.release([KEY_A, KEY_B])
    assert await deduplicator.reserve([KEY_A, KEY_B]) == [
        ReservationStatus.DUPLICATE,
        ReservationStatus.RESERVED,
    ]


async def test_keys_are_scoped_per_tenant(deduplicator: RedisEventDeduplicator) -> None:
    await deduplicator.reserve([KEY_A])
    await deduplicator.mark_committed([KEY_A])
    other_tenant = DedupeKey("tenant_2", KEY_A.event_id)
    assert await deduplicator.reserve([other_tenant]) == [ReservationStatus.RESERVED]


async def test_mixed_batch_statuses_preserve_order(deduplicator: RedisEventDeduplicator) -> None:
    key_c = DedupeKey("tenant_1", "event-c")
    await deduplicator.reserve([KEY_A, KEY_B])
    await deduplicator.mark_committed([KEY_B])
    assert await deduplicator.reserve([KEY_A, KEY_B, key_c]) == [
        ReservationStatus.IN_FLIGHT,
        ReservationStatus.DUPLICATE,
        ReservationStatus.RESERVED,
    ]


async def test_empty_inputs_are_noops(deduplicator: RedisEventDeduplicator) -> None:
    assert await deduplicator.reserve([]) == []
    await deduplicator.mark_committed([])
    await deduplicator.release([])
