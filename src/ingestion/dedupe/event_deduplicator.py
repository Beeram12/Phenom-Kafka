"""Two-phase Redis deduplication of client event ids."""

from __future__ import annotations

from collections.abc import Sequence

from redis.asyncio import Redis

from ingestion.domain.models import DedupeKey
from ingestion.domain.protocols import ReservationStatus

PENDING_MARKER = "pending"
COMMITTED_MARKER = "committed"

# Delete each key only if it still holds the pending marker, so a failed batch can never
# erase a committed marker written by another request.
_RELEASE_IF_PENDING_SCRIPT = """
local released = 0
for _, key in ipairs(KEYS) do
    if redis.call('GET', key) == ARGV[1] then
        redis.call('DEL', key)
        released = released + 1
    end
end
return released
"""


class RedisEventDeduplicator:
    """Deduplicates on ``dedupe:{tenant_id}:{event_id}`` using SET NX.

    Phase 1 ``reserve``: SET key "pending" NX EX pending_ttl. Only the winner publishes.
    Phase 2a ``mark_committed``: after the Kafka commit, SET key "committed" EX 7 days.
    Phase 2b ``release``: on failure, delete the pending marker so the client can retry.
    A crash between phases leaves a pending marker that simply expires.
    """

    def __init__(
        self,
        redis_client: Redis,
        *,
        committed_ttl_seconds: int,
        pending_ttl_seconds: int,
        key_prefix: str = "dedupe",
    ) -> None:
        self._redis = redis_client
        self._committed_ttl_seconds = committed_ttl_seconds
        self._pending_ttl_seconds = pending_ttl_seconds
        self._key_prefix = key_prefix
        self._release_script = redis_client.register_script(_RELEASE_IF_PENDING_SCRIPT)

    def redis_key(self, key: DedupeKey) -> str:
        """The Redis key for one event."""
        return f"{self._key_prefix}:{key.tenant_id}:{key.event_id}"

    async def reserve(self, keys: Sequence[DedupeKey]) -> list[ReservationStatus]:
        """Try to reserve every key; returns RESERVED / DUPLICATE / IN_FLIGHT per key."""
        if not keys:
            return []
        redis_keys = [self.redis_key(key) for key in keys]
        async with self._redis.pipeline(transaction=False) as pipeline:
            for redis_key in redis_keys:
                pipeline.set(redis_key, PENDING_MARKER, nx=True, ex=self._pending_ttl_seconds)
            set_results: list[bool | None] = await pipeline.execute()

        contended_keys = [
            redis_key
            for redis_key, was_set in zip(redis_keys, set_results, strict=True)
            if not was_set
        ]
        existing_markers: dict[str, str | None] = {}
        if contended_keys:
            marker_values = await self._redis.mget(contended_keys)
            existing_markers = {
                redis_key: _decode(marker)
                for redis_key, marker in zip(contended_keys, marker_values, strict=True)
            }

        statuses: list[ReservationStatus] = []
        for redis_key, was_set in zip(redis_keys, set_results, strict=True):
            if was_set:
                statuses.append(ReservationStatus.RESERVED)
            elif existing_markers.get(redis_key) == COMMITTED_MARKER:
                statuses.append(ReservationStatus.DUPLICATE)
            else:
                statuses.append(ReservationStatus.IN_FLIGHT)
        return statuses

    async def mark_committed(self, keys: Sequence[DedupeKey]) -> None:
        """Promote markers to 'committed' with the long TTL."""
        if not keys:
            return
        async with self._redis.pipeline(transaction=False) as pipeline:
            for key in keys:
                pipeline.set(self.redis_key(key), COMMITTED_MARKER, ex=self._committed_ttl_seconds)
            await pipeline.execute()

    async def release(self, keys: Sequence[DedupeKey]) -> None:
        """Delete pending markers (never committed ones)."""
        if not keys:
            return
        await self._release_script(
            keys=[self.redis_key(key) for key in keys], args=[PENDING_MARKER]
        )


def _decode(marker: bytes | str | None) -> str | None:
    if isinstance(marker, bytes):
        return marker.decode()
    return marker
