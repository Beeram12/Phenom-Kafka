"""A fixed-size pool of transactional producers, one open transaction per producer."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import structlog
from aiokafka import AIOKafkaProducer

from ingestion.kafka.producer_factory import TransactionalProducerFactory
from ingestion.observability.metrics import IngestionMetrics


class ProducerPoolClosedError(RuntimeError):
    """Raised when leasing from a pool that is shutting down."""


@dataclass(slots=True)
class ProducerSlot:
    """One pool position with a stable transactional_id and its (re-creatable) producer."""

    index: int
    transactional_id: str
    producer: AIOKafkaProducer | None = None
    generation: int = 0


class TransactionalProducerPool:
    """Owns N transactional producers and hands out one exclusively per batch.

    A transactional producer can have only one open transaction, so N producers allow N
    concurrent in-flight transactions. Idle slots live in an asyncio.Queue; ``lease()``
    blocks until one is free. transactional_id = f"{prefix}-{instance_id}-{index}" is stable
    across restarts, so a restarted instance fences its own zombie producers.
    """

    def __init__(
        self,
        *,
        producer_factory: TransactionalProducerFactory,
        pool_size: int,
        transactional_id_prefix: str,
        instance_id: str,
        start_timeout_seconds: float,
        stop_timeout_seconds: float,
        metrics: IngestionMetrics,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._producer_factory = producer_factory
        self._start_timeout_seconds = start_timeout_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._metrics = metrics
        self._logger = logger
        self._slots = [
            ProducerSlot(index, f"{transactional_id_prefix}-{instance_id}-{index}")
            for index in range(pool_size)
        ]
        self._idle_slots: asyncio.Queue[ProducerSlot] = asyncio.Queue()
        self._started = False
        self._closed = False

    @property
    def transactional_ids(self) -> list[str]:
        """The stable transactional ids owned by this pool."""
        return [slot.transactional_id for slot in self._slots]

    async def start(self) -> None:
        """Create and start every producer (fails fast if Kafka is unreachable)."""
        try:
            await asyncio.gather(*(self.ensure_started(slot) for slot in self._slots))
        except BaseException:
            for slot in self._slots:
                if slot.producer is not None:
                    await self._stop_quietly(slot.producer, slot)
                    slot.producer = None
            raise
        for slot in self._slots:
            self._idle_slots.put_nowait(slot)
        self._started = True
        self._logger.info("producer_pool_started", transactional_ids=self.transactional_ids)

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[ProducerSlot]:
        """Exclusively borrow an idle slot for the duration of one transaction."""
        if self._closed:
            raise ProducerPoolClosedError("producer pool is closed")
        slot = await self._idle_slots.get()
        try:
            yield slot
        finally:
            self._idle_slots.put_nowait(slot)

    async def ensure_started(self, slot: ProducerSlot) -> AIOKafkaProducer:
        """Return the slot's producer, (re)creating it and re-initialising transactions."""
        if slot.producer is not None:
            return slot.producer
        producer = self._producer_factory.create_transactional(slot.transactional_id)
        try:
            async with asyncio.timeout(self._start_timeout_seconds):
                await producer.start()
        except BaseException:
            await self._stop_quietly(producer, slot)
            raise
        slot.producer = producer
        slot.generation += 1
        self._logger.info(
            "transactional_producer_started",
            transactional_id=slot.transactional_id,
            generation=slot.generation,
        )
        return producer

    async def discard(self, slot: ProducerSlot, reason: str) -> None:
        """Close the slot's producer; the next ``ensure_started`` recreates it.

        Recreating with the same transactional_id bumps the producer epoch, which fences
        the old instance and aborts whatever transaction it left open.
        """
        producer, slot.producer = slot.producer, None
        if producer is None:
            return
        self._metrics.producer_recreations_total.inc()
        self._logger.warning(
            "transactional_producer_discarded",
            transactional_id=slot.transactional_id,
            generation=slot.generation,
            reason=reason,
        )
        await self._stop_quietly(producer, slot)

    async def close(self) -> None:
        """Wait for all leases to be returned, then stop every producer."""
        self._closed = True
        if not self._started:
            return
        returned_slots = [await self._idle_slots.get() for _ in self._slots]
        await asyncio.gather(
            *(self._stop_quietly(slot.producer, slot) for slot in returned_slots if slot.producer)
        )
        for slot in returned_slots:
            slot.producer = None
        self._logger.info("producer_pool_closed")

    async def _stop_quietly(self, producer: AIOKafkaProducer, slot: ProducerSlot) -> None:
        try:
            async with asyncio.timeout(self._stop_timeout_seconds):
                await producer.stop()
        except Exception as error:
            self._logger.warning(
                "transactional_producer_stop_failed",
                transactional_id=slot.transactional_id,
                error_class=type(error).__name__,
                error=str(error),
            )
