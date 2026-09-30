"""Bounded micro-batching of pending events with size, byte and time flush triggers."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Final

import structlog

from ingestion.domain.models import EventOutcome, EventStatus, PendingEvent
from ingestion.domain.protocols import BatchPublisher
from ingestion.observability.metrics import IngestionMetrics


class QueueFullError(Exception):
    """The accumulator cannot take the submitted events right now (backpressure)."""


class AccumulatorClosedError(Exception):
    """The accumulator is shutting down and accepts no new events."""


class _StopSignal:
    """Queue sentinel that tells the flush loop to drain and exit."""


_STOP: Final = _StopSignal()


class EventBatchAccumulator:
    """Collects PendingEvents into batches and hands each batch to a BatchPublisher.

    A batch is flushed when ANY of these is reached:
      * ``batch_max_records`` events,
      * ``batch_max_bytes`` of record bytes (an event that would overflow starts the next
        batch; a single oversized event is flushed alone),
      * ``batch_flush_interval_ms`` since the batch's first event arrived.

    The queue is bounded: ``submit_all`` raises QueueFullError instead of buffering without
    limit, and at most ``max_in_flight_batches`` batches are published concurrently, so a slow
    Kafka pushes back all the way to clients.
    """

    def __init__(
        self,
        *,
        batch_publisher: BatchPublisher,
        max_queue_size: int,
        batch_max_records: int,
        batch_max_bytes: int,
        batch_flush_interval_ms: int,
        max_in_flight_batches: int,
        metrics: IngestionMetrics,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._batch_publisher = batch_publisher
        self._max_queue_size = max_queue_size
        self._batch_max_records = batch_max_records
        self._batch_max_bytes = batch_max_bytes
        self._batch_flush_interval_seconds = batch_flush_interval_ms / 1000
        self._metrics = metrics
        self._logger = logger
        # One extra slot so the stop sentinel always fits.
        self._pending_events: asyncio.Queue[PendingEvent | _StopSignal] = asyncio.Queue(
            maxsize=max_queue_size + 1
        )
        self._in_flight_slots = asyncio.Semaphore(max_in_flight_batches)
        self._in_flight_tasks: set[asyncio.Task[None]] = set()
        self._carry_over: PendingEvent | None = None
        self._stop_seen = False
        self._accepting = False
        self._flush_task: asyncio.Task[None] | None = None

    @property
    def queue_depth(self) -> int:
        """Events waiting to be batched."""
        return self._pending_events.qsize()

    @property
    def is_accepting(self) -> bool:
        """True while new events may be submitted."""
        return self._accepting

    def start(self) -> None:
        """Start the background flush loop."""
        if self._flush_task is not None:
            raise RuntimeError("accumulator already started")
        self._accepting = True
        self._flush_task = asyncio.create_task(self._flush_loop(), name="batch-flush-loop")

    def submit_all(self, pending_events: Sequence[PendingEvent]) -> None:
        """Enqueue all events atomically, or none of them.

        Raises AccumulatorClosedError during shutdown and QueueFullError when the queue lacks
        room for every event. No await happens between the check and the puts, so the
        all-or-nothing guarantee holds within the event loop.
        """
        if not self._accepting:
            raise AccumulatorClosedError("service is shutting down")
        free_capacity = self._max_queue_size - self._pending_events.qsize()
        if len(pending_events) > free_capacity:
            self._metrics.queue_rejections_total.inc()
            raise QueueFullError(
                f"ingestion queue full ({self._pending_events.qsize()}/{self._max_queue_size})"
            )
        for pending_event in pending_events:
            self._pending_events.put_nowait(pending_event)
        self._metrics.queue_depth.set(self._pending_events.qsize())

    async def stop(self) -> None:
        """Stop accepting, flush everything already queued and wait for in-flight batches."""
        if self._flush_task is None:
            return
        self._accepting = False
        self._pending_events.put_nowait(_STOP)
        await self._flush_task
        if self._in_flight_tasks:
            await asyncio.gather(*self._in_flight_tasks, return_exceptions=True)
        self._logger.info("batch_accumulator_stopped")

    async def _flush_loop(self) -> None:
        while True:
            batch = await self._collect_batch()
            if not batch:
                return
            await self._in_flight_slots.acquire()
            publish_task = asyncio.create_task(self._publish(batch))
            self._in_flight_tasks.add(publish_task)
            publish_task.add_done_callback(self._in_flight_tasks.discard)

    async def _collect_batch(self) -> list[PendingEvent]:
        first_event = self._carry_over
        self._carry_over = None
        if first_event is None:
            if self._stop_seen:
                return []
            first_item = await self._pending_events.get()
            if isinstance(first_item, _StopSignal):
                self._stop_seen = True
                return []
            first_event = first_item

        batch = [first_event]
        batch_bytes = first_event.size_bytes
        loop = asyncio.get_running_loop()
        flush_deadline = loop.time() + self._batch_flush_interval_seconds

        while len(batch) < self._batch_max_records and not self._stop_seen:
            next_item = self._take_nowait()
            if next_item is None:
                remaining_seconds = flush_deadline - loop.time()
                if remaining_seconds <= 0:
                    break
                try:
                    async with asyncio.timeout(remaining_seconds):
                        next_item = await self._pending_events.get()
                except TimeoutError:
                    break
            if isinstance(next_item, _StopSignal):
                self._stop_seen = True
                break
            if batch_bytes + next_item.size_bytes > self._batch_max_bytes:
                self._carry_over = next_item
                break
            batch.append(next_item)
            batch_bytes += next_item.size_bytes

        self._metrics.queue_depth.set(self._pending_events.qsize())
        return batch

    def _take_nowait(self) -> PendingEvent | _StopSignal | None:
        try:
            return self._pending_events.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _publish(self, batch: list[PendingEvent]) -> None:
        try:
            await self._batch_publisher.publish_batch(batch)
        except Exception as error:
            self._logger.exception("batch_publish_crashed", batch_size=len(batch))
            for pending_event in batch:
                pending_event.resolve(
                    EventOutcome(EventStatus.FAILED, f"internal error: {type(error).__name__}")
                )
        finally:
            self._in_flight_slots.release()
