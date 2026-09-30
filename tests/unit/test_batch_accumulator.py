"""EventBatchAccumulator: record / byte / time flush triggers, backpressure and drain."""

from __future__ import annotations

import asyncio
import time

import pytest

from ingestion.batching.batch_accumulator import (
    AccumulatorClosedError,
    EventBatchAccumulator,
    QueueFullError,
)
from ingestion.domain.models import EventStatus
from ingestion.observability.logging import get_logger
from ingestion.observability.metrics import IngestionMetrics
from tests.support import RecordingBatchPublisher, make_pending_event


def make_accumulator(
    publisher: RecordingBatchPublisher,
    *,
    max_queue_size: int = 1_000,
    batch_max_records: int = 10,
    batch_max_bytes: int = 1_000_000,
    batch_flush_interval_ms: int = 50,
) -> EventBatchAccumulator:
    return EventBatchAccumulator(
        batch_publisher=publisher,
        max_queue_size=max_queue_size,
        batch_max_records=batch_max_records,
        batch_max_bytes=batch_max_bytes,
        batch_flush_interval_ms=batch_flush_interval_ms,
        max_in_flight_batches=2,
        metrics=IngestionMetrics(),
        logger=get_logger("test"),
    )


async def test_flushes_on_record_count() -> None:
    publisher = RecordingBatchPublisher()
    accumulator = make_accumulator(publisher, batch_max_records=10, batch_flush_interval_ms=10_000)
    accumulator.start()
    pending_events = [make_pending_event() for _ in range(25)]
    accumulator.submit_all(pending_events)
    await asyncio.wait_for(asyncio.gather(*(e.ack_future for e in pending_events[:20])), 1.0)
    assert [len(batch) for batch in publisher.batches] == [10, 10]
    await accumulator.stop()
    assert [len(batch) for batch in publisher.batches] == [10, 10, 5]


async def test_flushes_on_byte_size() -> None:
    publisher = RecordingBatchPublisher()
    event_size = make_pending_event(value=b"x" * 400).size_bytes
    accumulator = make_accumulator(
        publisher,
        batch_max_records=1_000,
        batch_max_bytes=event_size * 3,
        batch_flush_interval_ms=10_000,
    )
    accumulator.start()
    pending_events = [make_pending_event(value=b"x" * 400) for _ in range(7)]
    accumulator.submit_all(pending_events)
    await asyncio.wait_for(asyncio.gather(*(e.ack_future for e in pending_events[:6])), 1.0)
    assert [len(batch) for batch in publisher.batches] == [3, 3]
    for batch in publisher.batches:
        assert sum(pending.size_bytes for pending in batch) <= event_size * 3
    await accumulator.stop()


async def test_single_oversized_event_is_flushed_alone() -> None:
    publisher = RecordingBatchPublisher()
    accumulator = make_accumulator(publisher, batch_max_bytes=1_024, batch_flush_interval_ms=10_000)
    accumulator.start()
    accumulator.submit_all([make_pending_event(value=b"x" * 5_000), make_pending_event()])
    await accumulator.stop()
    assert [len(batch) for batch in publisher.batches] == [1, 1]


async def test_flushes_on_time_interval() -> None:
    publisher = RecordingBatchPublisher()
    accumulator = make_accumulator(publisher, batch_max_records=1_000, batch_flush_interval_ms=80)
    accumulator.start()
    pending_events = [make_pending_event() for _ in range(3)]
    started_at = time.monotonic()
    accumulator.submit_all(pending_events)
    await asyncio.wait_for(asyncio.gather(*(e.ack_future for e in pending_events)), 1.0)
    elapsed = time.monotonic() - started_at
    assert [len(batch) for batch in publisher.batches] == [3]
    assert 0.06 <= elapsed < 0.5
    await accumulator.stop()


async def test_backpressure_rejects_whole_submission_when_full() -> None:
    publisher = RecordingBatchPublisher()
    accumulator = make_accumulator(publisher, max_queue_size=5)
    # Not started: nothing drains the queue.
    accumulator._accepting = True
    accumulator.submit_all([make_pending_event() for _ in range(4)])
    with pytest.raises(QueueFullError):
        accumulator.submit_all([make_pending_event() for _ in range(2)])
    assert accumulator.queue_depth == 4  # all-or-nothing: none of the 2 were enqueued


async def test_stop_drains_queue_and_rejects_new_events() -> None:
    publisher = RecordingBatchPublisher()
    accumulator = make_accumulator(publisher, batch_max_records=4, batch_flush_interval_ms=10_000)
    accumulator.start()
    pending_events = [make_pending_event() for _ in range(9)]
    accumulator.submit_all(pending_events)
    await accumulator.stop()
    assert all(e.ack_future.result().status is EventStatus.ACCEPTED for e in pending_events)
    assert sum(len(batch) for batch in publisher.batches) == 9
    with pytest.raises(AccumulatorClosedError):
        accumulator.submit_all([make_pending_event()])


async def test_crashing_publisher_resolves_events_as_failed() -> None:
    class CrashingPublisher(RecordingBatchPublisher):
        async def publish_batch(self, batch):  # type: ignore[no-untyped-def]
            raise RuntimeError("bug")

    accumulator = make_accumulator(CrashingPublisher(), batch_flush_interval_ms=5)
    accumulator.start()
    pending_event = make_pending_event()
    accumulator.submit_all([pending_event])
    outcome = await asyncio.wait_for(pending_event.ack_future, 1.0)
    assert outcome.status is EventStatus.FAILED
    await accumulator.stop()
