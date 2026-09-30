"""TransactionalPublisher against fake transactional producers."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
from aiokafka import errors as kafka_errors

from ingestion.domain.models import EventStatus, FailedStage
from ingestion.kafka.error_classifier import ErrorClassifier
from ingestion.kafka.producer_pool import TransactionalProducerPool
from ingestion.kafka.transactional_publisher import TransactionalPublisher
from ingestion.observability.logging import get_logger
from ingestion.observability.metrics import IngestionMetrics
from ingestion.resilience.retry_policy import ExponentialBackoffPolicy
from tests.support import (
    FakeDeadLetterSink,
    FakeDeduplicator,
    FakeProducerFactory,
    make_pending_event,
)


async def _no_sleep(_: float) -> None:
    return None


@dataclass
class PublisherHarness:
    publisher: TransactionalPublisher
    factory: FakeProducerFactory
    pool: TransactionalProducerPool
    deduplicator: FakeDeduplicator
    dead_letter_sink: FakeDeadLetterSink
    metrics: IngestionMetrics


async def build_harness(
    factory: FakeProducerFactory,
    *,
    max_attempts: int = 3,
    commit_timeout_seconds: float = 2.0,
    dlq_succeeds: bool = True,
) -> PublisherHarness:
    metrics = IngestionMetrics()
    pool = TransactionalProducerPool(
        producer_factory=factory,
        pool_size=1,
        transactional_id_prefix="tx",
        instance_id="test-0",
        start_timeout_seconds=1,
        stop_timeout_seconds=1,
        metrics=metrics,
        logger=get_logger("test"),
    )
    await pool.start()
    deduplicator = FakeDeduplicator()
    dead_letter_sink = FakeDeadLetterSink(succeed=dlq_succeeds)
    publisher = TransactionalPublisher(
        producer_pool=pool,
        retry_policy=ExponentialBackoffPolicy(
            base_delay_ms=1,
            multiplier=2,
            max_delay_ms=4,
            max_attempts=max_attempts,
            logger=get_logger("test"),
            random_source=random.Random(1),
            sleep=_no_sleep,
        ),
        error_classifier=ErrorClassifier(),
        dead_letter_sink=dead_letter_sink,
        deduplicator=deduplicator,
        metrics=metrics,
        logger=get_logger("test"),
        send_timeout_seconds=2.0,
        commit_timeout_seconds=commit_timeout_seconds,
        abort_timeout_seconds=1.0,
    )
    return PublisherHarness(publisher, factory, pool, deduplicator, dead_letter_sink, metrics)


@pytest.fixture
async def harness() -> AsyncIterator[PublisherHarness]:
    built = await build_harness(FakeProducerFactory())
    yield built
    await built.pool.close()


async def test_commit_acks_every_event_and_marks_dedupe(harness: PublisherHarness) -> None:
    batch = [make_pending_event() for _ in range(5)]
    result = await harness.publisher.publish_batch(batch)
    assert result.committed_count == 5
    assert all(event.ack_future.result().status is EventStatus.ACCEPTED for event in batch)
    assert harness.deduplicator.committed_calls == [[event.dedupe_key for event in batch]]
    assert harness.factory.all_committed_values == [event.record.value for event in batch]


async def test_retriable_commit_error_aborts_and_retries_whole_batch() -> None:
    factory = FakeProducerFactory(
        shared_commit_errors=[
            kafka_errors.KafkaConnectionError(),
            kafka_errors.RequestTimedOutError(),
        ]
    )
    harness = await build_harness(factory, max_attempts=5)
    batch = [make_pending_event() for _ in range(3)]
    result = await harness.publisher.publish_batch(batch)
    producer = factory.created[0][1]
    assert result.transaction_attempts == 3
    assert producer.aborted_count == 2
    assert producer.committed_values == [event.record.value for event in batch]  # exactly once
    assert all(event.ack_future.result().status is EventStatus.ACCEPTED for event in batch)
    await harness.pool.close()


async def test_exhausted_retries_send_batch_to_dlq_and_release_markers() -> None:
    factory = FakeProducerFactory(
        shared_commit_errors=[kafka_errors.KafkaConnectionError() for _ in range(3)]
    )
    harness = await build_harness(factory, max_attempts=3)
    batch = [make_pending_event() for _ in range(4)]
    result = await harness.publisher.publish_batch(batch)
    assert result.dead_lettered_count == 4
    assert all(event.ack_future.result().status is EventStatus.SENT_TO_DLQ for event in batch)
    dead_letter = harness.dead_letter_sink.dead_letters[0]
    assert dead_letter.failed_stage is FailedStage.PUBLISH
    assert dead_letter.attempt_count == 3
    assert dead_letter.error_class == "KafkaConnectionError"
    assert dead_letter.original_payload == batch[0].record.value
    assert dead_letter.first_failed_at <= dead_letter.last_failed_at
    assert harness.deduplicator.released_calls == [[event.dedupe_key for event in batch]]
    assert factory.all_committed_values == []
    await harness.pool.close()


async def test_dlq_write_failure_resolves_failed() -> None:
    factory = FakeProducerFactory(shared_commit_errors=[kafka_errors.KafkaConnectionError()])
    harness = await build_harness(factory, max_attempts=1, dlq_succeeds=False)
    batch = [make_pending_event()]
    result = await harness.publisher.publish_batch(batch)
    assert result.failed_count == 1
    outcome = batch[0].ack_future.result()
    assert outcome.status is EventStatus.FAILED
    assert "DLQ write also failed" in outcome.error_message
    await harness.pool.close()


async def test_poison_record_is_isolated_by_bisection() -> None:
    poison_value = b"poison-record"
    factory = FakeProducerFactory(
        producer_template={
            "poison_values": frozenset({poison_value}),
            "poison_error": kafka_errors.MessageSizeTooLargeError(),
        }
    )
    harness = await build_harness(factory)
    batch = [make_pending_event() for _ in range(8)]
    batch[5] = make_pending_event(value=poison_value)
    result = await harness.publisher.publish_batch(batch)

    statuses = [event.ack_future.result().status for event in batch]
    assert (
        statuses
        == [EventStatus.ACCEPTED] * 5 + [EventStatus.SENT_TO_DLQ] + [EventStatus.ACCEPTED] * 2
    )
    assert result.committed_count == 7
    assert result.dead_lettered_count == 1
    assert [letter.event_id for letter in harness.dead_letter_sink.dead_letters] == [
        batch[5].event_id
    ]
    assert poison_value not in factory.all_committed_values
    assert len(factory.all_committed_values) == 7
    await harness.pool.close()


async def test_fatal_error_recreates_producer_with_same_transactional_id() -> None:
    factory = FakeProducerFactory(shared_commit_errors=[kafka_errors.ProducerFenced()])
    harness = await build_harness(factory, max_attempts=3)
    batch = [make_pending_event() for _ in range(2)]
    await harness.publisher.publish_batch(batch)

    transactional_ids = [transactional_id for transactional_id, _ in factory.created]
    assert transactional_ids == ["tx-test-0-0", "tx-test-0-0"]
    first_producer, second_producer = (producer for _, producer in factory.created)
    assert first_producer.stopped
    assert first_producer.aborted_count == 0  # never reuse a fenced producer
    assert second_producer.committed_values == [event.record.value for event in batch]
    assert harness.metrics.producer_recreations_total._value.get() == 1
    await harness.pool.close()


async def test_commit_timeout_is_reported_unknown_and_not_retried() -> None:
    factory = FakeProducerFactory(producer_template={"commit_delay_seconds": 0.5})
    harness = await build_harness(factory, commit_timeout_seconds=0.05)
    batch = [make_pending_event()]
    result = await harness.publisher.publish_batch(batch)
    outcome = batch[0].ack_future.result()
    assert outcome.status is EventStatus.FAILED
    assert "commit outcome unknown" in outcome.error_message
    assert result.transaction_attempts == 1
    assert harness.dead_letter_sink.dead_letters == []
    assert harness.deduplicator.released_calls == []  # pending marker left to expire
    await harness.pool.close()


async def test_empty_batch_is_noop(harness: PublisherHarness) -> None:
    result = await harness.publisher.publish_batch([])
    assert result.batch_size == 0
