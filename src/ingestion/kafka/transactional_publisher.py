"""Publishes one batch per Kafka transaction and resolves every event's ack future."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from aiokafka import AIOKafkaProducer

from ingestion.domain.models import (
    BatchResult,
    EventOutcome,
    EventStatus,
    FailedStage,
    PendingEvent,
)
from ingestion.domain.protocols import DeadLetter, DeadLetterSink, Deduplicator
from ingestion.kafka.error_classifier import ErrorCategory, ErrorClassifier
from ingestion.kafka.producer_pool import ProducerSlot, TransactionalProducerPool
from ingestion.kafka.serializers import SerializationError
from ingestion.observability.metrics import IngestionMetrics
from ingestion.resilience.retry_policy import ExponentialBackoffPolicy, RetryError


class CommitOutcomeUnknownError(Exception):
    """commit_transaction did not finish in time: the batch may or may not be committed.

    Retrying could duplicate the batch, dead-lettering could lose it. Such events are
    reported FAILED and their pending dedupe markers are left to expire.
    """


class SendPhaseTimeoutError(TimeoutError):
    """begin + send + delivery did not finish in time; the transaction was aborted (retriable)."""


@dataclass(slots=True)
class _AttemptLog:
    """Bookkeeping across the attempts made for one (sub-)batch."""

    attempts: int = 0
    first_failed_at: datetime | None = None
    last_failed_at: datetime | None = None


class TransactionalPublisher:
    """Commits a batch as ONE Kafka transaction (spanning topics) with retries.

    Flow per batch:
      * lease a producer -> begin -> send all records -> await all sends -> commit
      * success: mark dedupe keys committed, resolve futures ACCEPTED
      * RETRIABLE error: abort, back off (full jitter), retry the WHOLE batch with the same
        event ids. Aborted records are invisible to read_committed consumers.
      * FATAL error: discard the producer (recreated with the same transactional_id on the
        next attempt, which fences the old epoch), then retry
      * NON_RETRIABLE error: bisect the batch to isolate poison records; only those go to
        the DLQ while the rest still commit
      * retries exhausted: dead-letter the whole batch, resolve SENT_TO_DLQ (or FAILED if
        even the DLQ write fails), release pending dedupe markers
    """

    def __init__(
        self,
        *,
        producer_pool: TransactionalProducerPool,
        retry_policy: ExponentialBackoffPolicy,
        error_classifier: ErrorClassifier,
        dead_letter_sink: DeadLetterSink,
        deduplicator: Deduplicator,
        metrics: IngestionMetrics,
        logger: structlog.stdlib.BoundLogger,
        send_timeout_seconds: float,
        commit_timeout_seconds: float,
        abort_timeout_seconds: float = 10.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._producer_pool = producer_pool
        self._retry_policy = retry_policy
        self._error_classifier = error_classifier
        self._dead_letter_sink = dead_letter_sink
        self._deduplicator = deduplicator
        self._metrics = metrics
        self._logger = logger
        self._send_timeout_seconds = send_timeout_seconds
        self._commit_timeout_seconds = commit_timeout_seconds
        self._abort_timeout_seconds = abort_timeout_seconds
        self._clock = clock

    async def publish_batch(self, batch: Sequence[PendingEvent]) -> BatchResult:
        """Publish ``batch``; every event future is resolved before this returns."""
        pending_events = list(batch)
        if not pending_events:
            return BatchResult(batch_size=0)
        started_at = time.monotonic()
        self._metrics.batch_size_records.observe(len(pending_events))
        try:
            result = await self._publish_or_isolate(pending_events)
        except BaseException as error:
            # Never leave a gRPC handler waiting forever (e.g. cancelled during shutdown).
            for pending_event in pending_events:
                pending_event.resolve(
                    EventOutcome(EventStatus.FAILED, f"internal error: {type(error).__name__}")
                )
            raise
        self._metrics.batch_commit_seconds.observe(time.monotonic() - started_at)
        self._logger.info(
            "batch_published",
            batch_size=result.batch_size,
            committed=result.committed_count,
            dead_lettered=result.dead_lettered_count,
            failed=result.failed_count,
            transaction_attempts=result.transaction_attempts,
            duration_ms=round((time.monotonic() - started_at) * 1000, 1),
        )
        return result

    async def _publish_or_isolate(self, batch: list[PendingEvent]) -> BatchResult:
        attempt_log = _AttemptLog()
        try:
            await self._retry_policy.run(
                lambda: self._attempt_transaction(batch, attempt_log),
                should_retry=self._should_retry,
                operation_name="kafka_transaction",
            )
        except RetryError as retry_error:
            return await self._handle_failure(batch, retry_error, attempt_log)
        await self._acknowledge(batch)
        return BatchResult(
            batch_size=len(batch),
            committed_count=len(batch),
            transaction_attempts=attempt_log.attempts,
        )

    async def _handle_failure(
        self, batch: list[PendingEvent], retry_error: RetryError, attempt_log: _AttemptLog
    ) -> BatchResult:
        error = retry_error.last_error
        if isinstance(error, CommitOutcomeUnknownError):
            return self._fail_with_unknown_outcome(batch, error, attempt_log)

        category = self._error_classifier.classify(error)
        if category is ErrorCategory.NON_RETRIABLE and not retry_error.exhausted:
            if len(batch) > 1:
                return await self._bisect(batch, error, attempt_log)
            stage = (
                FailedStage.SERIALIZATION
                if isinstance(error, SerializationError)
                else FailedStage.PUBLISH
            )
            return await self._dead_letter(batch, error, stage, attempt_log)
        return await self._dead_letter(batch, error, FailedStage.PUBLISH, attempt_log)

    async def _bisect(
        self, batch: list[PendingEvent], error: Exception, attempt_log: _AttemptLog
    ) -> BatchResult:
        middle = len(batch) // 2
        self._logger.warning(
            "batch_bisected",
            batch_size=len(batch),
            error_class=type(error).__name__,
            error=str(error),
        )
        # Sequential halves keep per-partition order and hold one producer at a time.
        left_result = await self._publish_or_isolate(batch[:middle])
        right_result = await self._publish_or_isolate(batch[middle:])
        own_attempts = BatchResult(batch_size=0, transaction_attempts=attempt_log.attempts)
        return own_attempts.merge(left_result).merge(right_result)

    def _should_retry(self, error: Exception) -> bool:
        if isinstance(error, CommitOutcomeUnknownError):
            return False
        return self._error_classifier.classify(error) is not ErrorCategory.NON_RETRIABLE

    async def _attempt_transaction(
        self, batch: list[PendingEvent], attempt_log: _AttemptLog
    ) -> None:
        attempt_log.attempts += 1
        try:
            async with self._producer_pool.lease() as slot:
                producer = await self._producer_pool.ensure_started(slot)
                await self._send_all(slot, producer, batch)
                await self._commit(slot, producer)
        except Exception as error:
            now = self._clock()
            attempt_log.first_failed_at = attempt_log.first_failed_at or now
            attempt_log.last_failed_at = now
            category = self._error_classifier.classify(error)
            self._metrics.transaction_attempts_total.labels(result="failed").inc()
            self._metrics.publish_errors_total.labels(
                category=category.value, error_class=type(error).__name__
            ).inc()
            raise
        self._metrics.transaction_attempts_total.labels(result="committed").inc()

    async def _send_all(
        self, slot: ProducerSlot, producer: AIOKafkaProducer, batch: list[PendingEvent]
    ) -> None:
        """Begin the transaction and send every record; aborts on any failure."""
        try:
            async with asyncio.timeout(self._send_timeout_seconds):
                await producer.begin_transaction()
                delivery_futures = []
                for pending_event in batch:
                    record = pending_event.record
                    delivery_futures.append(
                        await producer.send(
                            record.topic, value=record.value, key=record.key, headers=record.headers
                        )
                    )
                delivery_results = await asyncio.gather(*delivery_futures, return_exceptions=True)
                for delivery_result in delivery_results:
                    if isinstance(delivery_result, BaseException):
                        raise delivery_result
        except TimeoutError as error:
            timeout_error = SendPhaseTimeoutError(
                f"send phase exceeded {self._send_timeout_seconds}s for {len(batch)} records"
            )
            await self._abort(slot, producer, timeout_error)
            raise timeout_error from error
        except Exception as error:
            await self._abort(slot, producer, error)
            raise

    async def _commit(self, slot: ProducerSlot, producer: AIOKafkaProducer) -> None:
        try:
            async with asyncio.timeout(self._commit_timeout_seconds):
                await producer.commit_transaction()
        except TimeoutError as error:
            await self._producer_pool.discard(slot, reason="commit timed out")
            raise CommitOutcomeUnknownError(
                f"commit did not complete within {self._commit_timeout_seconds}s"
            ) from error
        except Exception as error:
            await self._abort(slot, producer, error)
            raise

    async def _abort(
        self, slot: ProducerSlot, producer: AIOKafkaProducer, cause: Exception
    ) -> None:
        """Abort the open transaction, or discard the producer if it cannot be aborted."""
        if self._error_classifier.classify(cause) is ErrorCategory.FATAL:
            await self._producer_pool.discard(slot, reason=f"fatal: {type(cause).__name__}")
            return
        try:
            async with asyncio.timeout(self._abort_timeout_seconds):
                await producer.abort_transaction()
        except Exception as abort_error:
            await self._producer_pool.discard(
                slot, reason=f"abort failed: {type(abort_error).__name__}: {abort_error}"
            )

    async def _acknowledge(self, batch: list[PendingEvent]) -> None:
        """The transaction committed: record dedupe keys, then ACK every event."""
        try:
            await self._deduplicator.mark_committed([event.dedupe_key for event in batch])
        except Exception as error:
            # The data IS committed, so the ACK stands; only resend protection is weakened
            # until the pending markers expire.
            self._metrics.dedupe_errors_total.labels(operation="mark_committed").inc()
            self._logger.error(
                "dedupe_mark_committed_failed",
                batch_size=len(batch),
                error_class=type(error).__name__,
                error=str(error),
            )
        for pending_event in batch:
            pending_event.resolve(EventOutcome(EventStatus.ACCEPTED))

    async def _dead_letter(
        self,
        batch: list[PendingEvent],
        error: Exception,
        stage: FailedStage,
        attempt_log: _AttemptLog,
    ) -> BatchResult:
        now = self._clock()
        error_description = f"{type(error).__name__}: {error}"
        dead_letters = [
            DeadLetter(
                original_topic=pending_event.record.topic,
                original_payload=pending_event.record.value,
                event_id=pending_event.event_id,
                tenant_id=pending_event.tenant_id,
                event_type=pending_event.envelope.event_type,
                error_class=type(error).__name__,
                error_message=str(error),
                failed_stage=stage,
                attempt_count=attempt_log.attempts,
                first_failed_at=attempt_log.first_failed_at or now,
                last_failed_at=attempt_log.last_failed_at or now,
            )
            for pending_event in batch
        ]
        written_flags = await self._dead_letter_sink.send(dead_letters)
        dead_lettered_count = 0
        for pending_event, written in zip(batch, written_flags, strict=True):
            if written:
                dead_lettered_count += 1
                pending_event.resolve(
                    EventOutcome(EventStatus.SENT_TO_DLQ, f"{stage.value}: {error_description}")
                )
            else:
                pending_event.resolve(
                    EventOutcome(
                        EventStatus.FAILED,
                        f"{stage.value}: {error_description}; DLQ write also failed",
                    )
                )
        failed_count = len(batch) - dead_lettered_count
        await self._release_markers(batch)
        return BatchResult(
            batch_size=len(batch),
            dead_lettered_count=dead_lettered_count,
            failed_count=failed_count,
            transaction_attempts=attempt_log.attempts,
        )

    def _fail_with_unknown_outcome(
        self, batch: list[PendingEvent], error: Exception, attempt_log: _AttemptLog
    ) -> BatchResult:
        # Pending markers are deliberately kept: they expire after the pending TTL, which
        # blocks an immediate resend from double-publishing if the commit did land.
        self._logger.error(
            "batch_commit_outcome_unknown",
            batch_size=len(batch),
            event_ids=[pending_event.event_id for pending_event in batch[:20]],
            error=str(error),
        )
        for pending_event in batch:
            pending_event.resolve(
                EventOutcome(
                    EventStatus.FAILED,
                    f"commit outcome unknown ({error}); retry after the dedupe pending TTL",
                )
            )
        return BatchResult(
            batch_size=len(batch),
            failed_count=len(batch),
            transaction_attempts=attempt_log.attempts,
        )

    async def _release_markers(self, batch: list[PendingEvent]) -> None:
        try:
            await self._deduplicator.release([event.dedupe_key for event in batch])
        except Exception as error:
            self._metrics.dedupe_errors_total.labels(operation="release").inc()
            self._logger.error(
                "dedupe_release_failed",
                batch_size=len(batch),
                error_class=type(error).__name__,
                error=str(error),
            )
