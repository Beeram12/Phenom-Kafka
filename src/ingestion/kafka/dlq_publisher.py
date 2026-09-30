"""Dead-letter queue writer backed by its own NON-transactional idempotent producer."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import structlog
from aiokafka import AIOKafkaProducer

from ingestion.domain.protocols import DeadLetter
from ingestion.kafka.error_classifier import ErrorCategory, ErrorClassifier
from ingestion.kafka.producer_factory import KafkaProducerFactory
from ingestion.kafka.serializers import EnvelopeSerializer
from ingestion.observability.metrics import IngestionMetrics
from ingestion.resilience.retry_policy import ExponentialBackoffPolicy, RetryError


class DlqPublisher:
    """Writes DlqRecord protos to the DLQ topic.

    Uses a separate producer so a fenced or wedged transactional producer can never block
    dead-lettering. Each record is retried independently; a record that still cannot be
    written is reported as not written (the caller resolves it FAILED), logged at ERROR
    and counted - never silently dropped.
    """

    def __init__(
        self,
        *,
        producer_factory: KafkaProducerFactory,
        dlq_topic: str,
        serializer: EnvelopeSerializer,
        retry_policy: ExponentialBackoffPolicy,
        error_classifier: ErrorClassifier,
        metrics: IngestionMetrics,
        logger: structlog.stdlib.BoundLogger,
        start_timeout_seconds: float,
        stop_timeout_seconds: float,
    ) -> None:
        self._producer_factory = producer_factory
        self._dlq_topic = dlq_topic
        self._serializer = serializer
        self._retry_policy = retry_policy
        self._error_classifier = error_classifier
        self._metrics = metrics
        self._logger = logger
        self._start_timeout_seconds = start_timeout_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Create and start the DLQ producer."""
        producer = self._producer_factory.create_idempotent("dlq")
        async with asyncio.timeout(self._start_timeout_seconds):
            await producer.start()
        self._producer = producer
        self._logger.info("dlq_producer_started", topic=self._dlq_topic)

    async def close(self) -> None:
        """Flush and stop the DLQ producer."""
        producer, self._producer = self._producer, None
        if producer is None:
            return
        try:
            async with asyncio.timeout(self._stop_timeout_seconds):
                await producer.stop()
        except Exception as error:
            self._logger.warning("dlq_producer_stop_failed", error=str(error))

    async def send(self, dead_letters: Sequence[DeadLetter]) -> list[bool]:
        """Write every dead letter concurrently; returns a written flag per entry."""
        return list(await asyncio.gather(*(self._send_one(letter) for letter in dead_letters)))

    async def _send_one(self, dead_letter: DeadLetter) -> bool:
        producer = self._producer
        if producer is None:
            self._record_loss(dead_letter, "DLQ producer is not running")
            return False
        record = self._serializer.build_dead_letter_record(self._dlq_topic, dead_letter)
        try:
            await self._retry_policy.run(
                lambda: producer.send_and_wait(
                    record.topic, value=record.value, key=record.key, headers=record.headers
                ),
                should_retry=lambda error: (
                    self._error_classifier.classify(error) is not ErrorCategory.NON_RETRIABLE
                ),
                operation_name="dlq_send",
            )
        except RetryError as retry_error:
            self._record_loss(dead_letter, str(retry_error))
            return False
        self._metrics.dlq_records_total.labels(stage=dead_letter.failed_stage.value).inc()
        return True

    def _record_loss(self, dead_letter: DeadLetter, reason: str) -> None:
        self._metrics.dlq_write_failures_total.inc()
        self._logger.error(
            "dlq_write_failed",
            event_id=dead_letter.event_id,
            tenant_id=dead_letter.tenant_id,
            event_type=dead_letter.event_type,
            original_error_class=dead_letter.error_class,
            failed_stage=dead_letter.failed_stage.value,
            reason=reason,
        )
