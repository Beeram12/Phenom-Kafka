"""Composition root: builds the object graph, runs the server, shuts down gracefully."""

from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from datetime import timedelta

import grpc
import structlog
from redis.asyncio import Redis

from ingestion.api.grpc_server import HEALTH_CHECK_METHOD, build_grpc_server
from ingestion.api.ingestion_servicer import IngestionServicer
from ingestion.api.interceptors import (
    ApiKeyAuthenticator,
    AuthInterceptor,
    RequestLoggingInterceptor,
    TenantContext,
)
from ingestion.batching.batch_accumulator import EventBatchAccumulator
from ingestion.config import Settings
from ingestion.dedupe.event_deduplicator import RedisEventDeduplicator
from ingestion.domain.protocols import Deduplicator
from ingestion.domain.routing import DLQ_TOPIC, TopicRouter
from ingestion.domain.validation import EventValidator
from ingestion.kafka.dlq_publisher import DlqPublisher
from ingestion.kafka.error_classifier import ErrorClassifier
from ingestion.kafka.producer_factory import KafkaProducerFactory
from ingestion.kafka.producer_pool import TransactionalProducerPool
from ingestion.kafka.serializers import EnvelopeSerializer
from ingestion.kafka.transactional_publisher import TransactionalPublisher
from ingestion.observability.logging import configure_logging, get_logger
from ingestion.observability.metrics import IngestionMetrics
from ingestion.resilience.retry_policy import ExponentialBackoffPolicy


@dataclass(slots=True)
class IngestionApplication:
    """The wired service. ``start``/``stop`` order matters for zero-loss shutdown."""

    settings: Settings
    server: grpc.aio.Server
    bound_port: int
    accumulator: EventBatchAccumulator
    servicer: IngestionServicer
    producer_pool: TransactionalProducerPool
    dlq_publisher: DlqPublisher
    redis_client: Redis
    logger: structlog.stdlib.BoundLogger

    async def start(self) -> None:
        """Connect producers first so we never accept events we cannot publish."""
        await self.redis_client.ping()
        await self.dlq_publisher.start()
        await self.producer_pool.start()
        self.accumulator.start()
        await self.server.start()
        self.logger.info(
            "ingestion_service_started",
            port=self.bound_port,
            instance_id=self.settings.instance_id,
        )

    async def stop(self) -> None:
        """Stop accepting -> let in-flight RPCs finish -> flush & commit -> close producers."""
        self.logger.info("ingestion_service_stopping")
        # 1. New events are refused (UNAVAILABLE) while queued ones keep flowing.
        stop_accumulator = asyncio.create_task(self.accumulator.stop())
        # 2. Stop taking RPCs; in-flight RPCs get the grace period to receive their acks.
        await self.server.stop(self.settings.grpc.shutdown_grace_seconds)
        # 3. Every queued event is flushed and committed (or dead-lettered).
        await stop_accumulator
        await self.servicer.drain_background_work()
        # 4. Close producers only after all transactions are finished.
        await self.producer_pool.close()
        await self.dlq_publisher.close()
        await self.redis_client.aclose()
        self.logger.info("ingestion_service_stopped")


def build_application(
    settings: Settings,
    metrics: IngestionMetrics,
    producer_factory: KafkaProducerFactory | None = None,
) -> IngestionApplication:
    """Wire every dependency by constructor injection.

    ``producer_factory`` can be overridden (e.g. with fault-injecting producers in tests).
    """
    logger = get_logger("ingestion")
    kafka_settings = settings.kafka
    retry_settings = settings.retry

    router = TopicRouter()
    validator = EventValidator(
        router, max_clock_skew=timedelta(seconds=settings.validation.max_clock_skew_seconds)
    )
    serializer = EnvelopeSerializer(
        producer_id=settings.producer_id,
        max_dlq_payload_bytes=kafka_settings.max_request_size - 64 * 1024,
    )
    error_classifier = ErrorClassifier()
    producer_factory = producer_factory or KafkaProducerFactory(kafka_settings)

    redis_client = Redis.from_url(
        settings.redis.url,
        socket_timeout=settings.redis.socket_timeout_seconds,
        socket_connect_timeout=settings.redis.socket_timeout_seconds,
    )
    deduplicator: Deduplicator = RedisEventDeduplicator(
        redis_client,
        committed_ttl_seconds=settings.redis.dedupe_ttl_seconds,
        pending_ttl_seconds=settings.redis.pending_ttl_seconds,
    )

    def backoff_policy(max_attempts: int, name: str) -> ExponentialBackoffPolicy:
        return ExponentialBackoffPolicy(
            base_delay_ms=retry_settings.base_delay_ms,
            multiplier=retry_settings.multiplier,
            max_delay_ms=retry_settings.max_delay_ms,
            max_attempts=max_attempts,
            logger=get_logger(f"ingestion.retry.{name}"),
        )

    request_timeout_seconds = kafka_settings.request_timeout_ms / 1000
    dlq_publisher = DlqPublisher(
        producer_factory=producer_factory,
        dlq_topic=DLQ_TOPIC.name,
        serializer=serializer,
        retry_policy=backoff_policy(retry_settings.dlq_max_attempts, "dlq"),
        error_classifier=error_classifier,
        metrics=metrics,
        logger=get_logger("ingestion.dlq"),
        start_timeout_seconds=kafka_settings.producer_start_timeout_ms / 1000,
        stop_timeout_seconds=kafka_settings.producer_stop_timeout_ms / 1000,
    )
    producer_pool = TransactionalProducerPool(
        producer_factory=producer_factory,
        pool_size=kafka_settings.producer_pool_size,
        transactional_id_prefix=kafka_settings.transactional_id_prefix,
        instance_id=settings.instance_id,
        start_timeout_seconds=kafka_settings.producer_start_timeout_ms / 1000,
        stop_timeout_seconds=kafka_settings.producer_stop_timeout_ms / 1000,
        metrics=metrics,
        logger=get_logger("ingestion.producer_pool"),
    )
    publisher = TransactionalPublisher(
        producer_pool=producer_pool,
        retry_policy=backoff_policy(retry_settings.max_attempts, "transaction"),
        error_classifier=error_classifier,
        dead_letter_sink=dlq_publisher,
        deduplicator=deduplicator,
        metrics=metrics,
        logger=get_logger("ingestion.publisher"),
        # aiokafka surfaces delivery errors after request_timeout; this is a safety net.
        send_timeout_seconds=request_timeout_seconds + 5,
        commit_timeout_seconds=request_timeout_seconds * 2,
    )
    accumulator = EventBatchAccumulator(
        batch_publisher=publisher,
        max_queue_size=settings.batching.max_queue_size,
        batch_max_records=settings.batching.batch_max_records,
        batch_max_bytes=settings.batching.batch_max_bytes,
        batch_flush_interval_ms=settings.batching.batch_flush_interval_ms,
        max_in_flight_batches=settings.effective_max_in_flight_batches,
        metrics=metrics,
        logger=get_logger("ingestion.accumulator"),
    )
    tenant_context = TenantContext()
    servicer = IngestionServicer(
        validator=validator,
        router=router,
        serializer=serializer,
        deduplicator=deduplicator,
        accumulator=accumulator,
        dead_letter_sink=dlq_publisher,
        tenant_resolver=tenant_context.current,
        metrics=metrics,
        logger=get_logger("ingestion.servicer"),
        instance_id=settings.instance_id,
        request_timeout_seconds=settings.grpc.request_timeout_ms / 1000,
        max_events_per_request=settings.grpc.max_events_per_request,
    )
    server, bound_port = build_grpc_server(
        servicer,
        interceptors=[
            RequestLoggingInterceptor(metrics, get_logger("ingestion.grpc")),
            AuthInterceptor(
                ApiKeyAuthenticator(settings.api_keys),
                tenant_context,
                public_methods=frozenset({HEALTH_CHECK_METHOD}),
            ),
        ],
        grpc_settings=settings.grpc,
    )
    return IngestionApplication(
        settings=settings,
        server=server,
        bound_port=bound_port,
        accumulator=accumulator,
        servicer=servicer,
        producer_pool=producer_pool,
        dlq_publisher=dlq_publisher,
        redis_client=redis_client,
        logger=logger,
    )


async def run(settings: Settings) -> None:
    """Run until SIGTERM/SIGINT, then shut down gracefully."""
    metrics = IngestionMetrics()
    metrics.serve(settings.metrics_port)
    application = build_application(settings, metrics)
    shutdown_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(shutdown_signal, shutdown_requested.set)

    try:
        await application.start()
    except BaseException:
        application.logger.exception("ingestion_service_start_failed")
        await application.stop()
        raise
    await shutdown_requested.wait()
    await application.stop()


def main() -> None:
    """Process entry point (``python -m ingestion.main``)."""
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
