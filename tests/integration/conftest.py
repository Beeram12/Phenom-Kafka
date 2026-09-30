"""Integration fixtures: the real service wired to the docker-compose Kafka + Redis.

Connection settings are pinned to the local stack (IT_KAFKA_BOOTSTRAP / IT_REDIS_URL) and
never read from .env, so a managed-cluster .env can't make tests write to it.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from urllib.parse import urlparse

import grpc
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import ConsumerRecord
from scripts.create_topics import create_topics

from ingestion.config import GrpcSettings, KafkaSettings, RedisSettings, RetrySettings, Settings
from ingestion.domain.routing import ALL_TOPICS
from ingestion.generated.events.v1 import ingestion_service_pb2_grpc
from ingestion.kafka.producer_factory import KafkaProducerFactory
from ingestion.main import IngestionApplication, build_application
from ingestion.observability.metrics import IngestionMetrics

KAFKA_BOOTSTRAP = os.environ.get("IT_KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_URL = os.environ.get("IT_REDIS_URL", "redis://localhost:6379/15")
API_KEY = "it-key"
TENANT_ID = "tenant_it"
AUTH_METADATA = (("x-api-key", API_KEY),)


def _reachable(host_port: str) -> bool:
    host, _, port = host_port.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    redis_address = urlparse(REDIS_URL)
    stack_up = _reachable(KAFKA_BOOTSTRAP) and _reachable(
        f"{redis_address.hostname}:{redis_address.port or 6379}"
    )
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not stack_up:
                item.add_marker(
                    pytest.mark.skip(reason="docker-compose stack not running (make up)")
                )


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="PLAINTEXT",
        producer_pool_size=2,
        request_timeout_ms=5_000,
        transaction_timeout_ms=30_000,
    )


@pytest.fixture
async def ensure_topics(kafka_settings: KafkaSettings) -> None:
    await create_topics(kafka_settings, ALL_TOPICS, replication_factor=1, min_insync_replicas=1)


@dataclass
class RunningService:
    application: IngestionApplication
    stub: ingestion_service_pb2_grpc.EventIngestionServiceAsyncStub


ServiceStarter = Callable[..., Awaitable[RunningService]]


@pytest.fixture
async def start_service(
    kafka_settings: KafkaSettings, ensure_topics: None
) -> AsyncIterator[ServiceStarter]:
    """Start the real service (optionally with a custom producer factory / retry settings)."""
    started: list[tuple[IngestionApplication, grpc.aio.Channel]] = []

    async def start(
        producer_factory: KafkaProducerFactory | None = None,
        max_attempts: int = 3,
    ) -> RunningService:
        settings = Settings(
            _env_file=None,  # type: ignore[call-arg]
            instance_id=f"it-{uuid.uuid4().hex[:8]}",
            metrics_port=0,
            api_keys={API_KEY: TENANT_ID},
            kafka=kafka_settings,
            redis=RedisSettings(url=REDIS_URL),
            retry=RetrySettings(base_delay_ms=20, max_delay_ms=100, max_attempts=max_attempts),
            grpc=GrpcSettings(host="127.0.0.1", port=0, request_timeout_ms=60_000),
        )
        application = build_application(settings, IngestionMetrics(), producer_factory)
        await application.start()
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{application.bound_port}")
        started.append((application, channel))
        stub = ingestion_service_pb2_grpc.EventIngestionServiceStub(channel)
        return RunningService(application, stub)

    yield start
    for application, channel in started:
        await channel.close(grace=None)
        await application.stop()


class TopicCursor:
    """Remembers end offsets, then reads only records produced afterwards."""

    def __init__(self, kafka_settings: KafkaSettings, topics: Iterable[str]) -> None:
        self._bootstrap_servers = kafka_settings.bootstrap_servers
        self._topics = list(topics)
        self._start_offsets: dict[TopicPartition, int] = {}

    async def _partitions(self) -> list[TopicPartition]:
        admin_client = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin_client.start()
        try:
            descriptions = await admin_client.describe_topics(self._topics)
        finally:
            await admin_client.close()
        return [
            TopicPartition(description["topic"], partition["partition"])
            for description in descriptions
            for partition in description["partitions"]
        ]

    async def mark(self) -> None:
        """Record the current end offset of every partition."""
        consumer = AIOKafkaConsumer(bootstrap_servers=self._bootstrap_servers)
        await consumer.start()
        try:
            self._start_offsets = await consumer.end_offsets(await self._partitions())
        finally:
            await consumer.stop()

    async def read_new(
        self, isolation_level: str = "read_committed", idle_timeout_seconds: float = 3.0
    ) -> list[ConsumerRecord]:
        """Every record after ``mark()`` visible under ``isolation_level``."""
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            isolation_level=isolation_level,
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            partitions = list(self._start_offsets)
            consumer.assign(partitions)
            for partition, offset in self._start_offsets.items():
                consumer.seek(partition, offset)
            end_offsets = await consumer.end_offsets(partitions)
            records: list[ConsumerRecord] = []
            last_progress = time.monotonic()
            while time.monotonic() - last_progress < idle_timeout_seconds:
                batches = await consumer.getmany(timeout_ms=300)
                for partition_records in batches.values():
                    records.extend(partition_records)
                if batches:
                    last_progress = time.monotonic()
                positions = [await consumer.position(partition) for partition in partitions]
                if all(
                    position >= end_offsets[partition]
                    for position, partition in zip(positions, partitions, strict=True)
                ):
                    break
            return records
        finally:
            await consumer.stop()


@pytest.fixture
async def topic_cursor(kafka_settings: KafkaSettings, ensure_topics: None) -> TopicCursor:
    cursor = TopicCursor(kafka_settings, [spec.name for spec in ALL_TOPICS])
    await cursor.mark()
    return cursor


@pytest.fixture
async def raw_transactional_producer(
    kafka_settings: KafkaSettings,
) -> AsyncIterator[AIOKafkaProducer]:
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_settings.bootstrap_servers,
        enable_idempotence=True,
        transactional_id=f"it-raw-{uuid.uuid4().hex[:8]}",
    )
    await producer.start()
    yield producer
    await asyncio.wait_for(producer.stop(), 10)
