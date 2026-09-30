"""Construction of correctly-configured aiokafka producers."""

from __future__ import annotations

from typing import Protocol

from aiokafka import AIOKafkaProducer

from ingestion.config import KafkaSettings
from ingestion.kafka.connection import kafka_connection_options


class TransactionalProducerFactory(Protocol):
    """Anything that can build an un-started transactional producer."""

    def create_transactional(self, transactional_id: str) -> AIOKafkaProducer:
        """Build a producer bound to ``transactional_id``."""
        ...


class KafkaProducerFactory:
    """Builds idempotent producers, transactional or not, from KafkaSettings.

    Producers are returned un-started; the caller owns ``start()`` / ``stop()``.
    Tests subclass this to inject fault-injecting producers.
    """

    def __init__(self, kafka_settings: KafkaSettings) -> None:
        self._kafka_settings = kafka_settings

    def _common_config(self, client_id: str) -> dict[str, object]:
        settings = self._kafka_settings
        return {
            **kafka_connection_options(settings),
            "client_id": client_id,
            "enable_idempotence": True,
            "acks": "all",
            "compression_type": settings.compression_type,
            "linger_ms": settings.linger_ms,
            "max_batch_size": settings.max_batch_size,
            "max_request_size": settings.max_request_size,
            "request_timeout_ms": settings.request_timeout_ms,
        }

    def create_transactional(self, transactional_id: str) -> AIOKafkaProducer:
        """Idempotent + transactional producer. ``transactional_id`` must be stable."""
        return AIOKafkaProducer(
            **self._common_config(f"{self._kafka_settings.client_id}-{transactional_id}"),
            transactional_id=transactional_id,
            transaction_timeout_ms=self._kafka_settings.transaction_timeout_ms,
        )

    def create_idempotent(self, client_suffix: str) -> AIOKafkaProducer:
        """Idempotent, NON-transactional producer (used for the DLQ)."""
        return AIOKafkaProducer(
            **self._common_config(f"{self._kafka_settings.client_id}-{client_suffix}")
        )
