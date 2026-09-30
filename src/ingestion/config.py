"""Typed service configuration loaded from environment variables and an optional .env file.

Every setting lives under the ``INGEST_`` prefix; nested groups use ``__`` as the
delimiter, e.g. ``INGEST_KAFKA__BOOTSTRAP_SERVERS=broker:9092`` or
``INGEST_BATCHING__BATCH_MAX_RECORDS=1000``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseModel):
    """Kafka connection, security and transactional producer settings.

    Managed Kafka (e.g. Aiven) typically needs security_protocol=SASL_SSL,
    sasl_mechanism=SCRAM-SHA-256, username/password and the service CA certificate.
    """

    bootstrap_servers: str = "localhost:9092"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = "SCRAM-SHA-256"
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    ssl_cafile: Path | None = Field(default=None, description="CA certificate (PEM).")
    ssl_certfile: Path | None = Field(default=None, description="Client cert (mTLS only).")
    ssl_keyfile: Path | None = Field(default=None, description="Client key (mTLS only).")
    topic_replication_factor: int = Field(default=1, ge=1, description="Used by create_topics.")
    client_id: str = "event-ingestion"
    transactional_id_prefix: str = "event-ingestion-tx"
    producer_pool_size: int = Field(default=4, ge=1, le=64)
    compression_type: str = "lz4"
    linger_ms: int = Field(default=10, ge=0)
    max_batch_size: int = Field(default=65_536, ge=1_024)
    max_request_size: int = Field(default=1_048_576, ge=1_024)
    request_timeout_ms: int = Field(default=15_000, ge=1_000)
    transaction_timeout_ms: int = Field(default=60_000, ge=1_000)
    producer_start_timeout_ms: int = Field(default=30_000, ge=1_000)
    producer_stop_timeout_ms: int = Field(default=10_000, ge=100)

    @model_validator(mode="after")
    def _check_security(self) -> KafkaSettings:
        if self.security_protocol.startswith("SASL") and not (
            self.sasl_username and self.sasl_password
        ):
            raise ValueError(f"{self.security_protocol} requires sasl_username and sasl_password")
        for certificate_path in (self.ssl_cafile, self.ssl_certfile, self.ssl_keyfile):
            if certificate_path is not None and not certificate_path.is_file():
                raise ValueError(f"certificate file not found: {certificate_path}")
        return self


class BatchingSettings(BaseModel):
    """Micro-batching limits: a batch is flushed as soon as ANY limit is reached."""

    batch_max_records: int = Field(default=500, ge=1)
    batch_max_bytes: int = Field(default=1_048_576, ge=1_024)
    batch_flush_interval_ms: int = Field(default=100, ge=1)
    max_queue_size: int = Field(default=10_000, ge=1)
    max_in_flight_batches: int | None = Field(
        default=None, ge=1, description="Defaults to the producer pool size."
    )


class RetrySettings(BaseModel):
    """Exponential backoff with full jitter, applied to whole-batch transactions."""

    base_delay_ms: int = Field(default=100, ge=1)
    multiplier: float = Field(default=2.0, ge=1.0)
    max_delay_ms: int = Field(default=5_000, ge=1)
    max_attempts: int = Field(default=5, ge=1)
    dlq_max_attempts: int = Field(default=5, ge=1)


class RedisSettings(BaseModel):
    """Redis connection and deduplication TTLs."""

    url: str = "redis://localhost:6379/0"
    dedupe_ttl_seconds: int = Field(default=7 * 24 * 3600, ge=60)
    pending_ttl_seconds: int = Field(
        default=300,
        ge=5,
        description="TTL of the in-flight marker. Must exceed the request timeout.",
    )
    socket_timeout_seconds: float = Field(default=2.0, gt=0)


class GrpcSettings(BaseModel):
    """gRPC server settings."""

    host: str = "0.0.0.0"
    port: int = Field(default=50051, ge=0, le=65_535, description="0 = ephemeral port.")
    max_events_per_request: int = Field(default=1_000, ge=1)
    request_timeout_ms: int = Field(default=30_000, ge=100)
    shutdown_grace_seconds: float = Field(default=20.0, ge=0)
    max_receive_message_bytes: int = Field(default=8 * 1_048_576, ge=1_024)


class ValidationSettings(BaseModel):
    """Event validation tolerances."""

    max_clock_skew_seconds: int = Field(default=300, ge=0)


class Settings(BaseSettings):
    """Root settings object; build once in the composition root and inject everywhere."""

    model_config = SettingsConfigDict(
        env_prefix="INGEST_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    service_name: str = "event-ingestion"
    instance_id: str = Field(
        default="local-0",
        description="Stable identity (pod name / StatefulSet ordinal). Used in transactional ids "
        "so a restarted instance fences its own zombie producers.",
    )
    log_level: str = "INFO"
    metrics_port: int = Field(default=9100, ge=0, le=65_535, description="0 disables /metrics.")
    api_keys: dict[str, str] = Field(
        default_factory=lambda: {
            f"dev-key-tenant-{tenant_number}": f"tenant_{tenant_number}"
            for tenant_number in range(1, 6)
        },
        description='API key -> tenant_id. JSON in env, e.g. INGEST_API_KEYS=\'{"k1":"t1"}\'.',
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    batching: BatchingSettings = Field(default_factory=BatchingSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    grpc: GrpcSettings = Field(default_factory=GrpcSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)

    @model_validator(mode="after")
    def _check_cross_field_invariants(self) -> Settings:
        request_timeout_seconds = self.grpc.request_timeout_ms / 1000
        if self.redis.pending_ttl_seconds <= request_timeout_seconds:
            raise ValueError(
                "redis.pending_ttl_seconds must exceed grpc.request_timeout_ms, otherwise an "
                "in-flight event could be accepted twice by a concurrent resend"
            )
        if self.batching.batch_max_bytes > self.kafka.max_request_size * 64:
            raise ValueError(
                "batching.batch_max_bytes is unreasonably larger than max_request_size"
            )
        if not self.instance_id.strip():
            raise ValueError("instance_id must be a stable, non-empty identifier")
        return self

    @property
    def effective_max_in_flight_batches(self) -> int:
        """Concurrent batches allowed; one per pooled transactional producer by default."""
        return self.batching.max_in_flight_batches or self.kafka.producer_pool_size

    @property
    def producer_id(self) -> str:
        """Value of the `producer_id` Kafka header."""
        return f"{self.service_name}-{self.instance_id}"
