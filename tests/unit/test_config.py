"""Settings validation and Kafka connection options."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from ingestion.config import KafkaSettings, Settings
from ingestion.kafka.connection import kafka_connection_options


def test_defaults_match_spec() -> None:
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.batching.batch_max_records == 500
    assert settings.batching.batch_max_bytes == 1_048_576
    assert settings.batching.batch_flush_interval_ms == 100
    assert settings.retry.base_delay_ms == 100
    assert settings.retry.max_attempts == 5
    assert settings.kafka.producer_pool_size == 4
    assert settings.redis.dedupe_ttl_seconds == 7 * 24 * 3600
    assert settings.effective_max_in_flight_batches == 4


def test_nested_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_BATCHING__BATCH_MAX_RECORDS", "42")
    monkeypatch.setenv("INGEST_INSTANCE_ID", "pod-3")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.batching.batch_max_records == 42
    assert settings.producer_id == "event-ingestion-pod-3"


def test_pending_ttl_must_exceed_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_REDIS__PENDING_TTL_SECONDS", "10")
    with pytest.raises(ValidationError, match="pending_ttl_seconds"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_sasl_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="requires sasl_username"):
        KafkaSettings(security_protocol="SASL_SSL")


def test_missing_ca_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="certificate file not found"):
        KafkaSettings(security_protocol="SSL", ssl_cafile=tmp_path / "missing.pem")


def test_plaintext_connection_options() -> None:
    assert kafka_connection_options(KafkaSettings()) == {
        "bootstrap_servers": "localhost:9092",
        "security_protocol": "PLAINTEXT",
    }


def test_sasl_ssl_connection_options_hide_nothing_but_log_nothing() -> None:
    kafka_settings = KafkaSettings(
        bootstrap_servers="broker.example.com:28672",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="avnadmin",
        sasl_password="s3cret",  # type: ignore[arg-type]
    )
    options = kafka_connection_options(kafka_settings)
    assert options["security_protocol"] == "SASL_SSL"
    assert options["sasl_mechanism"] == "SCRAM-SHA-256"
    assert options["sasl_plain_username"] == "avnadmin"
    assert options["sasl_plain_password"] == "s3cret"
    assert options["ssl_context"] is not None
    assert "s3cret" not in repr(kafka_settings)  # SecretStr keeps it out of logs
