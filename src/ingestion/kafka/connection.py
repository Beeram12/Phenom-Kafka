"""Kafka connection/security options shared by producers, consumers and admin clients."""

from __future__ import annotations

from aiokafka.helpers import create_ssl_context

from ingestion.config import KafkaSettings


def kafka_connection_options(kafka_settings: KafkaSettings) -> dict[str, object]:
    """Keyword arguments for any aiokafka client: bootstrap servers plus TLS / SASL."""
    options: dict[str, object] = {
        "bootstrap_servers": kafka_settings.bootstrap_servers,
        "security_protocol": kafka_settings.security_protocol,
    }
    if kafka_settings.security_protocol in ("SSL", "SASL_SSL"):
        options["ssl_context"] = create_ssl_context(
            cafile=str(kafka_settings.ssl_cafile) if kafka_settings.ssl_cafile else None,
            certfile=str(kafka_settings.ssl_certfile) if kafka_settings.ssl_certfile else None,
            keyfile=str(kafka_settings.ssl_keyfile) if kafka_settings.ssl_keyfile else None,
        )
    if kafka_settings.security_protocol.startswith("SASL"):
        options["sasl_mechanism"] = kafka_settings.sasl_mechanism
        options["sasl_plain_username"] = kafka_settings.sasl_username
        options["sasl_plain_password"] = (
            kafka_settings.sasl_password.get_secret_value()
            if kafka_settings.sasl_password
            else None
        )
    return options
