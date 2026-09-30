"""Idempotently create every topic declared in ingestion.domain.routing.

Existing topics are skipped (their partition count is reported, never changed).
Connection + security settings (SASL_SSL etc.) come from the same INGEST_KAFKA__* env / .env
as the service.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from ingestion.config import KafkaSettings, Settings
from ingestion.domain.routing import ALL_TOPICS, TopicSpec
from ingestion.kafka.connection import kafka_connection_options


def _parse_args(kafka_settings: KafkaSettings) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=kafka_settings.bootstrap_servers)
    parser.add_argument(
        "--replication-factor", type=int, default=kafka_settings.topic_replication_factor
    )
    parser.add_argument("--min-insync-replicas", type=int, default=None)
    parser.add_argument(
        "--max-partitions",
        type=int,
        default=None,
        help="cap partitions per topic (managed free tiers limit partitions)",
    )
    return parser.parse_args()


def _topic_configs(min_insync_replicas: int) -> dict[str, str]:
    return {
        "min.insync.replicas": str(min_insync_replicas),
        "compression.type": "producer",
        "retention.ms": str(7 * 24 * 3600 * 1000),
    }


async def create_topics(
    kafka_settings: KafkaSettings,
    topic_specs: tuple[TopicSpec, ...],
    replication_factor: int,
    min_insync_replicas: int,
    max_partitions: int | None = None,
) -> int:
    """Create missing topics; returns the number of topics created."""
    admin_client = AIOKafkaAdminClient(**kafka_connection_options(kafka_settings))
    await admin_client.start()
    try:
        existing_topics = set(await admin_client.list_topics())
        missing_specs = [spec for spec in topic_specs if spec.name not in existing_topics]
        for spec in topic_specs:
            if spec.name in existing_topics:
                print(f"skip    {spec.name} (already exists)")

        created_count = 0
        for spec in missing_specs:
            partitions = min(spec.partitions, max_partitions or spec.partitions)
            new_topic = NewTopic(
                name=spec.name,
                num_partitions=partitions,
                replication_factor=replication_factor,
                topic_configs=_topic_configs(min_insync_replicas),
            )
            try:
                response = await admin_client.create_topics([new_topic])
            except TopicAlreadyExistsError:
                print(f"skip    {spec.name} (created concurrently)")
                continue
            for topic_name, error_code, error_message in _topic_errors(response):
                if error_code == TopicAlreadyExistsError.errno:
                    print(f"skip    {topic_name} (created concurrently)")
                elif error_code:
                    raise RuntimeError(
                        f"failed to create {topic_name}: {error_code} {error_message}"
                    )
                else:
                    created_count += 1
                    print(f"created {topic_name} (partitions={partitions})")
        return created_count
    finally:
        await admin_client.close()


def _topic_errors(response: object) -> list[tuple[str, int, str | None]]:
    """Normalise CreateTopicsResponse.topic_errors across protocol versions."""
    topic_errors = getattr(response, "topic_errors", [])
    normalised: list[tuple[str, int, str | None]] = []
    for topic_error in topic_errors:
        topic_name, error_code = topic_error[0], topic_error[1]
        error_message = topic_error[2] if len(topic_error) > 2 else None
        normalised.append((topic_name, error_code, error_message))
    return normalised


def main() -> int:
    """CLI entry point."""
    kafka_settings = Settings().kafka
    arguments = _parse_args(kafka_settings)
    kafka_settings = kafka_settings.model_copy(
        update={"bootstrap_servers": arguments.bootstrap_servers}
    )
    # min.insync.replicas = RF-1 (at least 1): acks=all survives one broker loss.
    min_insync_replicas = arguments.min_insync_replicas or max(1, arguments.replication_factor - 1)
    asyncio.run(
        create_topics(
            kafka_settings,
            ALL_TOPICS,
            arguments.replication_factor,
            min_insync_replicas,
            arguments.max_partitions,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
