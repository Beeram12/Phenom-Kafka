"""End-to-end verifier: reads every event topic with isolation_level=read_committed.

Checks:
  * no event_id appears twice in the committed log (exactly-once as seen by consumers)
  * committed events == ACCEPTED acks from the generator report (no loss, no extras)
  * each record's topic / key / headers match the routing registry
Also prints a DLQ breakdown by error_class and failed_stage.

With --report, only records whose Kafka timestamp falls inside the generator run window are
counted, so earlier runs on the same topics do not interfere.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypedDict

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import ConsumerRecord

from ingestion.config import KafkaSettings, Settings
from ingestion.domain.routing import DLQ_TOPIC, EVENT_TOPICS, TopicRouter
from ingestion.generated.events.v1 import dlq_pb2, events_pb2
from ingestion.kafka.connection import kafka_connection_options

REQUIRED_HEADERS = {"event_type", "schema_version", "tenant_id", "event_id", "producer_id"}
# Record timestamps are producer CreateTime (set at send), so a run's records fall between
# its start and its end plus in-flight acks. Keep the window tight so back-to-back runs on the
# same topics do not bleed into each other.
WINDOW_LEAD_MS = 1_000
WINDOW_TRAIL_MS = 5_000


class GeneratorReport(TypedDict):
    """Subset of generator_report.json used here."""

    started_at_ms: int
    finished_at_ms: int
    accepted_event_ids: list[str]
    duplicate_event_ids: list[str]
    unresolved_failed_event_ids: list[str]


@dataclass(slots=True)
class VerificationState:
    """Everything observed while reading the topics."""

    event_id_occurrences: Counter[str] = field(default_factory=Counter)
    committed_by_topic: Counter[str] = field(default_factory=Counter)
    routing_violations: list[str] = field(default_factory=list)
    dlq_by_error_class: Counter[str] = field(default_factory=Counter)
    dlq_by_stage: Counter[str] = field(default_factory=Counter)
    records_outside_window: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--bootstrap-servers", default=Settings().kafka.bootstrap_servers)
    parser.add_argument("--report", type=Path, default=None, help="generator_report.json")
    parser.add_argument(
        "--idle-timeout", type=float, default=5.0, help="seconds without new records"
    )
    return parser.parse_args()


async def _list_partitions(
    kafka_settings: KafkaSettings, topics: Iterable[str]
) -> list[TopicPartition]:
    admin_client = AIOKafkaAdminClient(**kafka_connection_options(kafka_settings))
    await admin_client.start()
    try:
        descriptions = await admin_client.describe_topics(list(topics))
    finally:
        await admin_client.close()
    return [
        TopicPartition(description["topic"], partition["partition"])
        for description in descriptions
        for partition in sorted(description["partitions"], key=lambda item: item["partition"])
    ]


async def _read_all(
    kafka_settings: KafkaSettings, topics: Iterable[str], idle_timeout_seconds: float
) -> list[ConsumerRecord]:
    partitions = await _list_partitions(kafka_settings, topics)
    consumer = AIOKafkaConsumer(
        **kafka_connection_options(kafka_settings),
        isolation_level="read_committed",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=None,
    )
    await consumer.start()
    try:
        consumer.assign(partitions)
        await consumer.seek_to_beginning(*partitions)
        end_offsets = await consumer.end_offsets(partitions)
        records: list[ConsumerRecord] = []
        last_progress = time.monotonic()
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=5_000)
            for partition_records in batches.values():
                records.extend(partition_records)
            if batches:
                last_progress = time.monotonic()
            positions = {partition: await consumer.position(partition) for partition in partitions}
            caught_up = all(
                positions[partition] >= end_offsets[partition] for partition in partitions
            )
            if caught_up or time.monotonic() - last_progress > idle_timeout_seconds:
                return records
    finally:
        await consumer.stop()


def _in_window(timestamp_ms: int, window: tuple[int, int] | None) -> bool:
    return window is None or window[0] <= timestamp_ms <= window[1]


def _check_event_record(
    record: ConsumerRecord, router: TopicRouter, state: VerificationState
) -> None:
    topic: str = record.topic
    headers = {name: value.decode() for name, value in record.headers}
    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(record.value)
    state.event_id_occurrences[envelope.event_id] += 1
    state.committed_by_topic[topic] += 1

    missing_headers = REQUIRED_HEADERS - headers.keys()
    if missing_headers:
        state.routing_violations.append(
            f"{envelope.event_id}: missing headers {sorted(missing_headers)}"
        )
    if (
        headers.get("event_id") != envelope.event_id
        or headers.get("tenant_id") != envelope.tenant_id
    ):
        state.routing_violations.append(f"{envelope.event_id}: headers disagree with payload")
    expected_route = router.resolve(envelope)
    if expected_route.topic != topic:
        state.routing_violations.append(
            f"{envelope.event_id}: on {topic}, expected {expected_route.topic}"
        )
    record_key = record.key.decode() if record.key else ""
    if record_key != expected_route.partition_key:
        state.routing_violations.append(
            f"{envelope.event_id}: key {record_key!r} != {expected_route.partition_key!r}"
        )


async def _main(arguments: argparse.Namespace) -> int:
    report: GeneratorReport | None = None
    window: tuple[int, int] | None = None
    if arguments.report:
        report = json.loads(arguments.report.read_text())
        window = (
            report["started_at_ms"] - WINDOW_LEAD_MS,
            report["finished_at_ms"] + WINDOW_TRAIL_MS,
        )

    event_topic_names = {spec.name for spec in EVENT_TOPICS}
    records = await _read_all(
        Settings().kafka.model_copy(update={"bootstrap_servers": arguments.bootstrap_servers}),
        [*event_topic_names, DLQ_TOPIC.name],
        arguments.idle_timeout,
    )
    router = TopicRouter()
    state = VerificationState()
    for record in records:
        if not _in_window(record.timestamp, window):
            state.records_outside_window += 1
            continue
        if record.topic in event_topic_names:
            _check_event_record(record, router, state)
        else:
            dlq_record = dlq_pb2.DlqRecord()
            dlq_record.ParseFromString(record.value)
            state.dlq_by_error_class[dlq_record.error_class] += 1
            state.dlq_by_stage[dlq_pb2.FailedStage.Name(dlq_record.failed_stage)] += 1

    duplicated_ids = {
        event_id: count for event_id, count in state.event_id_occurrences.items() if count > 1
    }
    committed_ids = set(state.event_id_occurrences)
    failures: list[str] = []

    print("=== consume_and_verify (read_committed) ===")
    print(
        f"records read           : {len(records)} "
        f"(outside run window: {state.records_outside_window})"
    )
    print(f"committed per topic    : {dict(state.committed_by_topic)}")
    print(f"unique committed events: {len(committed_ids)}")
    if duplicated_ids:
        failures.append(
            f"{len(duplicated_ids)} event_ids committed more than once, "
            f"e.g. {list(duplicated_ids)[:5]}"
        )
    if state.routing_violations:
        failures.append(
            f"{len(state.routing_violations)} routing/header violations, "
            f"e.g. {state.routing_violations[:3]}"
        )

    if report is not None:
        accepted_ids = set(report["accepted_event_ids"])
        # DUPLICATE means an earlier attempt committed, e.g. one whose ack timed out.
        confirmed_ids = accepted_ids | set(report.get("duplicate_event_ids", []))
        unresolved_ids = set(report.get("unresolved_failed_event_ids", []))
        print(f"ACCEPTED acks (report) : {len(accepted_ids)}")
        print(f"confirmed via DUPLICATE: {len(confirmed_ids - accepted_ids)}")
        missing = accepted_ids - committed_ids
        unexpected = committed_ids - confirmed_ids - unresolved_ids
        if missing:
            failures.append(f"{len(missing)} ACCEPTED events missing, e.g. {sorted(missing)[:5]}")
        if unexpected:
            failures.append(
                f"{len(unexpected)} committed events never acknowledged, "
                f"e.g. {sorted(unexpected)[:5]}"
            )
        late_commits = committed_ids & unresolved_ids
        if late_commits:
            print(f"WARNING: {len(late_commits)} events committed after their FAILED ack timeout")
        expected_count = len(confirmed_ids) + len(late_commits)
        if len(committed_ids) != expected_count:
            failures.append(f"committed {len(committed_ids)} != acknowledged {expected_count}")

    print("DLQ by error_class     :", dict(state.dlq_by_error_class.most_common()) or "{}")
    print("DLQ by failed_stage    :", dict(state.dlq_by_stage.most_common()) or "{}")
    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nOK: no duplicates" + (", committed == acknowledged" if report is not None else ""))
    return 0


def main() -> int:
    """CLI entry point."""
    return asyncio.run(_main(_parse_args()))


if __name__ == "__main__":
    sys.exit(main())
