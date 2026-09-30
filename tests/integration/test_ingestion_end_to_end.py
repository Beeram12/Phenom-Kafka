"""End-to-end tests against the docker-compose Kafka + Redis (run `make up` first)."""

from __future__ import annotations

import asyncio
import os
import shutil
from collections import Counter

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka import errors as kafka_errors

from ingestion.config import KafkaSettings
from ingestion.domain.routing import ROUTING_REGISTRY, TopicRouter
from ingestion.generated.events.v1 import dlq_pb2, events_pb2, ingestion_service_pb2
from ingestion.kafka.producer_factory import KafkaProducerFactory
from tests.integration.conftest import AUTH_METADATA, TENANT_ID, ServiceStarter, TopicCursor
from tests.support import make_envelope

pytestmark = pytest.mark.integration


class FlakyCommitProducer(AIOKafkaProducer):  # type: ignore[misc]
    """Real producer whose commit fails while ``failures_remaining`` > 0.

    The records have already been written to the partition logs when the commit fails,
    so the publisher must abort (making them invisible) and retry.
    """

    def __init__(self, failures_remaining: list[int], **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._failures_remaining = failures_remaining

    async def commit_transaction(self) -> None:
        if self._failures_remaining[0] > 0:
            self._failures_remaining[0] -= 1
            await self.flush()  # make sure the records really reached the broker
            raise kafka_errors.KafkaConnectionError("injected broker failure before commit")
        await super().commit_transaction()


class FlakyCommitProducerFactory(KafkaProducerFactory):
    """Builds FlakyCommitProducers that share one failure budget."""

    def __init__(self, kafka_settings: KafkaSettings, commit_failures: int) -> None:
        super().__init__(kafka_settings)
        self.failures_remaining = [commit_failures]

    def create_transactional(self, transactional_id: str) -> AIOKafkaProducer:
        return FlakyCommitProducer(
            self.failures_remaining,
            **self._common_config(f"it-{transactional_id}"),
            transactional_id=transactional_id,
            transaction_timeout_ms=30_000,
        )


async def publish(
    stub: object, events: list[events_pb2.EventEnvelope]
) -> list[ingestion_service_pb2.EventResult]:
    response = await stub.PublishEvents(  # type: ignore[attr-defined]
        ingestion_service_pb2.PublishEventsRequest(request_id="it", events=events),
        metadata=AUTH_METADATA,
    )
    return list(response.results)


def event_ids_on_topics(records: list[object], event_ids: set[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for record in records:
        if record.topic == "food.events.dlq":  # type: ignore[attr-defined]
            continue
        envelope = events_pb2.EventEnvelope.FromString(record.value)  # type: ignore[attr-defined]
        if envelope.event_id in event_ids:
            counts[envelope.event_id] += 1
    return counts


async def test_happy_path_commits_multi_topic_batch_and_acks(
    start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    service = await start_service()
    events = [make_envelope(event_type, tenant_id="spoofed") for event_type in ROUTING_REGISTRY]
    results = await publish(service.stub, events)
    assert [result.status for result in results] == [ingestion_service_pb2.ACCEPTED] * len(events)

    records = await topic_cursor.read_new("read_committed")
    by_event_id = {
        events_pb2.EventEnvelope.FromString(record.value).event_id: record
        for record in records
        if record.topic != "food.events.dlq"
    }
    router = TopicRouter()
    for sent in events:
        record = by_event_id[sent.event_id]
        stored = events_pb2.EventEnvelope.FromString(record.value)
        assert stored.tenant_id == TENANT_ID  # client value was overwritten
        assert stored.HasField("received_time")
        route = router.resolve(stored)
        assert record.topic == route.topic
        assert record.key.decode() == route.partition_key
        headers = {name: value.decode() for name, value in record.headers}
        assert headers["event_type"] == sent.event_type
        assert headers["schema_version"] == "1"
        assert headers["tenant_id"] == TENANT_ID
        assert headers["event_id"] == sent.event_id
        assert headers["producer_id"].startswith("event-ingestion-it-")


async def test_duplicate_submission_returns_duplicate_and_commits_once(
    start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    service = await start_service()
    envelope = make_envelope("order_placed")
    first = await publish(service.stub, [envelope])
    second = await publish(service.stub, [envelope])
    assert first[0].status == ingestion_service_pb2.ACCEPTED
    assert second[0].status == ingestion_service_pb2.DUPLICATE

    records = await topic_cursor.read_new("read_committed")
    assert event_ids_on_topics(records, {envelope.event_id}) == {envelope.event_id: 1}


async def test_broker_failure_mid_batch_retries_and_commits_exactly_once(
    kafka_settings: KafkaSettings, start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    factory = FlakyCommitProducerFactory(kafka_settings, commit_failures=2)
    service = await start_service(producer_factory=factory, max_attempts=4)
    events = [make_envelope("item_viewed") for _ in range(20)]
    results = await publish(service.stub, events)
    assert {result.status for result in results} == {ingestion_service_pb2.ACCEPTED}
    assert factory.failures_remaining == [0]

    event_ids = {event.event_id for event in events}
    committed = event_ids_on_topics(await topic_cursor.read_new("read_committed"), event_ids)
    assert committed == dict.fromkeys(event_ids, 1)  # retried batch, still exactly once

    # The two aborted attempts physically exist in the log but are invisible when committed.
    uncommitted = event_ids_on_topics(await topic_cursor.read_new("read_uncommitted"), event_ids)
    assert uncommitted == dict.fromkeys(event_ids, 3)


async def test_persistent_broker_failure_sends_batch_to_dlq(
    kafka_settings: KafkaSettings, start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    factory = FlakyCommitProducerFactory(kafka_settings, commit_failures=1_000)
    service = await start_service(producer_factory=factory, max_attempts=3)
    events = [make_envelope("cart_item_added") for _ in range(5)]
    results = await publish(service.stub, events)
    assert {result.status for result in results} == {ingestion_service_pb2.SENT_TO_DLQ}

    records = await topic_cursor.read_new("read_committed")
    event_ids = {event.event_id for event in events}
    assert event_ids_on_topics(records, event_ids) == {}
    dlq_records = [
        dlq_pb2.DlqRecord.FromString(record.value)
        for record in records
        if record.topic == "food.events.dlq"
    ]
    dead_letters = {
        record.event_id: record for record in dlq_records if record.event_id in event_ids
    }
    assert set(dead_letters) == event_ids
    for dead_letter in dead_letters.values():
        assert dead_letter.error_class == "KafkaConnectionError"
        assert dead_letter.failed_stage == dlq_pb2.PUBLISH
        assert dead_letter.attempt_count == 3
        assert dead_letter.original_topic == "food.browse.v1"
        assert dead_letter.tenant_id == TENANT_ID
        assert (
            events_pb2.EventEnvelope.FromString(dead_letter.original_payload).event_id in event_ids
        )

    # Dedupe markers were released, so the client may resend once Kafka is healthy.
    factory.failures_remaining[0] = 0
    retry_results = await publish(service.stub, events)
    assert {result.status for result in retry_results} == {ingestion_service_pb2.ACCEPTED}


async def test_invalid_event_is_rejected_and_dead_lettered(
    start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    service = await start_service()
    invalid = make_envelope("rating_submitted")
    invalid.rating_submitted.overall_rating = 7
    [result] = await publish(service.stub, [invalid])
    assert result.status == ingestion_service_pb2.REJECTED_INVALID
    await asyncio.sleep(0.5)  # the DLQ write may finish just after the response

    records = await topic_cursor.read_new("read_committed")
    dead_letters = [
        dlq_pb2.DlqRecord.FromString(record.value)
        for record in records
        if record.topic == "food.events.dlq"
    ]
    [dead_letter] = [letter for letter in dead_letters if letter.event_id == invalid.event_id]
    assert dead_letter.failed_stage == dlq_pb2.VALIDATION
    assert dead_letter.error_class == "ValidationError"
    assert "overall_rating" in dead_letter.error_message


async def test_aborted_transaction_is_invisible_to_read_committed(
    raw_transactional_producer: AIOKafkaProducer, topic_cursor: TopicCursor
) -> None:
    marker = make_envelope("session_started")
    await raw_transactional_producer.begin_transaction()
    await raw_transactional_producer.send_and_wait(
        "food.sessions.v1", value=marker.SerializeToString(), key=b"tenant_it:user_1"
    )
    await raw_transactional_producer.abort_transaction()

    committed = await topic_cursor.read_new("read_committed")
    uncommitted = await topic_cursor.read_new("read_uncommitted")
    assert event_ids_on_topics(committed, {marker.event_id}) == {}
    assert event_ids_on_topics(uncommitted, {marker.event_id}) == {marker.event_id: 1}


@pytest.mark.skipif(
    os.environ.get("IT_CHAOS") != "1" or shutil.which("docker") is None,
    reason="real broker outage test; set IT_CHAOS=1 (pauses the kafka container)",
)
async def test_real_broker_pause_mid_batch_commits_exactly_once(
    start_service: ServiceStarter, topic_cursor: TopicCursor
) -> None:
    service = await start_service(max_attempts=5)
    events = [make_envelope("item_viewed") for _ in range(50)]

    async def docker(action: str) -> None:
        process = await asyncio.create_subprocess_exec("docker", action, "ingestion-kafka")
        assert await process.wait() == 0

    async def pause_broker_briefly() -> None:
        await asyncio.sleep(0.05)
        await docker("pause")
        try:
            await asyncio.sleep(8)
        finally:
            await docker("unpause")

    outage = asyncio.create_task(pause_broker_briefly())
    try:
        results = await publish(service.stub, events)
    finally:
        await outage
    statuses = {result.status for result in results}
    assert statuses <= {ingestion_service_pb2.ACCEPTED, ingestion_service_pb2.SENT_TO_DLQ}

    accepted_ids = {
        result.event_id for result in results if result.status == ingestion_service_pb2.ACCEPTED
    }
    committed = event_ids_on_topics(
        await topic_cursor.read_new("read_committed"), {event.event_id for event in events}
    )
    assert committed == dict.fromkeys(accepted_ids, 1)
