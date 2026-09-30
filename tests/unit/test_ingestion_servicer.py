"""The gRPC layer end to end (real in-process server + interceptors, fake Kafka/Redis)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import timedelta

import grpc
import pytest

from ingestion.api.grpc_server import HEALTH_CHECK_METHOD, build_grpc_server
from ingestion.api.ingestion_servicer import IngestionServicer
from ingestion.api.interceptors import (
    ApiKeyAuthenticator,
    AuthInterceptor,
    RequestLoggingInterceptor,
    TenantContext,
)
from ingestion.batching.batch_accumulator import EventBatchAccumulator
from ingestion.config import GrpcSettings
from ingestion.domain.models import BatchResult, PendingEvent
from ingestion.domain.routing import TopicRouter
from ingestion.domain.validation import EventValidator
from ingestion.generated.events.v1 import (
    events_pb2,
    ingestion_service_pb2,
    ingestion_service_pb2_grpc,
)
from ingestion.kafka.serializers import EnvelopeSerializer
from ingestion.observability.logging import get_logger
from ingestion.observability.metrics import IngestionMetrics
from tests.support import (
    FakeDeadLetterSink,
    FakeDeduplicator,
    RecordingBatchPublisher,
    make_envelope,
)

API_KEY = "key-for-tenant-9"
AUTH_METADATA = (("x-api-key", API_KEY),)
Status = ingestion_service_pb2


class NeverAcksPublisher(RecordingBatchPublisher):
    """Leaves futures unresolved to simulate a stuck Kafka commit."""

    async def publish_batch(self, batch: Sequence[PendingEvent]) -> BatchResult:
        self.batches.append(list(batch))
        return BatchResult(batch_size=len(batch))


@dataclass
class ServiceHarness:
    stub: ingestion_service_pb2_grpc.EventIngestionServiceAsyncStub
    publisher: RecordingBatchPublisher
    deduplicator: FakeDeduplicator
    dead_letter_sink: FakeDeadLetterSink
    accumulator: EventBatchAccumulator


@pytest.fixture
def publisher() -> RecordingBatchPublisher:
    return RecordingBatchPublisher()


@pytest.fixture
def max_queue_size() -> int:
    return 1_000


@pytest.fixture
async def service(
    publisher: RecordingBatchPublisher, max_queue_size: int
) -> AsyncIterator[ServiceHarness]:
    metrics = IngestionMetrics()
    router = TopicRouter()
    deduplicator = FakeDeduplicator()
    dead_letter_sink = FakeDeadLetterSink()
    accumulator = EventBatchAccumulator(
        batch_publisher=publisher,
        max_queue_size=max_queue_size,
        batch_max_records=100,
        batch_max_bytes=1_000_000,
        batch_flush_interval_ms=5,
        max_in_flight_batches=2,
        metrics=metrics,
        logger=get_logger("test"),
    )
    tenant_context = TenantContext()
    servicer = IngestionServicer(
        validator=EventValidator(router, max_clock_skew=timedelta(minutes=5)),
        router=router,
        serializer=EnvelopeSerializer("test"),
        deduplicator=deduplicator,
        accumulator=accumulator,
        dead_letter_sink=dead_letter_sink,
        tenant_resolver=tenant_context.current,
        metrics=metrics,
        logger=get_logger("test"),
        instance_id="test-0",
        request_timeout_seconds=0.5,
        max_events_per_request=10,
    )
    server, port = build_grpc_server(
        servicer,
        [
            RequestLoggingInterceptor(metrics, get_logger("test")),
            AuthInterceptor(
                ApiKeyAuthenticator({API_KEY: "tenant_9"}),
                tenant_context,
                public_methods=frozenset({HEALTH_CHECK_METHOD}),
            ),
        ],
        GrpcSettings(host="127.0.0.1", port=0),
    )
    publisher.deduplicator = deduplicator
    accumulator.start()
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    yield ServiceHarness(
        ingestion_service_pb2_grpc.EventIngestionServiceStub(channel),
        publisher,
        deduplicator,
        dead_letter_sink,
        accumulator,
    )
    await channel.close(grace=None)
    await server.stop(0)
    await accumulator.stop()


async def publish(
    service: ServiceHarness, *events: events_pb2.EventEnvelope
) -> list[ingestion_service_pb2.EventResult]:
    response = await service.stub.PublishEvents(
        ingestion_service_pb2.PublishEventsRequest(request_id="req-1", events=events),
        metadata=AUTH_METADATA,
    )
    assert response.request_id == "req-1"
    return list(response.results)


async def test_accepts_and_overwrites_tenant_and_received_time(service: ServiceHarness) -> None:
    envelope = make_envelope("order_placed", tenant_id="spoofed-tenant")
    [result] = await publish(service, envelope)
    assert result.status == Status.ACCEPTED
    assert result.event_id == envelope.event_id
    published = service.publisher.batches[0][0]
    assert published.tenant_id == "tenant_9"
    assert published.envelope.HasField("received_time")
    assert published.record.key == b"tenant_9:o1"
    headers = dict(published.record.headers)
    assert headers["tenant_id"] == b"tenant_9"
    assert headers["event_id"] == envelope.event_id.encode()


async def test_invalid_event_rejected_and_dead_lettered(service: ServiceHarness) -> None:
    invalid = make_envelope("rating_submitted")
    invalid.rating_submitted.overall_rating = 7
    valid = make_envelope("item_viewed")
    results = await publish(service, invalid, valid)
    assert [result.status for result in results] == [Status.REJECTED_INVALID, Status.ACCEPTED]
    assert "overall_rating" in results[0].error_message
    [dead_letter] = service.dead_letter_sink.dead_letters
    assert dead_letter.event_id == invalid.event_id
    assert dead_letter.tenant_id == "tenant_9"
    assert dead_letter.original_topic == "food.feedback.v1"
    assert dead_letter.failed_stage.value == "VALIDATION"


async def test_resend_after_commit_is_duplicate(service: ServiceHarness) -> None:
    envelope = make_envelope()
    assert (await publish(service, envelope))[0].status == Status.ACCEPTED
    [result] = await publish(service, envelope)
    assert result.status == Status.DUPLICATE
    assert sum(len(batch) for batch in service.publisher.batches) == 1


async def test_repeat_within_one_request_is_published_once(service: ServiceHarness) -> None:
    envelope = make_envelope()
    results = await publish(service, envelope, envelope)
    assert [result.status for result in results] == [Status.ACCEPTED, Status.DUPLICATE]
    assert sum(len(batch) for batch in service.publisher.batches) == 1


async def test_missing_or_bad_api_key_is_unauthenticated(service: ServiceHarness) -> None:
    request = ingestion_service_pb2.PublishEventsRequest(events=[make_envelope()])
    for metadata in ((), (("x-api-key", "wrong"),)):
        with pytest.raises(grpc.aio.AioRpcError) as error_info:
            await service.stub.PublishEvents(request, metadata=metadata)
        assert error_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert service.publisher.batches == []


async def test_bearer_authorization_header_is_accepted(service: ServiceHarness) -> None:
    response = await service.stub.PublishEvents(
        ingestion_service_pb2.PublishEventsRequest(events=[make_envelope()]),
        metadata=(("authorization", f"Bearer {API_KEY}"),),
    )
    assert response.results[0].status == Status.ACCEPTED


async def test_health_check_is_public(service: ServiceHarness) -> None:
    response = await service.stub.HealthCheck(ingestion_service_pb2.HealthCheckRequest())
    assert response.status == ingestion_service_pb2.HealthCheckResponse.SERVING
    assert response.instance_id == "test-0"


async def test_too_many_events_is_invalid_argument(service: ServiceHarness) -> None:
    with pytest.raises(grpc.aio.AioRpcError) as error_info:
        await publish(service, *(make_envelope() for _ in range(11)))
    assert error_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.parametrize("max_queue_size", [1])
async def test_full_queue_is_resource_exhausted_and_releases_markers(
    service: ServiceHarness,
) -> None:
    # Two events can never fit into a queue of one: the request is rejected as a whole.
    events = [make_envelope(), make_envelope()]
    with pytest.raises(grpc.aio.AioRpcError) as error_info:
        await publish(service, *events)
    assert error_info.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert service.deduplicator.markers == {}  # client may retry the same event ids


async def test_dedupe_store_outage_fails_events_safely(service: ServiceHarness) -> None:
    service.deduplicator.fail_reserve = True
    [result] = await publish(service, make_envelope())
    assert result.status == Status.FAILED
    assert "deduplication store unavailable" in result.error_message
    assert service.publisher.batches == []


@pytest.mark.parametrize("publisher", [NeverAcksPublisher()])
async def test_ack_timeout_reports_failed_without_cancelling(service: ServiceHarness) -> None:
    envelope = make_envelope()
    [result] = await publish(service, envelope)
    assert result.status == Status.FAILED
    assert "timed out waiting for the Kafka commit" in result.error_message
    pending = service.publisher.batches[0][0]
    assert not pending.ack_future.cancelled()  # publisher can still resolve it later
    await asyncio.sleep(0)
