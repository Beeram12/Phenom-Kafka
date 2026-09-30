"""Builders and in-memory fakes shared by unit and integration tests."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ingestion.domain.models import (
    BatchResult,
    DedupeKey,
    EventOutcome,
    EventStatus,
    KafkaRecord,
    PendingEvent,
)
from ingestion.domain.protocols import DeadLetter, ReservationStatus
from ingestion.domain.routing import TopicRouter
from ingestion.generated.events.v1 import events_pb2
from ingestion.kafka.serializers import EnvelopeSerializer

TENANT_ID = "tenant_1"


def make_envelope(
    event_type: str = "session_started", **overrides: Any
) -> events_pb2.EventEnvelope:
    """A VALID envelope of ``event_type``; ``overrides`` replace envelope fields."""
    payloads: dict[str, dict[str, Any]] = {
        "session_started": {"session_started": events_pb2.SessionStarted(entry_point="home")},
        "item_viewed": {
            "item_viewed": events_pb2.ItemViewed(item_id="i1", restaurant_id="r1", unit_price=120)
        },
        "price_filter_applied": {
            "price_filter_applied": events_pb2.PriceFilterApplied(
                min_price=100, max_price=300, category="dosa"
            )
        },
        "cart_item_added": {
            "cart_item_added": events_pb2.CartItemAdded(
                item_id="i1", unit_price=120, quantity=2, restaurant_id="r1"
            )
        },
        "order_placed": {
            "order_placed": events_pb2.OrderPlaced(
                order_id="o1",
                restaurant_id="r1",
                items=[
                    events_pb2.OrderItem(
                        item_id="i1",
                        name="Masala Dosa",
                        category="dosa",
                        unit_price=120,
                        quantity=2,
                    )
                ],
                subtotal=240,
                discount=0,
                total=240,
                currency="INR",
                order_type="delivery",
            )
        },
        "rating_prompt_shown": {"rating_prompt_shown": events_pb2.RatingPromptShown(order_id="o1")},
        "rating_submitted": {
            "rating_submitted": events_pb2.RatingSubmitted(
                order_id="o1", restaurant_id="r1", overall_rating=5
            )
        },
    }
    fields: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": 1,
        "tenant_id": TENANT_ID,
        "source": events_pb2.MOBILE,
        "user_id": "user_1",
        "anonymous_id": "device_1",
        "session_id": "sess_1",
        **payloads[event_type],
    }
    event_time = overrides.pop("event_time", datetime.now(UTC))
    fields.update(overrides)
    envelope = events_pb2.EventEnvelope(**fields)
    if event_time is not None:
        envelope.event_time.FromDatetime(event_time)
    return envelope


def make_pending_event(
    envelope: events_pb2.EventEnvelope | None = None, value: bytes | None = None
) -> PendingEvent:
    """A PendingEvent with a real routed record (or a custom ``value``)."""
    envelope = envelope or make_envelope()
    record = EnvelopeSerializer("test-producer").build_record(
        envelope, TopicRouter().resolve(envelope)
    )
    if value is not None:
        record = KafkaRecord(record.topic, record.key, value, record.headers)
    return PendingEvent(
        envelope=envelope, record=record, ack_future=asyncio.get_running_loop().create_future()
    )


@dataclass
class FakeDeduplicator:
    """In-memory two-phase deduplicator."""

    markers: dict[DedupeKey, str] = field(default_factory=dict)
    committed_calls: list[list[DedupeKey]] = field(default_factory=list)
    released_calls: list[list[DedupeKey]] = field(default_factory=list)
    fail_reserve: bool = False

    async def reserve(self, keys: Sequence[DedupeKey]) -> list[ReservationStatus]:
        if self.fail_reserve:
            raise ConnectionError("redis down")
        statuses = []
        for key in keys:
            marker = self.markers.get(key)
            if marker is None:
                self.markers[key] = "pending"
                statuses.append(ReservationStatus.RESERVED)
            elif marker == "committed":
                statuses.append(ReservationStatus.DUPLICATE)
            else:
                statuses.append(ReservationStatus.IN_FLIGHT)
        return statuses

    async def mark_committed(self, keys: Sequence[DedupeKey]) -> None:
        self.committed_calls.append(list(keys))
        for key in keys:
            self.markers[key] = "committed"

    async def release(self, keys: Sequence[DedupeKey]) -> None:
        self.released_calls.append(list(keys))
        for key in keys:
            if self.markers.get(key) == "pending":
                del self.markers[key]


@dataclass
class FakeDeadLetterSink:
    """Records dead letters; ``succeed`` controls the reported write result."""

    dead_letters: list[DeadLetter] = field(default_factory=list)
    succeed: bool = True

    async def send(self, dead_letters: Sequence[DeadLetter]) -> list[bool]:
        self.dead_letters.extend(dead_letters)
        return [self.succeed] * len(dead_letters)


@dataclass
class RecordingBatchPublisher:
    """Accepts every batch, marks dedupe keys committed (like the real publisher)."""

    batches: list[list[PendingEvent]] = field(default_factory=list)
    status: EventStatus = EventStatus.ACCEPTED
    deduplicator: FakeDeduplicator | None = None

    async def publish_batch(self, batch: Sequence[PendingEvent]) -> BatchResult:
        self.batches.append(list(batch))
        if self.deduplicator is not None:
            await self.deduplicator.mark_committed([event.dedupe_key for event in batch])
        for pending_event in batch:
            pending_event.resolve(EventOutcome(self.status))
        return BatchResult(batch_size=len(batch), committed_count=len(batch))


class FakeTransactionalProducer:
    """Mimics the aiokafka transactional API.

    ``commit_errors`` are raised by successive commit calls; records whose value is in
    ``poison_values`` fail delivery with ``poison_error``. Only committed records land in
    ``committed_values``.
    """

    def __init__(
        self,
        commit_errors: list[BaseException] | None = None,
        poison_values: frozenset[bytes] = frozenset(),
        poison_error: BaseException | None = None,
        commit_delay_seconds: float = 0.0,
    ) -> None:
        self.commit_errors = commit_errors if commit_errors is not None else []
        self.poison_values = poison_values
        self.poison_error = poison_error
        self.commit_delay_seconds = commit_delay_seconds
        self.open_transaction: list[bytes] = []
        self.committed_values: list[bytes] = []
        self.aborted_count = 0
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def begin_transaction(self) -> None:
        self.open_transaction = []

    async def send(
        self, topic: str, value: bytes, key: bytes, headers: list[tuple[str, bytes]]
    ) -> asyncio.Future[None]:
        delivery: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if value in self.poison_values and self.poison_error is not None:
            delivery.set_exception(self.poison_error)
        else:
            delivery.set_result(None)
        self.open_transaction.append(value)
        return delivery

    async def commit_transaction(self) -> None:
        if self.commit_delay_seconds:
            await asyncio.sleep(self.commit_delay_seconds)
        if self.commit_errors:
            raise self.commit_errors.pop(0)
        self.committed_values.extend(self.open_transaction)
        self.open_transaction = []

    async def abort_transaction(self) -> None:
        self.aborted_count += 1
        self.open_transaction = []


@dataclass
class FakeProducerFactory:
    """Hands out FakeTransactionalProducers and records every creation."""

    producer_template: dict[str, Any] = field(default_factory=dict)
    shared_commit_errors: list[BaseException] = field(default_factory=list)
    created: list[tuple[str, FakeTransactionalProducer]] = field(default_factory=list)

    def create_transactional(self, transactional_id: str) -> FakeTransactionalProducer:
        producer = FakeTransactionalProducer(
            commit_errors=self.shared_commit_errors, **self.producer_template
        )
        self.created.append((transactional_id, producer))
        return producer

    @property
    def all_committed_values(self) -> list[bytes]:
        return [value for _, producer in self.created for value in producer.committed_values]
