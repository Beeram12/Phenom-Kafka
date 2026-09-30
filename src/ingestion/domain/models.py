"""Internal domain types shared across the ingestion pipeline."""

from __future__ import annotations

import asyncio
import enum
import time
from dataclasses import dataclass, field

from ingestion.generated.events.v1 import events_pb2

KafkaHeaders = list[tuple[str, bytes]]


class EventStatus(enum.Enum):
    """Final per-event outcome reported back to the client."""

    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REJECTED_INVALID = "REJECTED_INVALID"
    SENT_TO_DLQ = "SENT_TO_DLQ"
    FAILED = "FAILED"


class FailedStage(enum.Enum):
    """Pipeline stage at which an event was dead-lettered."""

    VALIDATION = "VALIDATION"
    SERIALIZATION = "SERIALIZATION"
    PUBLISH = "PUBLISH"


@dataclass(frozen=True, slots=True)
class EventOutcome:
    """Resolution value of a PendingEvent's ack future."""

    status: EventStatus
    error_message: str = ""


@dataclass(frozen=True, slots=True)
class KafkaRecord:
    """A fully prepared Kafka record: destination, key, serialized value and headers."""

    topic: str
    key: bytes
    value: bytes
    headers: KafkaHeaders

    @property
    def size_bytes(self) -> int:
        """Approximate on-the-wire size used for batch byte accounting."""
        header_bytes = sum(len(name) + len(value) for name, value in self.headers)
        return len(self.key) + len(self.value) + header_bytes


@dataclass(frozen=True, slots=True)
class DedupeKey:
    """Identity of an event for deduplication purposes."""

    tenant_id: str
    event_id: str


@dataclass(slots=True)
class PendingEvent:
    """A validated, deduplicated event waiting to be committed to Kafka.

    ``ack_future`` is awaited by the gRPC handler and resolved exactly once by the
    publisher with the final EventOutcome.
    """

    envelope: events_pb2.EventEnvelope
    record: KafkaRecord
    ack_future: asyncio.Future[EventOutcome]
    enqueued_at_monotonic: float = field(default_factory=time.monotonic)

    @property
    def event_id(self) -> str:
        """The client-supplied event id."""
        return self.envelope.event_id

    @property
    def tenant_id(self) -> str:
        """The authenticated tenant id."""
        return self.envelope.tenant_id

    @property
    def dedupe_key(self) -> DedupeKey:
        """Key under which this event's dedupe marker is stored."""
        return DedupeKey(tenant_id=self.tenant_id, event_id=self.event_id)

    @property
    def size_bytes(self) -> int:
        """Size of the Kafka record this event produces."""
        return self.record.size_bytes

    def resolve(self, outcome: EventOutcome) -> None:
        """Resolve the ack future once; later resolutions are ignored."""
        if not self.ack_future.done():
            self.ack_future.set_result(outcome)


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Summary of what happened to one flushed batch (used for logging and metrics)."""

    batch_size: int
    committed_count: int = 0
    dead_lettered_count: int = 0
    failed_count: int = 0
    transaction_attempts: int = 0

    def merge(self, other: BatchResult) -> BatchResult:
        """Combine results of two sub-batches produced by bisection."""
        return BatchResult(
            batch_size=self.batch_size + other.batch_size,
            committed_count=self.committed_count + other.committed_count,
            dead_lettered_count=self.dead_lettered_count + other.dead_lettered_count,
            failed_count=self.failed_count + other.failed_count,
            transaction_attempts=self.transaction_attempts + other.transaction_attempts,
        )
