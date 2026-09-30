"""Structural interfaces for the pipeline's collaborators, so each can be faked in tests."""

from __future__ import annotations

import enum
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from ingestion.domain.models import BatchResult, DedupeKey, FailedStage, PendingEvent
from ingestion.domain.validation import ValidationResult
from ingestion.generated.events.v1 import events_pb2


class Validator(Protocol):
    """Validates a single envelope."""

    def validate(self, envelope: events_pb2.EventEnvelope) -> ValidationResult:
        """Return the validation outcome for ``envelope``."""
        ...


class ReservationStatus(enum.Enum):
    """Result of trying to reserve an event id for publishing."""

    RESERVED = "RESERVED"  # caller now owns the pending marker and must commit or release it
    DUPLICATE = "DUPLICATE"  # already committed earlier
    IN_FLIGHT = "IN_FLIGHT"  # another request currently owns the pending marker


class Deduplicator(Protocol):
    """Two-phase event-id deduplication: reserve -> (mark_committed | release)."""

    async def reserve(self, keys: Sequence[DedupeKey]) -> list[ReservationStatus]:
        """Try to place a short-lived pending marker for each key."""
        ...

    async def mark_committed(self, keys: Sequence[DedupeKey]) -> None:
        """Promote pending markers to long-lived committed markers."""
        ...

    async def release(self, keys: Sequence[DedupeKey]) -> None:
        """Drop pending markers so the client may retry these events."""
        ...


class BatchPublisher(Protocol):
    """Publishes one batch and resolves every event's ack future."""

    async def publish_batch(self, batch: Sequence[PendingEvent]) -> BatchResult:
        """Publish ``batch``; must resolve every future before returning."""
        ...


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Everything needed to build one DLQ record."""

    original_topic: str
    original_payload: bytes
    event_id: str
    tenant_id: str
    event_type: str
    error_class: str
    error_message: str
    failed_stage: FailedStage
    attempt_count: int
    first_failed_at: datetime
    last_failed_at: datetime


class DeadLetterSink(Protocol):
    """Writes dead letters; returns per-entry success so nothing is dropped silently."""

    async def send(self, dead_letters: Sequence[DeadLetter]) -> list[bool]:
        """Publish ``dead_letters`` and report which were durably written."""
        ...
