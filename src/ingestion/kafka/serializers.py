"""Protobuf <-> bytes conversion and Kafka header construction."""

from __future__ import annotations

from datetime import datetime

from google.protobuf.message import DecodeError, EncodeError
from google.protobuf.timestamp_pb2 import Timestamp

from ingestion.domain.models import FailedStage, KafkaHeaders, KafkaRecord
from ingestion.domain.protocols import DeadLetter
from ingestion.domain.routing import ResolvedRoute
from ingestion.generated.events.v1 import dlq_pb2, events_pb2

HEADER_EVENT_TYPE = "event_type"
HEADER_SCHEMA_VERSION = "schema_version"
HEADER_TENANT_ID = "tenant_id"
HEADER_EVENT_ID = "event_id"
HEADER_PRODUCER_ID = "producer_id"
HEADER_ERROR_CLASS = "error_class"
HEADER_FAILED_STAGE = "failed_stage"

_FAILED_STAGE_TO_PROTO = {
    FailedStage.VALIDATION: dlq_pb2.VALIDATION,
    FailedStage.SERIALIZATION: dlq_pb2.SERIALIZATION,
    FailedStage.PUBLISH: dlq_pb2.PUBLISH,
}


class SerializationError(Exception):
    """Raised when an event cannot be converted to or from bytes."""


def to_timestamp(moment: datetime) -> Timestamp:
    """Convert an aware datetime into a protobuf Timestamp."""
    timestamp = Timestamp()
    timestamp.FromDatetime(moment)
    return timestamp


class EnvelopeSerializer:
    """Builds Kafka records (value bytes + headers) for envelopes and dead letters."""

    def __init__(self, producer_id: str, max_dlq_payload_bytes: int = 900_000) -> None:
        self._producer_id = producer_id.encode()
        self._max_dlq_payload_bytes = max_dlq_payload_bytes

    def serialize(self, envelope: events_pb2.EventEnvelope) -> bytes:
        """Serialize an envelope to its wire format."""
        try:
            return envelope.SerializeToString()
        except EncodeError as error:
            raise SerializationError(f"cannot serialize envelope: {error}") from error

    @staticmethod
    def deserialize(value: bytes) -> events_pb2.EventEnvelope:
        """Parse an envelope from its wire format."""
        envelope = events_pb2.EventEnvelope()
        try:
            envelope.ParseFromString(value)
        except DecodeError as error:
            raise SerializationError(f"cannot parse envelope: {error}") from error
        return envelope

    def build_headers(self, envelope: events_pb2.EventEnvelope) -> KafkaHeaders:
        """Headers attached to every event record."""
        return [
            (HEADER_EVENT_TYPE, envelope.event_type.encode()),
            (HEADER_SCHEMA_VERSION, str(envelope.schema_version).encode()),
            (HEADER_TENANT_ID, envelope.tenant_id.encode()),
            (HEADER_EVENT_ID, envelope.event_id.encode()),
            (HEADER_PRODUCER_ID, self._producer_id),
        ]

    def build_record(self, envelope: events_pb2.EventEnvelope, route: ResolvedRoute) -> KafkaRecord:
        """Build the Kafka record for a routed envelope."""
        return KafkaRecord(
            topic=route.topic,
            key=route.partition_key.encode(),
            value=self.serialize(envelope),
            headers=self.build_headers(envelope),
        )

    def build_dead_letter_record(self, dlq_topic: str, dead_letter: DeadLetter) -> KafkaRecord:
        """Build the DLQ record; oversized payloads are omitted so the DLQ write can succeed."""
        original_payload = dead_letter.original_payload
        error_message = dead_letter.error_message
        if len(original_payload) > self._max_dlq_payload_bytes:
            error_message += f" [original_payload omitted: {len(original_payload)} bytes]"
            original_payload = b""
        dlq_record = dlq_pb2.DlqRecord(
            original_topic=dead_letter.original_topic,
            original_payload=original_payload,
            event_id=dead_letter.event_id,
            tenant_id=dead_letter.tenant_id,
            event_type=dead_letter.event_type,
            error_class=dead_letter.error_class,
            error_message=error_message[:4_000],
            failed_stage=_FAILED_STAGE_TO_PROTO[dead_letter.failed_stage],
            attempt_count=dead_letter.attempt_count,
            first_failed_at=to_timestamp(dead_letter.first_failed_at),
            last_failed_at=to_timestamp(dead_letter.last_failed_at),
        )
        return KafkaRecord(
            topic=dlq_topic,
            key=f"{dead_letter.tenant_id}:{dead_letter.event_id}".encode(),
            value=dlq_record.SerializeToString(),
            headers=[
                (HEADER_EVENT_TYPE, dead_letter.event_type.encode()),
                (HEADER_TENANT_ID, dead_letter.tenant_id.encode()),
                (HEADER_EVENT_ID, dead_letter.event_id.encode()),
                (HEADER_PRODUCER_ID, self._producer_id),
                (HEADER_ERROR_CLASS, dead_letter.error_class.encode()),
                (HEADER_FAILED_STAGE, dead_letter.failed_stage.value.encode()),
            ],
        )
