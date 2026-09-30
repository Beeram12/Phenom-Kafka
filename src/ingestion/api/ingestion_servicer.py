"""gRPC handler: stamp -> validate -> dedupe -> enqueue -> await commit acks -> respond."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import grpc
import structlog

from ingestion.batching.batch_accumulator import (
    AccumulatorClosedError,
    EventBatchAccumulator,
    QueueFullError,
)
from ingestion.domain.models import DedupeKey, EventOutcome, EventStatus, FailedStage, PendingEvent
from ingestion.domain.protocols import (
    DeadLetter,
    DeadLetterSink,
    Deduplicator,
    ReservationStatus,
    Validator,
)
from ingestion.domain.routing import TopicRouter
from ingestion.generated.events.v1 import (
    events_pb2,
    ingestion_service_pb2,
    ingestion_service_pb2_grpc,
)
from ingestion.kafka.serializers import EnvelopeSerializer, SerializationError
from ingestion.observability.metrics import IngestionMetrics

ServicerContext = grpc.aio.ServicerContext[Any, Any]

_STATUS_TO_PROTO = {
    EventStatus.ACCEPTED: ingestion_service_pb2.ACCEPTED,
    EventStatus.DUPLICATE: ingestion_service_pb2.DUPLICATE,
    EventStatus.REJECTED_INVALID: ingestion_service_pb2.REJECTED_INVALID,
    EventStatus.SENT_TO_DLQ: ingestion_service_pb2.SENT_TO_DLQ,
    EventStatus.FAILED: ingestion_service_pb2.FAILED,
}


@dataclass(slots=True)
class _RequestState:
    """Per-request bookkeeping: outcome slots plus the events still in progress."""

    envelopes: list[events_pb2.EventEnvelope]
    outcomes: list[EventOutcome | None]
    dead_letters: list[DeadLetter] = field(default_factory=list)
    publishable_indexes: list[int] = field(default_factory=list)
    repeat_of_index: dict[int, int] = field(default_factory=dict)
    pending_by_index: dict[int, PendingEvent] = field(default_factory=dict)


class IngestionServicer(ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    """Implements EventIngestionService. Holds no business logic of its own: it orchestrates
    the validator, deduplicator, accumulator and DLQ sink, then maps outcomes to protos.

    An ACCEPTED result is only returned after the event's Kafka transaction COMMITTED.
    """

    def __init__(
        self,
        *,
        validator: Validator,
        router: TopicRouter,
        serializer: EnvelopeSerializer,
        deduplicator: Deduplicator,
        accumulator: EventBatchAccumulator,
        dead_letter_sink: DeadLetterSink,
        tenant_resolver: Callable[[], str],
        metrics: IngestionMetrics,
        logger: structlog.stdlib.BoundLogger,
        instance_id: str,
        request_timeout_seconds: float,
        max_events_per_request: int,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._validator = validator
        self._router = router
        self._serializer = serializer
        self._deduplicator = deduplicator
        self._accumulator = accumulator
        self._dead_letter_sink = dead_letter_sink
        self._tenant_resolver = tenant_resolver
        self._metrics = metrics
        self._logger = logger
        self._instance_id = instance_id
        self._request_timeout_seconds = request_timeout_seconds
        self._max_events_per_request = max_events_per_request
        self._clock = clock
        self._background_dead_letter_writes: set[asyncio.Task[None]] = set()

    async def drain_background_work(self) -> None:
        """Wait for DLQ writes of rejected events that outlived their request (shutdown)."""
        if self._background_dead_letter_writes:
            await asyncio.gather(*self._background_dead_letter_writes, return_exceptions=True)

    async def PublishEvents(
        self,
        request: ingestion_service_pb2.PublishEventsRequest,
        context: ServicerContext,
    ) -> ingestion_service_pb2.PublishEventsResponse:
        """Publish a batch of events and return one EventResult per event, in order."""
        if len(request.events) > self._max_events_per_request:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"at most {self._max_events_per_request} events per request",
            )
        tenant_id = self._tenant_resolver()
        state = _RequestState(envelopes=list(request.events), outcomes=[None] * len(request.events))

        self._stamp_and_validate(state, tenant_id)
        await self._reserve_dedupe_keys(state)
        await self._build_pending_events(state)
        await self._enqueue(state, context)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._effective_timeout(context.time_remaining())
        dead_letter_write = self._start_dead_letter_write(state)
        await self._await_acks(state, deadline - loop.time())
        if dead_letter_write is not None:
            # Rejected events are REJECTED_INVALID regardless; the DLQ write is bounded by the
            # request deadline and otherwise completes in the background (drained on shutdown).
            await asyncio.wait([dead_letter_write], timeout=max(deadline - loop.time(), 0.0))
        self._resolve_in_request_repeats(state)
        return self._build_response(request.request_id, state)

    async def HealthCheck(
        self,
        request: ingestion_service_pb2.HealthCheckRequest,
        context: ServicerContext,
    ) -> ingestion_service_pb2.HealthCheckResponse:
        """SERVING while the accumulator accepts events, NOT_SERVING during shutdown."""
        serving_status = (
            ingestion_service_pb2.HealthCheckResponse.SERVING
            if self._accumulator.is_accepting
            else ingestion_service_pb2.HealthCheckResponse.NOT_SERVING
        )
        return ingestion_service_pb2.HealthCheckResponse(
            status=serving_status, instance_id=self._instance_id
        )

    def _stamp_and_validate(self, state: _RequestState, tenant_id: str) -> None:
        """Overwrite tenant/received_time (never trust the client) and validate each event."""
        received_time = self._clock()
        first_index_by_event_id: dict[str, int] = {}
        for index, envelope in enumerate(state.envelopes):
            envelope.tenant_id = tenant_id
            envelope.received_time.FromDatetime(received_time)
            validation = self._validator.validate(envelope)
            if not validation.is_valid:
                state.outcomes[index] = EventOutcome(
                    EventStatus.REJECTED_INVALID, validation.error_message
                )
                state.dead_letters.append(
                    self._dead_letter_for(
                        envelope,
                        "ValidationError",
                        validation.error_message,
                        FailedStage.VALIDATION,
                    )
                )
                continue
            first_index = first_index_by_event_id.setdefault(envelope.event_id, index)
            if first_index != index:
                state.repeat_of_index[index] = first_index
            else:
                state.publishable_indexes.append(index)

    async def _reserve_dedupe_keys(self, state: _RequestState) -> None:
        keys = [
            DedupeKey(state.envelopes[index].tenant_id, state.envelopes[index].event_id)
            for index in state.publishable_indexes
        ]
        try:
            reservations = await self._deduplicator.reserve(keys)
        except Exception as error:
            self._metrics.dedupe_errors_total.labels(operation="reserve").inc()
            self._logger.error(
                "dedupe_reserve_failed", error_class=type(error).__name__, error=str(error)
            )
            for index in state.publishable_indexes:
                state.outcomes[index] = EventOutcome(
                    EventStatus.FAILED, "deduplication store unavailable; retry later"
                )
            state.publishable_indexes = []
            return

        reserved_indexes: list[int] = []
        for index, reservation in zip(state.publishable_indexes, reservations, strict=True):
            if reservation is ReservationStatus.RESERVED:
                reserved_indexes.append(index)
            elif reservation is ReservationStatus.DUPLICATE:
                state.outcomes[index] = EventOutcome(
                    EventStatus.DUPLICATE, "event_id was already accepted"
                )
            else:
                state.outcomes[index] = EventOutcome(
                    EventStatus.FAILED, "the same event_id is being published by another request"
                )
        state.publishable_indexes = reserved_indexes

    async def _build_pending_events(self, state: _RequestState) -> None:
        loop = asyncio.get_running_loop()
        serialization_failures: list[int] = []
        for index in state.publishable_indexes:
            envelope = state.envelopes[index]
            try:
                record = self._serializer.build_record(envelope, self._router.resolve(envelope))
            except SerializationError as error:
                serialization_failures.append(index)
                state.dead_letters.append(
                    self._dead_letter_for(
                        envelope, type(error).__name__, str(error), FailedStage.SERIALIZATION
                    )
                )
                state.outcomes[index] = EventOutcome(EventStatus.SENT_TO_DLQ, str(error))
                continue
            state.pending_by_index[index] = PendingEvent(
                envelope=envelope, record=record, ack_future=loop.create_future()
            )
        if serialization_failures:
            await self._release_now(
                [
                    DedupeKey(state.envelopes[index].tenant_id, state.envelopes[index].event_id)
                    for index in serialization_failures
                ]
            )

    async def _enqueue(self, state: _RequestState, context: ServicerContext) -> None:
        if not state.pending_by_index:
            return
        try:
            self._accumulator.submit_all(list(state.pending_by_index.values()))
        except (QueueFullError, AccumulatorClosedError) as error:
            await self._release_now(
                [pending.dedupe_key for pending in state.pending_by_index.values()]
            )
            status_code = (
                grpc.StatusCode.RESOURCE_EXHAUSTED
                if isinstance(error, QueueFullError)
                else grpc.StatusCode.UNAVAILABLE
            )
            await context.abort(status_code, f"{error}; retry with backoff")

    def _effective_timeout(self, client_time_remaining: float | None) -> float:
        timeout_seconds = self._request_timeout_seconds
        if client_time_remaining is not None:
            timeout_seconds = min(timeout_seconds, max(client_time_remaining - 0.05, 0.0))
        return timeout_seconds

    def _start_dead_letter_write(self, state: _RequestState) -> asyncio.Task[None] | None:
        if not state.dead_letters:
            return None
        write_task = asyncio.create_task(self._write_dead_letters(state.dead_letters))
        self._background_dead_letter_writes.add(write_task)
        write_task.add_done_callback(self._background_dead_letter_writes.discard)
        return write_task

    async def _await_acks(self, state: _RequestState, timeout_seconds: float) -> None:
        if not state.pending_by_index:
            return
        timeout_seconds = max(timeout_seconds, 0.0)
        futures = [pending.ack_future for pending in state.pending_by_index.values()]
        # asyncio.wait never cancels the futures: the publisher still resolves them later.
        await asyncio.wait(futures, timeout=timeout_seconds)
        for index, pending in state.pending_by_index.items():
            if pending.ack_future.done():
                state.outcomes[index] = pending.ack_future.result()
            else:
                state.outcomes[index] = EventOutcome(
                    EventStatus.FAILED,
                    "timed out waiting for the Kafka commit; outcome pending, safe to resend "
                    "(a committed resend returns DUPLICATE)",
                )

    async def _write_dead_letters(self, dead_letters: list[DeadLetter]) -> None:
        written_flags = await self._dead_letter_sink.send(dead_letters)
        for dead_letter, written in zip(dead_letters, written_flags, strict=True):
            if not written:
                # Outcome for the client stays REJECTED_INVALID; the loss is logged + counted
                # by the DLQ publisher.
                self._logger.error(
                    "invalid_event_not_dead_lettered",
                    event_id=dead_letter.event_id,
                    tenant_id=dead_letter.tenant_id,
                )

    def _resolve_in_request_repeats(self, state: _RequestState) -> None:
        """An event_id repeated inside one request mirrors the first occurrence's outcome."""
        for index, first_index in state.repeat_of_index.items():
            first_outcome = state.outcomes[first_index]
            if first_outcome is None or first_outcome.status is EventStatus.ACCEPTED:
                state.outcomes[index] = EventOutcome(
                    EventStatus.DUPLICATE, "event_id repeated within the same request"
                )
            else:
                state.outcomes[index] = first_outcome

    def _build_response(
        self, request_id: str, state: _RequestState
    ) -> ingestion_service_pb2.PublishEventsResponse:
        response = ingestion_service_pb2.PublishEventsResponse(request_id=request_id)
        for envelope, outcome in zip(state.envelopes, state.outcomes, strict=True):
            final_outcome = outcome or EventOutcome(EventStatus.FAILED, "no outcome recorded")
            self._metrics.events_total.labels(status=final_outcome.status.value).inc()
            response.results.append(
                ingestion_service_pb2.EventResult(
                    event_id=envelope.event_id,
                    status=_STATUS_TO_PROTO[final_outcome.status],
                    error_message=final_outcome.error_message,
                )
            )
        return response

    def _dead_letter_for(
        self,
        envelope: events_pb2.EventEnvelope,
        error_class: str,
        error_message: str,
        stage: FailedStage,
    ) -> DeadLetter:
        now = self._clock()
        return DeadLetter(
            original_topic=self._router.topic_for(envelope.event_type) or "",
            original_payload=envelope.SerializeToString(),
            event_id=envelope.event_id,
            tenant_id=envelope.tenant_id,
            event_type=envelope.event_type,
            error_class=error_class,
            error_message=error_message,
            failed_stage=stage,
            attempt_count=1,
            first_failed_at=now,
            last_failed_at=now,
        )

    async def _release_now(self, keys: list[DedupeKey]) -> None:
        try:
            await self._deduplicator.release(keys)
        except Exception as error:
            self._metrics.dedupe_errors_total.labels(operation="release").inc()
            self._logger.error(
                "dedupe_release_failed", error_class=type(error).__name__, error=str(error)
            )
