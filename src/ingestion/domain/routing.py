"""The single source of truth for topics, event_type -> topic routing and partition keys.

Adding a new event type requires exactly two changes:
  1. a new payload message + ``oneof`` case in proto/events/v1/events.proto
  2. one entry in ``ROUTING_REGISTRY`` below.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from ingestion.generated.events.v1 import events_pb2


class RoutingError(ValueError):
    """Raised when an event cannot be routed (unknown type or missing key fields)."""


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """A Kafka topic and its partition count (consumed by scripts/create_topics.py)."""

    name: str
    partitions: int


ORDERS_TOPIC = TopicSpec("food.orders.v1", partitions=12)
FEEDBACK_TOPIC = TopicSpec("food.feedback.v1", partitions=6)
BROWSE_TOPIC = TopicSpec("food.browse.v1", partitions=24)
SESSIONS_TOPIC = TopicSpec("food.sessions.v1", partitions=12)
DLQ_TOPIC = TopicSpec("food.events.dlq", partitions=6)

EVENT_TOPICS: tuple[TopicSpec, ...] = (ORDERS_TOPIC, FEEDBACK_TOPIC, BROWSE_TOPIC, SESSIONS_TOPIC)
ALL_TOPICS: tuple[TopicSpec, ...] = (*EVENT_TOPICS, DLQ_TOPIC)

PartitionKeyFn = Callable[[events_pb2.EventEnvelope], str]


def _require(value: str, field_name: str) -> str:
    if not value:
        raise RoutingError(f"partition key field '{field_name}' is empty")
    return value


def _payload_order_id(envelope: events_pb2.EventEnvelope) -> str:
    payload_case = envelope.WhichOneof("payload")
    payload = getattr(envelope, payload_case) if payload_case else None
    order_id: str = getattr(payload, "order_id", "")
    return order_id


def order_scoped_key(envelope: events_pb2.EventEnvelope) -> str:
    """``{tenant_id}:{order_id}`` - keeps an order's lifecycle on one partition."""
    tenant_id = _require(envelope.tenant_id, "tenant_id")
    return f"{tenant_id}:{_require(_payload_order_id(envelope), 'order_id')}"


def session_scoped_key(envelope: events_pb2.EventEnvelope) -> str:
    """``{tenant_id}:{session_id}`` - keeps a browse session ordered."""
    tenant_id = _require(envelope.tenant_id, "tenant_id")
    return f"{tenant_id}:{_require(envelope.session_id, 'session_id')}"


def user_scoped_key(envelope: events_pb2.EventEnvelope) -> str:
    """``{tenant_id}:{user_id}`` - keeps a user's sessions ordered."""
    tenant_id = _require(envelope.tenant_id, "tenant_id")
    return f"{tenant_id}:{_require(envelope.user_id, 'user_id')}"


@dataclass(frozen=True, slots=True)
class EventRoute:
    """How one event_type is published: topic, partition key and expected payload case."""

    topic: str
    partition_key: PartitionKeyFn
    payload_field: str


# NOTE: never key by tenant_id alone - large tenants would create hot partitions.
ROUTING_REGISTRY: Mapping[str, EventRoute] = MappingProxyType(
    {
        "order_placed": EventRoute(ORDERS_TOPIC.name, order_scoped_key, "order_placed"),
        "rating_prompt_shown": EventRoute(
            FEEDBACK_TOPIC.name, order_scoped_key, "rating_prompt_shown"
        ),
        "rating_submitted": EventRoute(FEEDBACK_TOPIC.name, order_scoped_key, "rating_submitted"),
        "item_viewed": EventRoute(BROWSE_TOPIC.name, session_scoped_key, "item_viewed"),
        "price_filter_applied": EventRoute(
            BROWSE_TOPIC.name, session_scoped_key, "price_filter_applied"
        ),
        "cart_item_added": EventRoute(BROWSE_TOPIC.name, session_scoped_key, "cart_item_added"),
        "session_started": EventRoute(SESSIONS_TOPIC.name, user_scoped_key, "session_started"),
    }
)


@dataclass(frozen=True, slots=True)
class ResolvedRoute:
    """The concrete topic and partition key for one event."""

    topic: str
    partition_key: str


class TopicRouter:
    """Resolves an envelope to its destination topic and partition key."""

    def __init__(self, registry: Mapping[str, EventRoute] = ROUTING_REGISTRY) -> None:
        self._registry = registry

    @property
    def known_event_types(self) -> frozenset[str]:
        """All routable event types."""
        return frozenset(self._registry)

    def route_for(self, event_type: str) -> EventRoute:
        """Return the registry entry for ``event_type`` or raise RoutingError."""
        route = self._registry.get(event_type)
        if route is None:
            raise RoutingError(f"unknown event_type '{event_type}'")
        return route

    def topic_for(self, event_type: str) -> str | None:
        """Topic for ``event_type``, or None if it is not routable."""
        route = self._registry.get(event_type)
        return route.topic if route else None

    def resolve(self, envelope: events_pb2.EventEnvelope) -> ResolvedRoute:
        """Resolve topic + partition key, raising RoutingError when either is impossible."""
        route = self.route_for(envelope.event_type)
        return ResolvedRoute(topic=route.topic, partition_key=route.partition_key(envelope))
