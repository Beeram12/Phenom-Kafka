"""TopicRouter / ROUTING_REGISTRY: every event type maps to the right topic and key."""

from __future__ import annotations

import pytest

from ingestion.domain.routing import (
    ALL_TOPICS,
    ROUTING_REGISTRY,
    RoutingError,
    TopicRouter,
)
from ingestion.generated.events.v1 import events_pb2
from tests.support import TENANT_ID, make_envelope

EXPECTED_ROUTES = {
    "order_placed": ("food.orders.v1", f"{TENANT_ID}:o1"),
    "rating_prompt_shown": ("food.feedback.v1", f"{TENANT_ID}:o1"),
    "rating_submitted": ("food.feedback.v1", f"{TENANT_ID}:o1"),
    "item_viewed": ("food.browse.v1", f"{TENANT_ID}:sess_1"),
    "price_filter_applied": ("food.browse.v1", f"{TENANT_ID}:sess_1"),
    "cart_item_added": ("food.browse.v1", f"{TENANT_ID}:sess_1"),
    "session_started": ("food.sessions.v1", f"{TENANT_ID}:user_1"),
}


@pytest.mark.parametrize(("event_type", "expected"), EXPECTED_ROUTES.items())
def test_routes_each_event_type(event_type: str, expected: tuple[str, str]) -> None:
    resolved = TopicRouter().resolve(make_envelope(event_type))
    assert (resolved.topic, resolved.partition_key) == expected


def test_registry_covers_every_payload_case_exactly_once() -> None:
    payload_cases = {
        field.name for field in events_pb2.EventEnvelope.DESCRIPTOR.oneofs_by_name["payload"].fields
    }
    registered_cases = [route.payload_field for route in ROUTING_REGISTRY.values()]
    assert sorted(registered_cases) == sorted(payload_cases)
    assert set(ROUTING_REGISTRY) == set(EXPECTED_ROUTES)


def test_keys_are_never_tenant_only() -> None:
    for event_type in ROUTING_REGISTRY:
        key = TopicRouter().resolve(make_envelope(event_type)).partition_key
        tenant, _, entity = key.partition(":")
        assert tenant == TENANT_ID
        assert entity


def test_partition_counts_match_spec() -> None:
    assert {spec.name: spec.partitions for spec in ALL_TOPICS} == {
        "food.orders.v1": 12,
        "food.feedback.v1": 6,
        "food.browse.v1": 24,
        "food.sessions.v1": 12,
        "food.events.dlq": 6,
    }


def test_unknown_event_type_raises() -> None:
    envelope = make_envelope(event_type="session_started")
    envelope.event_type = "coupon_applied"
    with pytest.raises(RoutingError, match="unknown event_type"):
        TopicRouter().resolve(envelope)
    assert TopicRouter().topic_for("coupon_applied") is None


@pytest.mark.parametrize(
    ("event_type", "cleared_field"),
    [("item_viewed", "session_id"), ("session_started", "user_id"), ("order_placed", "tenant_id")],
)
def test_missing_key_field_raises(event_type: str, cleared_field: str) -> None:
    envelope = make_envelope(event_type)
    setattr(envelope, cleared_field, "")
    with pytest.raises(RoutingError, match="partition key"):
        TopicRouter().resolve(envelope)
