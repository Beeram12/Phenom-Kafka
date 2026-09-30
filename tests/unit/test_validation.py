"""EventValidator business rules."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ingestion.domain.routing import ROUTING_REGISTRY, TopicRouter
from ingestion.domain.validation import EventValidator
from ingestion.generated.events.v1 import events_pb2
from tests.support import make_envelope

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


@pytest.fixture
def validator() -> EventValidator:
    return EventValidator(TopicRouter(), max_clock_skew=timedelta(minutes=5), clock=lambda: NOW)


@pytest.mark.parametrize("event_type", sorted(ROUTING_REGISTRY))
def test_valid_events_pass(validator: EventValidator, event_type: str) -> None:
    result = validator.validate(make_envelope(event_type, event_time=NOW))
    assert result.is_valid, result.error_message


def test_rating_out_of_range(validator: EventValidator) -> None:
    envelope = make_envelope("rating_submitted", event_time=NOW)
    envelope.rating_submitted.overall_rating = 7
    result = validator.validate(envelope)
    assert not result.is_valid
    assert "overall_rating must be between 1 and 5 (got 7)" in result.error_message


def test_item_rating_out_of_range(validator: EventValidator) -> None:
    envelope = make_envelope("rating_submitted", event_time=NOW)
    envelope.rating_submitted.item_ratings.add(item_id="i1", rating=0)
    assert "item_ratings[0].rating" in validator.validate(envelope).error_message


@pytest.mark.parametrize(
    ("event_type", "mutate", "expected_reason"),
    [
        ("item_viewed", lambda e: setattr(e.item_viewed, "unit_price", -1.0), "unit_price"),
        ("cart_item_added", lambda e: setattr(e.cart_item_added, "unit_price", -5.0), "unit_price"),
        ("cart_item_added", lambda e: setattr(e.cart_item_added, "quantity", 0), "quantity"),
        (
            "order_placed",
            lambda e: setattr(e.order_placed.items[0], "unit_price", -1.0),
            "items[0]",
        ),
        ("order_placed", lambda e: setattr(e.order_placed, "total", -10.0), "total"),
        ("order_placed", lambda e: e.order_placed.ClearField("items"), "at least one item"),
        (
            "price_filter_applied",
            lambda e: setattr(e.price_filter_applied, "max_price", 50.0),
            "max_price must be >= min_price",
        ),
        ("item_viewed", lambda e: setattr(e.item_viewed, "unit_price", float("nan")), "finite"),
    ],
)
def test_prices_and_quantities(
    validator: EventValidator, event_type: str, mutate: object, expected_reason: str
) -> None:
    envelope = make_envelope(event_type, event_time=NOW)
    mutate(envelope)  # type: ignore[operator]
    result = validator.validate(envelope)
    assert not result.is_valid
    assert expected_reason in result.error_message


def test_event_time_within_skew_is_allowed(validator: EventValidator) -> None:
    envelope = make_envelope(event_time=NOW + timedelta(minutes=4, seconds=59))
    assert validator.validate(envelope).is_valid


def test_event_time_beyond_skew_is_rejected(validator: EventValidator) -> None:
    envelope = make_envelope(event_time=NOW + timedelta(minutes=5, seconds=1))
    assert "in the future" in validator.validate(envelope).error_message


def test_late_events_are_valid(validator: EventValidator) -> None:
    assert validator.validate(make_envelope(event_time=NOW - timedelta(days=3))).is_valid


@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"event_id": ""}, "event_id is required"),
        ({"event_id": "not-a-uuid"}, "event_id must be a UUID"),
        ({"tenant_id": ""}, "tenant_id is required"),
        ({"schema_version": 0}, "schema_version"),
        ({"source": events_pb2.EVENT_SOURCE_UNSPECIFIED}, "source is required"),
        ({"user_id": "", "anonymous_id": ""}, "user_id or anonymous_id"),
        ({"event_time": None}, "event_time is required"),
    ],
)
def test_required_envelope_fields(
    validator: EventValidator, overrides: dict[str, object], expected_reason: str
) -> None:
    overrides.setdefault("event_time", NOW)
    envelope = make_envelope("item_viewed", **overrides)
    assert expected_reason in validator.validate(envelope).error_message


def test_payload_must_match_event_type(validator: EventValidator) -> None:
    envelope = make_envelope("item_viewed", event_time=NOW)
    envelope.event_type = "order_placed"
    assert "does not match event_type" in validator.validate(envelope).error_message


def test_unknown_event_type(validator: EventValidator) -> None:
    envelope = make_envelope("item_viewed", event_time=NOW)
    envelope.event_type = "coupon_applied"
    assert "unknown event_type" in validator.validate(envelope).error_message


def test_missing_partition_key_field(validator: EventValidator) -> None:
    envelope = make_envelope("session_started", event_time=NOW, user_id="")
    assert "partition key field 'user_id'" in validator.validate(envelope).error_message


def test_collects_all_violations(validator: EventValidator) -> None:
    envelope = make_envelope("rating_submitted", event_time=NOW, event_id="bad", schema_version=0)
    envelope.rating_submitted.overall_rating = 9
    assert len(validator.validate(envelope).reasons) == 3
