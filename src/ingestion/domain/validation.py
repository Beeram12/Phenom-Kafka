"""Business validation of incoming event envelopes."""

from __future__ import annotations

import math
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ingestion.domain.routing import RoutingError, TopicRouter
from ingestion.generated.events.v1 import events_pb2

MIN_RATING = 1
MAX_RATING = 5


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of validating one envelope; ``reasons`` is empty when valid."""

    reasons: tuple[str, ...] = ()

    @property
    def is_valid(self) -> bool:
        """True when no rule was violated."""
        return not self.reasons

    @property
    def error_message(self) -> str:
        """All violations joined into one human-readable message."""
        return "; ".join(self.reasons)


class EventValidator:
    """Checks required fields, payload/type consistency, value ranges and event_time skew.

    Must run AFTER the server has stamped the authenticated tenant_id onto the envelope.
    """

    def __init__(
        self,
        router: TopicRouter,
        max_clock_skew: timedelta,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._router = router
        self._max_clock_skew = max_clock_skew
        self._clock = clock

    def validate(self, envelope: events_pb2.EventEnvelope) -> ValidationResult:
        """Validate one envelope and return every rule it violates."""
        reasons: list[str] = []
        self._check_envelope(envelope, reasons)
        self._check_routing(envelope, reasons)
        self._check_payload(envelope, reasons)
        return ValidationResult(tuple(reasons))

    def _check_envelope(self, envelope: events_pb2.EventEnvelope, reasons: list[str]) -> None:
        if not envelope.event_id:
            reasons.append("event_id is required")
        else:
            try:
                uuid.UUID(envelope.event_id)
            except ValueError:
                reasons.append("event_id must be a UUID")
        if not envelope.tenant_id:
            reasons.append("tenant_id is required")
        if envelope.schema_version < 1:
            reasons.append("schema_version must be >= 1")
        if envelope.source == events_pb2.EVENT_SOURCE_UNSPECIFIED:
            reasons.append("source is required")
        if not envelope.user_id and not envelope.anonymous_id:
            reasons.append("one of user_id or anonymous_id is required")
        if not envelope.HasField("event_time"):
            reasons.append("event_time is required")
        else:
            event_time = envelope.event_time.ToDatetime(tzinfo=UTC)
            latest_allowed = self._clock() + self._max_clock_skew
            if event_time > latest_allowed:
                reasons.append(
                    f"event_time {event_time.isoformat()} is in the future "
                    f"(more than {int(self._max_clock_skew.total_seconds())}s skew)"
                )

    def _check_routing(self, envelope: events_pb2.EventEnvelope, reasons: list[str]) -> None:
        if not envelope.event_type:
            reasons.append("event_type is required")
            return
        try:
            route = self._router.route_for(envelope.event_type)
        except RoutingError as error:
            reasons.append(str(error))
            return
        payload_case = envelope.WhichOneof("payload")
        if payload_case != route.payload_field:
            reasons.append(
                f"payload '{payload_case}' does not match event_type '{envelope.event_type}' "
                f"(expected '{route.payload_field}')"
            )
            return
        try:
            self._router.resolve(envelope)
        except RoutingError as error:
            reasons.append(str(error))

    def _check_payload(self, envelope: events_pb2.EventEnvelope, reasons: list[str]) -> None:
        payload_case = envelope.WhichOneof("payload")
        if payload_case is None:
            reasons.append("payload is required")
        elif payload_case == "order_placed":
            _check_order_placed(envelope.order_placed, reasons)
        elif payload_case == "rating_submitted":
            _check_rating_submitted(envelope.rating_submitted, reasons)
        elif payload_case == "rating_prompt_shown":
            _require_text(envelope.rating_prompt_shown.order_id, "order_id", reasons)
        elif payload_case == "price_filter_applied":
            _check_price_filter(envelope.price_filter_applied, reasons)
        elif payload_case == "cart_item_added":
            cart_item = envelope.cart_item_added
            _require_text(cart_item.item_id, "item_id", reasons)
            _check_price(cart_item.unit_price, "unit_price", reasons)
            _check_positive_quantity(cart_item.quantity, "quantity", reasons)
        elif payload_case == "item_viewed":
            _require_text(envelope.item_viewed.item_id, "item_id", reasons)
            _check_price(envelope.item_viewed.unit_price, "unit_price", reasons)


def _require_text(value: str, field_name: str, reasons: list[str]) -> None:
    if not value:
        reasons.append(f"{field_name} is required")


def _check_price(value: float, field_name: str, reasons: list[str]) -> None:
    if not math.isfinite(value) or value < 0:
        reasons.append(f"{field_name} must be a finite number >= 0 (got {value})")


def _check_positive_quantity(value: int, field_name: str, reasons: list[str]) -> None:
    if value <= 0:
        reasons.append(f"{field_name} must be > 0 (got {value})")


def _check_rating(value: int, field_name: str, reasons: list[str]) -> None:
    if not MIN_RATING <= value <= MAX_RATING:
        reasons.append(f"{field_name} must be between {MIN_RATING} and {MAX_RATING} (got {value})")


def _check_order_placed(order: events_pb2.OrderPlaced, reasons: list[str]) -> None:
    _require_text(order.order_id, "order_id", reasons)
    _require_text(order.restaurant_id, "restaurant_id", reasons)
    _require_text(order.currency, "currency", reasons)
    if not order.items:
        reasons.append("order must contain at least one item")
    for item_index, item in enumerate(order.items):
        _require_text(item.item_id, f"items[{item_index}].item_id", reasons)
        _check_price(item.unit_price, f"items[{item_index}].unit_price", reasons)
        _check_positive_quantity(item.quantity, f"items[{item_index}].quantity", reasons)
    for amount_name in ("subtotal", "discount", "total"):
        _check_price(getattr(order, amount_name), amount_name, reasons)


def _check_rating_submitted(rating: events_pb2.RatingSubmitted, reasons: list[str]) -> None:
    _require_text(rating.order_id, "order_id", reasons)
    _check_rating(rating.overall_rating, "overall_rating", reasons)
    for rating_index, item_rating in enumerate(rating.item_ratings):
        _require_text(item_rating.item_id, f"item_ratings[{rating_index}].item_id", reasons)
        _check_rating(item_rating.rating, f"item_ratings[{rating_index}].rating", reasons)


def _check_price_filter(price_filter: events_pb2.PriceFilterApplied, reasons: list[str]) -> None:
    _check_price(price_filter.min_price, "min_price", reasons)
    _check_price(price_filter.max_price, "max_price", reasons)
    if price_filter.max_price < price_filter.min_price:
        reasons.append("max_price must be >= min_price")
