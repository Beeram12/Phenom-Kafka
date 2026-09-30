"""Async gRPC load generator with realistic Indian food-delivery user journeys.

Journey: session_started -> item_viewed (1-5) -> price_filter_applied? -> cart_item_added (1-3)
         -> order_placed -> rating_prompt_shown -> rating_submitted
session_id / order_id / item ids are consistent within a journey.

Distributions:
  * journeys (and therefore orders) peak at lunch 12:30-14:30 and dinner 19:30-22:30 IST
  * ratings skewed to 4-5 stars
  * item and restaurant popularity follow a Zipf law

Fault injection:
  --late-ratio       events with event_time 1-3 days in the past (accepted: late is valid)
  --invalid-ratio    malformed events (rating=7, negative price, missing identity/tenant-scoped
                     key fields) -> REJECTED_INVALID + DLQ. A missing client tenant_id alone is
                     NOT an error: the server always derives tenant_id from the API key.
  --duplicate-ratio  events re-sent later with the same event_id -> DUPLICATE

Writes a JSON report (for scripts/consume_and_verify.py) and prints a summary.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
import uuid
from collections import Counter, deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import grpc
from faker import Faker

from ingestion.generated.events.v1 import (
    events_pb2,
    ingestion_service_pb2,
    ingestion_service_pb2_grpc,
)

IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = 1
MIN_PRICE_INR = 60
MAX_PRICE_INR = 900
MAX_REQUEST_ATTEMPTS = 5

CATEGORY_CATALOG: dict[str, tuple[int, int, list[str]]] = {
    "biryani": (
        180,
        450,
        [
            "Hyderabadi Chicken Dum Biryani",
            "Mutton Biryani",
            "Veg Biryani",
            "Egg Biryani",
            "Paneer Biryani",
            "Lucknowi Biryani",
            "Kolkata Chicken Biryani",
            "Prawn Biryani",
            "Ambur Biryani",
            "Chicken 65 Biryani",
        ],
    ),
    "dosa": (
        60,
        180,
        [
            "Masala Dosa",
            "Plain Dosa",
            "Mysore Masala Dosa",
            "Rava Dosa",
            "Onion Uttapam",
            "Set Dosa",
            "Ghee Roast Dosa",
            "Paneer Dosa",
            "Neer Dosa",
            "Pesarattu",
        ],
    ),
    "thali": (
        150,
        350,
        [
            "North Indian Veg Thali",
            "South Indian Meals",
            "Gujarati Thali",
            "Rajasthani Thali",
            "Punjabi Thali",
            "Bengali Fish Thali",
            "Maharashtrian Thali",
            "Andhra Meals",
            "Chicken Thali",
            "Mini Thali",
        ],
    ),
    "pizza": (
        200,
        900,
        [
            "Margherita Pizza",
            "Farmhouse Pizza",
            "Paneer Tikka Pizza",
            "Chicken Tikka Pizza",
            "Pepperoni Pizza",
            "Veggie Supreme Pizza",
            "BBQ Chicken Pizza",
            "Cheese Burst Pizza",
            "Peri Peri Paneer Pizza",
            "Tandoori Chicken Pizza",
        ],
    ),
    "burger": (
        100,
        350,
        [
            "Aloo Tikki Burger",
            "Chicken Burger",
            "Paneer Burger",
            "Veg Maharaja Burger",
            "Crispy Chicken Burger",
            "Double Cheese Burger",
            "Spicy Paneer Burger",
            "Fish Burger",
            "Mushroom Burger",
            "Egg Burger",
        ],
    ),
    "chinese": (
        140,
        380,
        [
            "Veg Hakka Noodles",
            "Chicken Fried Rice",
            "Gobi Manchurian",
            "Chilli Chicken",
            "Schezwan Noodles",
            "Veg Momos",
            "Chicken Momos",
            "Paneer Chilli",
            "Spring Rolls",
            "American Chopsuey",
        ],
    ),
    "desserts": (
        60,
        250,
        [
            "Gulab Jamun",
            "Rasmalai",
            "Gajar Halwa",
            "Kulfi",
            "Chocolate Brownie",
            "Rasgulla",
            "Jalebi",
            "Kheer",
            "Mysore Pak",
            "Double Ka Meetha",
        ],
    ),
    "beverages": (
        60,
        200,
        [
            "Masala Chai",
            "Filter Coffee",
            "Sweet Lassi",
            "Mango Lassi",
            "Cold Coffee",
            "Fresh Lime Soda",
            "Buttermilk",
            "Rose Milk",
            "Jaljeera",
            "Badam Milk",
        ],
    ),
}
CITIES = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad"]
ENTRY_POINTS = [("home", 0.55), ("push_notification", 0.15), ("search", 0.15), ("deeplink", 0.15)]
REFERRERS = ["", "google", "instagram", "whatsapp", "email_campaign"]
POSITIVE_TAGS = ["tasty", "hot_and_fresh", "good_packaging", "value_for_money", "quick_delivery"]
NEGATIVE_TAGS = ["cold_food", "late_delivery", "missing_item", "too_spicy", "poor_packaging"]
RATING_WEIGHTS = {5: 0.46, 4: 0.32, 3: 0.12, 2: 0.06, 1: 0.04}


def _hour_intensity(local_time: datetime) -> float:
    """Relative order intensity for an IST wall-clock time."""
    minutes = local_time.hour * 60 + local_time.minute
    if 12 * 60 + 30 <= minutes < 14 * 60 + 30:
        return 3.0
    if 19 * 60 + 30 <= minutes < 22 * 60 + 30:
        return 4.0
    if minutes < 7 * 60:
        return 0.05
    if 8 * 60 <= minutes < 11 * 60:
        return 0.6
    if 16 * 60 <= minutes < 19 * 60:
        return 0.8
    return 0.3


@dataclass(frozen=True, slots=True)
class MenuItem:
    """One dish in the shared menu."""

    item_id: str
    name: str
    category: str
    unit_price: float


@dataclass(frozen=True, slots=True)
class Restaurant:
    """A restaurant with its city and the menu items it serves."""

    restaurant_id: str
    name: str
    city: str
    menu: tuple[MenuItem, ...]
    menu_weights: tuple[float, ...]


def zipf_weights(count: int, exponent: float) -> list[float]:
    """Weights proportional to 1 / rank**exponent."""
    return [1 / (rank**exponent) for rank in range(1, count + 1)]


class Catalog:
    """Tenants, restaurants and a ~80-item menu with Zipf popularity."""

    def __init__(self, faker: Faker, rng: random.Random, restaurant_count: int = 50) -> None:
        self.menu = self._build_menu(rng)
        popularity = zipf_weights(len(self.menu), exponent=1.1)
        rng.shuffle(popularity)  # popularity rank independent of catalog order
        self._item_popularity = dict(
            zip((item.item_id for item in self.menu), popularity, strict=True)
        )
        self.restaurants = [
            self._build_restaurant(faker, rng, index) for index in range(restaurant_count)
        ]
        self.restaurant_weights = zipf_weights(restaurant_count, exponent=0.9)

    @staticmethod
    def _build_menu(rng: random.Random) -> list[MenuItem]:
        menu: list[MenuItem] = []
        for category, (min_price, max_price, dish_names) in CATEGORY_CATALOG.items():
            for dish_index, dish_name in enumerate(dish_names):
                price = round(rng.uniform(min_price, max_price) / 5) * 5
                menu.append(
                    MenuItem(
                        item_id=f"item_{category}_{dish_index:02d}",
                        name=dish_name,
                        category=category,
                        unit_price=float(min(max(price, MIN_PRICE_INR), MAX_PRICE_INR)),
                    )
                )
        return menu

    def _build_restaurant(self, faker: Faker, rng: random.Random, index: int) -> Restaurant:
        specialities = rng.sample(list(CATEGORY_CATALOG), k=rng.randint(2, 4))
        menu = tuple(item for item in self.menu if item.category in specialities)
        suffix = rng.choice(["Kitchen", "Bhavan", "Dhaba", "Cafe", "Express", "House", "Corner"])
        return Restaurant(
            restaurant_id=f"rest_{index:03d}",
            name=f"{faker.last_name()}'s {suffix}",
            city=rng.choice(CITIES),
            menu=menu,
            menu_weights=tuple(self._item_popularity[item.item_id] for item in menu),
        )


class EventTimeSampler:
    """Samples journey start times in the last 24h weighted by IST meal-time intensity."""

    def __init__(self, rng: random.Random) -> None:
        self._rng = rng

    def sample(self, now: datetime, journey_span: timedelta) -> datetime:
        """A start time such that the whole journey still ends before ``now``."""
        slot_starts = [now - timedelta(minutes=30 * slot) for slot in range(1, 49)]
        weights = [_hour_intensity(slot_start.astimezone(IST)) for slot_start in slot_starts]
        slot_start = self._rng.choices(slot_starts, weights=weights, k=1)[0]
        sampled = slot_start + timedelta(seconds=self._rng.uniform(0, 1800))
        return min(sampled, now - journey_span - timedelta(seconds=5))


@dataclass(slots=True)
class JourneyBuilder:
    """Builds one coherent user journey as a list of envelopes."""

    catalog: Catalog
    faker: Faker
    rng: random.Random
    time_sampler: EventTimeSampler
    users_per_tenant: int = 2_000
    _cursor: datetime = field(init=False, default_factory=lambda: datetime.now(UTC))

    def build(self) -> list[events_pb2.EventEnvelope]:
        """A journey with realistic funnel drop-off."""
        rng = self.rng
        restaurant = rng.choices(self.catalog.restaurants, weights=self.catalog.restaurant_weights)[
            0
        ]
        user_number = rng.randint(1, self.users_per_tenant)
        source = rng.choices([events_pb2.MOBILE, events_pb2.WEB], weights=[0.7, 0.3])[0]
        platform = "web" if source == events_pb2.WEB else rng.choice(["android", "android", "ios"])
        identity = {
            "user_id": f"user_{user_number:05d}",
            "anonymous_id": f"device_{uuid.UUID(int=rng.getrandbits(128)).hex[:16]}",
            "session_id": f"sess_{uuid.UUID(int=rng.getrandbits(128)).hex}",
            "source": source,
        }
        context = events_pb2.EventContext(
            timezone="Asia/Kolkata",
            city=restaurant.city,
            platform=platform,
            app_version=rng.choice(["5.12.0", "5.13.1", "5.14.0"]),
        )
        self._cursor = self.time_sampler.sample(datetime.now(UTC), journey_span=timedelta(hours=2))
        journey: list[events_pb2.EventEnvelope] = []

        def emit(event_type: str, gap_seconds: tuple[float, float], **payload: object) -> None:
            self._cursor += timedelta(seconds=rng.uniform(*gap_seconds))
            envelope = events_pb2.EventEnvelope(
                event_id=str(uuid.uuid4()),
                event_type=event_type,
                schema_version=SCHEMA_VERSION,
                context=context,
                **identity,  # type: ignore[arg-type]
                **payload,  # type: ignore[arg-type]
            )
            envelope.event_time.FromDatetime(self._cursor)
            journey.append(envelope)

        entry_point = rng.choices(
            [name for name, _ in ENTRY_POINTS], weights=[w for _, w in ENTRY_POINTS]
        )[0]
        emit(
            "session_started",
            (0, 0),
            session_started=events_pb2.SessionStarted(
                entry_point=entry_point, referrer=rng.choice(REFERRERS)
            ),
        )

        viewed_items = rng.choices(
            restaurant.menu, weights=restaurant.menu_weights, k=rng.randint(1, 5)
        )
        for item in viewed_items:
            emit(
                "item_viewed",
                (3, 40),
                item_viewed=events_pb2.ItemViewed(
                    item_id=item.item_id,
                    restaurant_id=restaurant.restaurant_id,
                    unit_price=item.unit_price,
                ),
            )

        if rng.random() < 0.4:
            min_price = float(rng.choice([0, 100, 150, 200]))
            emit(
                "price_filter_applied",
                (2, 20),
                price_filter_applied=events_pb2.PriceFilterApplied(
                    min_price=min_price,
                    max_price=min_price + rng.choice([200, 300, 500]),
                    category=rng.choice(list(CATEGORY_CATALOG)),
                ),
            )

        if rng.random() > 0.6:
            return journey
        cart: dict[str, tuple[MenuItem, int]] = {}
        for item in rng.choices(
            restaurant.menu, weights=restaurant.menu_weights, k=rng.randint(1, 3)
        ):
            quantity = rng.choices([1, 2, 3], weights=[0.75, 0.2, 0.05])[0]
            existing_quantity = cart.get(item.item_id, (item, 0))[1]
            cart[item.item_id] = (item, existing_quantity + quantity)
            emit(
                "cart_item_added",
                (5, 60),
                cart_item_added=events_pb2.CartItemAdded(
                    item_id=item.item_id,
                    unit_price=item.unit_price,
                    quantity=quantity,
                    restaurant_id=restaurant.restaurant_id,
                ),
            )

        if rng.random() > 0.7:
            return journey
        order_id = f"ord_{uuid.UUID(int=rng.getrandbits(128)).hex[:20]}"
        order_items = [
            events_pb2.OrderItem(
                item_id=item.item_id,
                name=item.name,
                category=item.category,
                unit_price=item.unit_price,
                quantity=quantity,
            )
            for item, quantity in cart.values()
        ]
        subtotal = sum(item.unit_price * quantity for item, quantity in cart.values())
        discount = round(subtotal * rng.choice([0.1, 0.15, 0.2]), 2) if rng.random() < 0.3 else 0.0
        emit(
            "order_placed",
            (20, 120),
            order_placed=events_pb2.OrderPlaced(
                order_id=order_id,
                restaurant_id=restaurant.restaurant_id,
                items=order_items,
                subtotal=subtotal,
                discount=discount,
                total=round(subtotal - discount, 2),
                currency="INR",
                order_type=rng.choices(
                    ["delivery", "takeaway", "dine_in"], weights=[0.85, 0.12, 0.03]
                )[0],
            ),
        )

        if rng.random() > 0.9:
            return journey
        emit(
            "rating_prompt_shown",
            (1800, 3600),
            rating_prompt_shown=events_pb2.RatingPromptShown(order_id=order_id),
        )

        if rng.random() > 0.6:
            return journey
        overall = rng.choices(list(RATING_WEIGHTS), weights=list(RATING_WEIGHTS.values()))[0]
        tag_pool = POSITIVE_TAGS if overall >= 4 else NEGATIVE_TAGS
        emit(
            "rating_submitted",
            (5, 120),
            rating_submitted=events_pb2.RatingSubmitted(
                order_id=order_id,
                restaurant_id=restaurant.restaurant_id,
                overall_rating=overall,
                item_ratings=[
                    events_pb2.ItemRating(
                        item_id=item.item_id,
                        rating=min(5, max(1, overall + rng.choice([-1, 0, 0, 1]))),
                    )
                    for item, _ in cart.values()
                ],
                tags=rng.sample(tag_pool, k=rng.randint(0, 2)),
                comment=self.faker.sentence(nb_words=8) if rng.random() < 0.3 else "",
            ),
        )
        return journey


class FaultInjector:
    """Applies late / invalid mutations to individual events."""

    def __init__(self, rng: random.Random, late_ratio: float, invalid_ratio: float) -> None:
        self._rng = rng
        self._late_ratio = late_ratio
        self._invalid_ratio = invalid_ratio

    def apply(self, envelope: events_pb2.EventEnvelope) -> str | None:
        """Mutate in place; returns the kind of invalidity injected, if any."""
        if self._rng.random() < self._late_ratio:
            late_time = envelope.event_time.ToDatetime(tzinfo=UTC) - timedelta(
                days=self._rng.uniform(1, 3)
            )
            envelope.event_time.FromDatetime(late_time)
        if self._rng.random() >= self._invalid_ratio:
            return None
        payload_case = envelope.WhichOneof("payload")
        if payload_case == "rating_submitted":
            envelope.rating_submitted.overall_rating = 7
            return "rating_out_of_range"
        if payload_case in ("item_viewed", "cart_item_added"):
            getattr(envelope, payload_case).unit_price = -abs(self._rng.uniform(10, 200))
            return "negative_price"
        if payload_case == "order_placed" and envelope.order_placed.items:
            envelope.order_placed.items[0].unit_price = -1.0
            return "negative_price"
        # Tenant is taken from the API key, so "missing tenant" is simulated by dropping the
        # fields the tenant-scoped partition key and identity checks need.
        envelope.ClearField("tenant_id")
        envelope.user_id = ""
        envelope.anonymous_id = ""
        return "missing_identity"


class EventStream:
    """Interleaves many concurrent journeys into one stream of events."""

    def __init__(
        self, journey_builder: JourneyBuilder, rng: random.Random, concurrent_journeys: int = 25
    ):
        self._journey_builder = journey_builder
        self._rng = rng
        self._active_journeys: list[deque[events_pb2.EventEnvelope]] = [
            deque(journey_builder.build()) for _ in range(concurrent_journeys)
        ]

    def __iter__(self) -> Iterator[events_pb2.EventEnvelope]:
        while True:
            journey_index = self._rng.randrange(len(self._active_journeys))
            journey = self._active_journeys[journey_index]
            yield journey.popleft()
            if not journey:
                self._active_journeys[journey_index] = deque(self._journey_builder.build())


@dataclass(slots=True)
class RunStatistics:
    """Aggregated results across all requests."""

    status_counts: Counter[str] = field(default_factory=Counter)
    rpc_error_counts: Counter[str] = field(default_factory=Counter)
    invalid_injected: Counter[str] = field(default_factory=Counter)
    ack_latencies_ms: list[float] = field(default_factory=list)
    accepted_event_ids: set[str] = field(default_factory=set)
    duplicate_event_ids: set[str] = field(default_factory=set)
    unresolved_failed_event_ids: set[str] = field(default_factory=set)
    failed_resends: int = 0
    accepted_by_event_type: Counter[str] = field(default_factory=Counter)
    events_sent: int = 0
    duplicates_sent: int = 0


def percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile of pre-sorted values."""
    if not sorted_values:
        return 0.0
    rank = max(0, min(len(sorted_values) - 1, round(fraction * len(sorted_values) + 0.5) - 1))
    return sorted_values[rank]


class LoadGenerator:
    """Sends batches at a target event rate with bounded concurrency and records outcomes."""

    def __init__(
        self,
        stub: ingestion_service_pb2_grpc.EventIngestionServiceAsyncStub,
        api_keys: list[str],
        stream: EventStream,
        fault_injector: FaultInjector,
        rng: random.Random,
        *,
        rate: float,
        duration_seconds: float,
        batch_size: int,
        duplicate_ratio: float,
        max_concurrency: int,
        request_timeout_seconds: float,
    ) -> None:
        self._stub = stub
        self._api_keys = api_keys
        self._stream = iter(stream)
        self._fault_injector = fault_injector
        self._rng = rng
        self._rate = rate
        self._duration_seconds = duration_seconds
        self._batch_size = batch_size
        self._duplicate_ratio = duplicate_ratio
        self._concurrency = asyncio.Semaphore(max_concurrency)
        self._request_timeout_seconds = request_timeout_seconds
        self._resend_queues: dict[str, deque[events_pb2.EventEnvelope]] = {
            key: deque() for key in api_keys
        }
        self.statistics = RunStatistics()

    async def run(self) -> None:
        """Generate load for the configured duration, then wait for outstanding requests."""
        started_at = time.monotonic()
        in_flight: set[asyncio.Task[None]] = set()
        batches_sent = 0
        while time.monotonic() - started_at < self._duration_seconds:
            api_key = self._api_keys[batches_sent % len(self._api_keys)]
            batch = self._next_batch(api_key)
            await self._concurrency.acquire()
            request_task = asyncio.create_task(self._send(api_key, batch))
            in_flight.add(request_task)
            request_task.add_done_callback(in_flight.discard)
            batches_sent += 1
            next_send_at = started_at + batches_sent * self._batch_size / self._rate
            await asyncio.sleep(max(0.0, next_send_at - time.monotonic()))
        if in_flight:
            await asyncio.gather(*in_flight)

    def _next_batch(self, api_key: str) -> list[events_pb2.EventEnvelope]:
        batch: list[events_pb2.EventEnvelope] = []
        resend_queue = self._resend_queues[api_key]
        while len(batch) < self._batch_size:
            if resend_queue and self._rng.random() < 0.5:
                batch.append(resend_queue.popleft())
                self.statistics.duplicates_sent += 1
                continue
            envelope = next(self._stream)
            invalid_kind = self._fault_injector.apply(envelope)
            if invalid_kind:
                self.statistics.invalid_injected[invalid_kind] += 1
            batch.append(envelope)
        return batch

    async def _send(self, api_key: str, batch: list[events_pb2.EventEnvelope]) -> None:
        try:
            request = ingestion_service_pb2.PublishEventsRequest(
                request_id=str(uuid.uuid4()), events=batch
            )
            for attempt in range(MAX_REQUEST_ATTEMPTS):
                sent_at = time.perf_counter()
                try:
                    response = await self._stub.PublishEvents(
                        request,
                        metadata=(("x-api-key", api_key),),
                        timeout=self._request_timeout_seconds,
                    )
                except grpc.aio.AioRpcError as error:
                    self.statistics.rpc_error_counts[error.code().name] += 1
                    retriable = error.code() in (
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        grpc.StatusCode.UNAVAILABLE,
                    )
                    if not retriable or attempt == MAX_REQUEST_ATTEMPTS - 1:
                        self.statistics.status_counts[f"RPC_{error.code().name}"] += len(batch)
                        return
                    await asyncio.sleep(self._rng.uniform(0, 0.2 * 2**attempt))
                    continue
                latency_ms = (time.perf_counter() - sent_at) * 1000
                self._record(api_key, batch, response, latency_ms)
                return
        finally:
            self._concurrency.release()

    def _record(
        self,
        api_key: str,
        batch: list[events_pb2.EventEnvelope],
        response: ingestion_service_pb2.PublishEventsResponse,
        latency_ms: float,
    ) -> None:
        statistics = self.statistics
        statistics.events_sent += len(batch)
        for envelope, result in zip(batch, response.results, strict=True):
            status_name = ingestion_service_pb2.EventStatus.Name(result.status)
            statistics.status_counts[status_name] += 1
            statistics.ack_latencies_ms.append(latency_ms)
            if result.status == ingestion_service_pb2.ACCEPTED:
                statistics.accepted_event_ids.add(result.event_id)
                statistics.accepted_by_event_type[envelope.event_type] += 1
                statistics.unresolved_failed_event_ids.discard(result.event_id)
            elif result.status == ingestion_service_pb2.DUPLICATE:
                # DUPLICATE proves an earlier attempt committed (e.g. after an ack timeout).
                statistics.duplicate_event_ids.add(result.event_id)
                statistics.unresolved_failed_event_ids.discard(result.event_id)
            elif result.status == ingestion_service_pb2.FAILED:
                # A well-behaved client resends FAILED events with the SAME event_id.
                statistics.unresolved_failed_event_ids.add(result.event_id)
                statistics.failed_resends += 1
                self._resend_queues[api_key].append(envelope)
                continue
            if self._rng.random() < self._duplicate_ratio:
                self._resend_queues[api_key].append(envelope)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--target", default="localhost:50051")
    parser.add_argument("--rate", type=float, default=200.0, help="events per second")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds")
    parser.add_argument("--batch-size", type=int, default=50, help="events per gRPC request")
    parser.add_argument("--tenants", type=int, default=5, choices=range(1, 6))
    parser.add_argument("--late-ratio", type=float, default=0.05)
    parser.add_argument("--invalid-ratio", type=float, default=0.02)
    parser.add_argument("--duplicate-ratio", type=float, default=0.03)
    parser.add_argument("--concurrency", type=int, default=16, help="max in-flight requests")
    parser.add_argument("--request-timeout", type=float, default=35.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--report", type=Path, default=Path("generator_report.json"))
    parser.add_argument(
        "--api-key-template",
        default="dev-key-tenant-{n}",
        help="API key per tenant number (must match INGEST_API_KEYS on the server)",
    )
    arguments = parser.parse_args()
    for ratio_name in ("late_ratio", "invalid_ratio", "duplicate_ratio"):
        if not 0 <= getattr(arguments, ratio_name) <= 1:
            parser.error(f"--{ratio_name.replace('_', '-')} must be within [0, 1]")
    return arguments


def _print_summary(statistics: RunStatistics, wall_seconds: float) -> None:
    latencies = sorted(statistics.ack_latencies_ms)
    total = sum(statistics.status_counts.values())
    print("\n=== generator summary ===")
    print(f"wall time            : {wall_seconds:.1f}s")
    print(f"events with a result : {total}  ({total / max(wall_seconds, 1e-9):.0f}/s)")
    print(
        f"re-sent (same id)    : {statistics.duplicates_sent} "
        f"(incl. {statistics.failed_resends} FAILED)"
    )
    print(f"unresolved FAILED    : {len(statistics.unresolved_failed_event_ids)}")
    print(f"invalid injected     : {dict(statistics.invalid_injected)}")
    print("status counts:")
    for status_name in ("ACCEPTED", "DUPLICATE", "REJECTED_INVALID", "SENT_TO_DLQ", "FAILED"):
        print(f"  {status_name:<17}: {statistics.status_counts.get(status_name, 0)}")
    for status_name, count in statistics.status_counts.items():
        if status_name.startswith("RPC_"):
            print(f"  {status_name:<17}: {count}")
    if statistics.rpc_error_counts:
        print(f"rpc errors (incl. retried): {dict(statistics.rpc_error_counts)}")
    print("accepted by event_type:")
    for event_type, count in statistics.accepted_by_event_type.most_common():
        print(f"  {event_type:<21}: {count}")
    print(
        "ack latency ms       : "
        f"p50={percentile(latencies, 0.50):.1f} "
        f"p95={percentile(latencies, 0.95):.1f} "
        f"p99={percentile(latencies, 0.99):.1f} "
        f"max={latencies[-1] if latencies else 0:.1f}"
    )


async def _main(arguments: argparse.Namespace) -> int:
    rng = random.Random(arguments.seed)
    faker = Faker("en_IN")
    faker.seed_instance(arguments.seed)
    catalog = Catalog(faker, rng)
    journey_builder = JourneyBuilder(catalog, faker, rng, EventTimeSampler(rng))
    stream = EventStream(journey_builder, rng)
    fault_injector = FaultInjector(rng, arguments.late_ratio, arguments.invalid_ratio)
    api_keys = [
        arguments.api_key_template.format(n=tenant_number)
        for tenant_number in range(1, arguments.tenants + 1)
    ]

    started_at_ms = int(time.time() * 1000)
    wall_started = time.monotonic()
    async with grpc.aio.insecure_channel(arguments.target) as channel:
        stub = ingestion_service_pb2_grpc.EventIngestionServiceStub(channel)
        generator = LoadGenerator(
            stub,
            api_keys,
            stream,
            fault_injector,
            rng,
            rate=arguments.rate,
            duration_seconds=arguments.duration,
            batch_size=arguments.batch_size,
            duplicate_ratio=arguments.duplicate_ratio,
            max_concurrency=arguments.concurrency,
            request_timeout_seconds=arguments.request_timeout,
        )
        await generator.run()
    finished_at_ms = int(time.time() * 1000)
    statistics = generator.statistics
    _print_summary(statistics, time.monotonic() - wall_started)

    report = {
        "started_at_ms": started_at_ms,
        "finished_at_ms": finished_at_ms,
        "status_counts": dict(statistics.status_counts),
        "accepted_count": len(statistics.accepted_event_ids),
        "accepted_event_ids": sorted(statistics.accepted_event_ids),
        "duplicate_event_ids": sorted(statistics.duplicate_event_ids),
        "unresolved_failed_event_ids": sorted(statistics.unresolved_failed_event_ids),
    }
    arguments.report.write_text(json.dumps(report))
    print(f"report written to {arguments.report}")
    return 0


def main() -> int:
    """CLI entry point."""
    return asyncio.run(_main(_parse_args()))


if __name__ == "__main__":
    sys.exit(main())
