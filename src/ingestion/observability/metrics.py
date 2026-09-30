"""Prometheus metrics, bound to an injected registry (no module-level collectors)."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


class IngestionMetrics:
    """All service metrics. Create one per process and pass it to every component."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.events_total = Counter(
            "ingestion_events_total",
            "Events by final status.",
            ["status"],
            registry=self.registry,
        )
        self.grpc_requests_total = Counter(
            "ingestion_grpc_requests_total",
            "gRPC requests by method and status code.",
            ["method", "code"],
            registry=self.registry,
        )
        self.grpc_request_seconds = Histogram(
            "ingestion_grpc_request_seconds",
            "gRPC request latency.",
            ["method"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.batch_size_records = Histogram(
            "ingestion_batch_size_records",
            "Records per flushed batch.",
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
            registry=self.registry,
        )
        self.batch_commit_seconds = Histogram(
            "ingestion_batch_commit_seconds",
            "Time to publish and commit one batch transaction (including retries).",
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.transaction_attempts_total = Counter(
            "ingestion_transaction_attempts_total",
            "Kafka transaction attempts by result.",
            ["result"],
            registry=self.registry,
        )
        self.publish_errors_total = Counter(
            "ingestion_publish_errors_total",
            "Publish errors by classification and error class.",
            ["category", "error_class"],
            registry=self.registry,
        )
        self.producer_recreations_total = Counter(
            "ingestion_producer_recreations_total",
            "Transactional producers closed and recreated after fatal errors.",
            registry=self.registry,
        )
        self.dlq_records_total = Counter(
            "ingestion_dlq_records_total",
            "Records written to the DLQ by failed stage.",
            ["stage"],
            registry=self.registry,
        )
        self.dlq_write_failures_total = Counter(
            "ingestion_dlq_write_failures_total",
            "DLQ writes that failed after all retries (event reported FAILED).",
            registry=self.registry,
        )
        self.dedupe_errors_total = Counter(
            "ingestion_dedupe_errors_total",
            "Redis errors in the deduplicator by operation.",
            ["operation"],
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "ingestion_queue_depth",
            "Events waiting in the batch accumulator queue.",
            registry=self.registry,
        )
        self.queue_rejections_total = Counter(
            "ingestion_queue_rejections_total",
            "Requests rejected with RESOURCE_EXHAUSTED because the queue was full.",
            registry=self.registry,
        )

    def serve(self, port: int) -> None:
        """Expose /metrics on ``port`` (no-op when port is 0)."""
        if port:
            start_http_server(port, registry=self.registry)
