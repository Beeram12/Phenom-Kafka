"""gRPC server construction."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import grpc

from ingestion.config import GrpcSettings
from ingestion.generated.events.v1 import ingestion_service_pb2_grpc

SERVICE_NAME = "events.v1.EventIngestionService"
HEALTH_CHECK_METHOD = f"/{SERVICE_NAME}/HealthCheck"


def build_grpc_server(
    servicer: ingestion_service_pb2_grpc.EventIngestionServiceServicer,
    interceptors: Sequence[grpc.aio.ServerInterceptor[Any, Any]],
    grpc_settings: GrpcSettings,
) -> tuple[grpc.aio.Server, int]:
    """Create the aio server with interceptors (outermost first) and bind its port.

    Returns the server and the bound port (useful when ``port`` is 0 in tests).
    """
    server = grpc.aio.server(
        interceptors=list(interceptors),
        options=[
            ("grpc.max_receive_message_length", grpc_settings.max_receive_message_bytes),
            ("grpc.so_reuseport", 0),
        ],
    )
    ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"{grpc_settings.host}:{grpc_settings.port}")
    return server, bound_port
