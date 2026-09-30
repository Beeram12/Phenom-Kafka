"""gRPC server interceptors: API-key authentication, structured request logging and timing."""

from __future__ import annotations

import hmac
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, cast

import grpc
import structlog

from ingestion.observability.metrics import IngestionMetrics

API_KEY_METADATA = "x-api-key"
AUTHORIZATION_METADATA = "authorization"

UnaryBehavior = Callable[[Any, grpc.aio.ServicerContext[Any, Any]], Awaitable[Any]]
# grpc-stubs declares these generic, but the runtime classes are not subscriptable.
if TYPE_CHECKING:
    MethodHandler = grpc.RpcMethodHandler[Any, Any]
    ServerInterceptorBase = grpc.aio.ServerInterceptor[Any, Any]
else:
    MethodHandler = grpc.RpcMethodHandler
    ServerInterceptorBase = grpc.aio.ServerInterceptor
Continuation = Callable[[grpc.HandlerCallDetails], Awaitable[MethodHandler]]


class TenantContext:
    """Request-scoped holder of the authenticated tenant (backed by a ContextVar).

    The auth interceptor sets it for the duration of one RPC; the servicer reads it. Being
    a ContextVar, the value is isolated per request task - no shared mutable state.
    """

    def __init__(self) -> None:
        self._tenant_id: ContextVar[str | None] = ContextVar(
            "authenticated_tenant_id", default=None
        )

    def current(self) -> str:
        """The authenticated tenant of the current RPC; raises if auth did not run."""
        tenant_id = self._tenant_id.get()
        if tenant_id is None:
            raise PermissionError("no authenticated tenant in context")
        return tenant_id

    def bind(self, tenant_id: str) -> object:
        """Set the tenant for the current context; returns a token for ``reset``."""
        return self._tenant_id.set(tenant_id)

    def reset(self, token: object) -> None:
        """Restore the previous value."""
        self._tenant_id.reset(token)  # type: ignore[arg-type]


class ApiKeyAuthenticator:
    """Resolves an API key to its tenant_id using constant-time comparison."""

    def __init__(self, api_keys: Mapping[str, str]) -> None:
        self._api_keys = {key.encode(): tenant_id for key, tenant_id in api_keys.items()}

    def tenant_for(self, api_key: str | None) -> str | None:
        """Tenant owning ``api_key``, or None if the key is unknown."""
        if not api_key:
            return None
        presented_key = api_key.encode()
        matched_tenant: str | None = None
        for known_key, tenant_id in self._api_keys.items():
            if hmac.compare_digest(known_key, presented_key):
                matched_tenant = tenant_id
        return matched_tenant


def extract_api_key(metadata: Mapping[str, str | bytes]) -> str | None:
    """Read the key from ``x-api-key`` or ``authorization: Bearer <key>``."""
    api_key = metadata.get(API_KEY_METADATA)
    if api_key is None:
        authorization = metadata.get(AUTHORIZATION_METADATA)
        if isinstance(authorization, str) and authorization.lower().startswith("bearer "):
            api_key = authorization[len("bearer ") :].strip()
    return api_key if isinstance(api_key, str) else None


def _wrap_unary_unary(handler: MethodHandler, behavior_wrapper: UnaryBehavior) -> MethodHandler:
    return grpc.unary_unary_rpc_method_handler(
        behavior_wrapper,
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


class AuthInterceptor(ServerInterceptorBase):
    """Rejects calls without a valid API key and binds the resolved tenant to the request."""

    def __init__(
        self,
        authenticator: ApiKeyAuthenticator,
        tenant_context: TenantContext,
        public_methods: frozenset[str] = frozenset(),
    ) -> None:
        self._authenticator = authenticator
        self._tenant_context = tenant_context
        self._public_methods = public_methods

    async def intercept_service(
        self, continuation: Continuation, handler_call_details: grpc.HandlerCallDetails
    ) -> MethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler
        if handler_call_details.method in self._public_methods:
            return handler

        metadata = dict(handler_call_details.invocation_metadata or ())
        tenant_id = self._authenticator.tenant_for(extract_api_key(metadata))
        inner_behavior = cast(UnaryBehavior, handler.unary_unary)
        tenant_context = self._tenant_context

        async def authenticated_behavior(
            request: Any, context: grpc.aio.ServicerContext[Any, Any]
        ) -> Any:
            if tenant_id is None:
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid API key")
            token = tenant_context.bind(tenant_id or "")
            try:
                return await inner_behavior(request, context)
            finally:
                tenant_context.reset(token)

        return _wrap_unary_unary(handler, authenticated_behavior)


class RequestLoggingInterceptor(ServerInterceptorBase):
    """Logs every RPC with its status code and latency, and records timing metrics."""

    def __init__(self, metrics: IngestionMetrics, logger: structlog.stdlib.BoundLogger) -> None:
        self._metrics = metrics
        self._logger = logger

    async def intercept_service(
        self, continuation: Continuation, handler_call_details: grpc.HandlerCallDetails
    ) -> MethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler
        method = handler_call_details.method
        inner_behavior = cast(UnaryBehavior, handler.unary_unary)
        metrics, logger = self._metrics, self._logger

        async def timed_behavior(request: Any, context: grpc.aio.ServicerContext[Any, Any]) -> Any:
            started_at = time.perf_counter()
            status_code = grpc.StatusCode.OK
            try:
                return await inner_behavior(request, context)
            except BaseException:
                status_code = context.code() or grpc.StatusCode.UNKNOWN
                raise
            finally:
                elapsed_seconds = time.perf_counter() - started_at
                code_name = status_code.name if isinstance(status_code, grpc.StatusCode) else "?"
                metrics.grpc_requests_total.labels(method=method, code=code_name).inc()
                metrics.grpc_request_seconds.labels(method=method).observe(elapsed_seconds)
                logger.info(
                    "grpc_request",
                    method=method,
                    code=code_name,
                    duration_ms=round(elapsed_seconds * 1000, 2),
                    request_id=getattr(request, "request_id", None) or None,
                    event_count=len(getattr(request, "events", ())) or None,
                )

        return _wrap_unary_unary(handler, timed_behavior)
