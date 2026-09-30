"""Maps producer exceptions to a handling strategy."""

from __future__ import annotations

import enum

from aiokafka import errors as kafka_errors

from ingestion.kafka.serializers import SerializationError


class ErrorCategory(enum.Enum):
    """How the publisher reacts to an error."""

    RETRIABLE = "RETRIABLE"  # transient: back off and retry the whole batch
    NON_RETRIABLE = "NON_RETRIABLE"  # the data is bad: bisect, dead-letter the culprits
    FATAL = "FATAL"  # the producer is unusable: recreate it, then retry


_RETRIABLE: tuple[type[BaseException], ...] = (
    kafka_errors.KafkaTimeoutError,
    kafka_errors.NotEnoughReplicasError,
    kafka_errors.NotEnoughReplicasAfterAppendError,
    kafka_errors.LeaderNotAvailableError,
    kafka_errors.NotLeaderForPartitionError,
    kafka_errors.RequestTimedOutError,
    kafka_errors.KafkaConnectionError,
    kafka_errors.NodeNotReadyError,
    kafka_errors.CoordinatorNotAvailableError,
    kafka_errors.NotCoordinatorError,
    kafka_errors.CoordinatorLoadInProgressError,
    kafka_errors.ConcurrentTransactions,
    kafka_errors.UnknownTopicOrPartitionError,
    TimeoutError,
    ConnectionError,
)

_NON_RETRIABLE: tuple[type[BaseException], ...] = (
    kafka_errors.MessageSizeTooLargeError,
    kafka_errors.RecordTooLargeError,
    kafka_errors.InvalidTopicError,
    kafka_errors.CorruptRecordException,
    kafka_errors.TopicAuthorizationFailedError,
    SerializationError,
)

_FATAL: tuple[type[BaseException], ...] = (
    kafka_errors.ProducerFenced,
    kafka_errors.OutOfOrderSequenceNumber,
    kafka_errors.InvalidProducerEpoch,
    kafka_errors.UnknownProducerId,
    kafka_errors.InvalidProducerIdMapping,
    kafka_errors.InvalidTxnState,
    kafka_errors.TransactionalIdAuthorizationFailed,
    kafka_errors.IllegalStateError,
    kafka_errors.ProducerClosed,
)


class ErrorClassifier:
    """Classifies exceptions raised while producing into RETRIABLE / NON_RETRIABLE / FATAL.

    Order matters: FATAL is checked first so a fenced producer is never retried as-is.
    Unknown errors default to RETRIABLE: bounded retries followed by the DLQ is safer than
    bisecting a batch that is probably fine.
    """

    def classify(self, error: BaseException) -> ErrorCategory:
        """Return the handling category for ``error``."""
        if isinstance(error, _FATAL):
            return ErrorCategory.FATAL
        if isinstance(error, _NON_RETRIABLE):
            return ErrorCategory.NON_RETRIABLE
        if isinstance(error, _RETRIABLE):
            return ErrorCategory.RETRIABLE
        if isinstance(error, kafka_errors.KafkaError) and getattr(error, "retriable", False):
            return ErrorCategory.RETRIABLE
        return ErrorCategory.RETRIABLE
