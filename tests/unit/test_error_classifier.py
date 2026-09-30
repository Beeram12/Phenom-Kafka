"""ErrorClassifier maps producer exceptions to RETRIABLE / NON_RETRIABLE / FATAL."""

from __future__ import annotations

import pytest
from aiokafka import errors as kafka_errors

from ingestion.kafka.error_classifier import ErrorCategory, ErrorClassifier
from ingestion.kafka.serializers import SerializationError
from ingestion.kafka.transactional_publisher import SendPhaseTimeoutError


@pytest.mark.parametrize(
    ("error", "expected_category"),
    [
        (kafka_errors.KafkaTimeoutError(), ErrorCategory.RETRIABLE),
        (kafka_errors.NotEnoughReplicasError(), ErrorCategory.RETRIABLE),
        (kafka_errors.NotEnoughReplicasAfterAppendError(), ErrorCategory.RETRIABLE),
        (kafka_errors.LeaderNotAvailableError(), ErrorCategory.RETRIABLE),
        (kafka_errors.NotLeaderForPartitionError(), ErrorCategory.RETRIABLE),
        (kafka_errors.RequestTimedOutError(), ErrorCategory.RETRIABLE),
        (kafka_errors.KafkaConnectionError(), ErrorCategory.RETRIABLE),
        (kafka_errors.CoordinatorNotAvailableError(), ErrorCategory.RETRIABLE),
        (kafka_errors.ConcurrentTransactions(), ErrorCategory.RETRIABLE),
        (TimeoutError(), ErrorCategory.RETRIABLE),
        (SendPhaseTimeoutError("send phase"), ErrorCategory.RETRIABLE),
        (kafka_errors.MessageSizeTooLargeError(), ErrorCategory.NON_RETRIABLE),
        (kafka_errors.RecordTooLargeError(), ErrorCategory.NON_RETRIABLE),
        (kafka_errors.InvalidTopicError(), ErrorCategory.NON_RETRIABLE),
        (kafka_errors.CorruptRecordException(), ErrorCategory.NON_RETRIABLE),
        (SerializationError("bad bytes"), ErrorCategory.NON_RETRIABLE),
        (kafka_errors.ProducerFenced(), ErrorCategory.FATAL),
        (kafka_errors.OutOfOrderSequenceNumber(), ErrorCategory.FATAL),
        (kafka_errors.InvalidProducerEpoch(), ErrorCategory.FATAL),
        (kafka_errors.UnknownProducerId(), ErrorCategory.FATAL),
        (kafka_errors.InvalidTxnState(), ErrorCategory.FATAL),
        (kafka_errors.IllegalStateError(), ErrorCategory.FATAL),
        (kafka_errors.ProducerClosed(), ErrorCategory.FATAL),
    ],
    ids=lambda value: type(value).__name__ if isinstance(value, BaseException) else value.value,
)
def test_classifies_known_errors(error: BaseException, expected_category: ErrorCategory) -> None:
    assert ErrorClassifier().classify(error) is expected_category


def test_unknown_errors_default_to_retriable() -> None:
    assert ErrorClassifier().classify(RuntimeError("surprise")) is ErrorCategory.RETRIABLE
