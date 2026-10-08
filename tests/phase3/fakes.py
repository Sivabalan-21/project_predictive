"""Kafka fakes for Phase 3 consumer tests. They record the ORDER of side effects in a shared ``events`` list."""
from __future__ import annotations

from tests.fakes_kafka import FakeKafkaMessage


class RecordingConsumer:
    def __init__(self, messages, events):
        self.script, self.events, self.committed, self.closed = list(messages), events, [], False

    def subscribe(self, topics, **_): self.subscribed = topics
    def poll(self, timeout=None): return self.script.pop(0) if self.script else None

    def commit(self, message=None, asynchronous=True):
        self.committed.append(message.offset())
        self.events.append(("offset_commit", message.offset()))

    def close(self): self.closed = True


class RecordingProducer:
    def __init__(self, events, fail=False):
        self.events, self.sent, self.fail = events, [], fail

    def produce(self, topic, key=None, value=None, on_delivery=None):
        from confluent_kafka import KafkaException, KafkaError
        if self.fail:
            raise KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))
        self.sent.append((topic, key, value)); self.events.append(("publish", topic))
        if on_delivery: on_delivery(None, None)

    def poll(self, t=0): return 0
    def flush(self, t=None): self.events.append(("flush",)); return 0


class RecordingIngestion:
    """Stands in for IngestionService in pure unit tests; logs when the 'DB transaction' happens."""
    def __init__(self, events, behaviours):
        self.events, self.behaviours, self.calls = events, list(behaviours), 0

    def ingest(self, event):
        self.calls += 1
        b = self.behaviours.pop(0) if self.behaviours else self.behaviours_default
        if isinstance(b, Exception):
            self.events.append(("db_failed", type(b).__name__)); raise b
        self.events.append(("db_commit",)); return b

    behaviours_default = None


def msg(value, offset=0):
    return FakeKafkaMessage(value, topic="sensor-readings", partition=0, offset=offset)
