"""Minimal fakes of confluent_kafka objects so producer/consumer logic is unit-testable without a broker."""
from __future__ import annotations

from confluent_kafka import KafkaError


class FakeKafkaMessage:
    def __init__(self, value, topic="machine-sensor-data", partition=0, offset=0, error=None):
        self._value, self._topic, self._partition, self._offset, self._error = value, topic, partition, offset, error

    def value(self): return self._value
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def error(self): return self._error


class FakeProducer:
    """Records produce() calls; delivers callbacks on poll()/flush() like the real client."""

    def __init__(self, events=None, buffer_errors=0, produce_exception=None, fail_delivery=False):
        self.sent, self.pending, self.events = [], [], events if events is not None else []
        self.buffer_errors, self.produce_exception, self.fail_delivery = buffer_errors, produce_exception, fail_delivery
        self.flushed = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.produce_exception is not None:
            raise self.produce_exception
        if self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append({"topic": topic, "key": key, "value": value})
        self.pending.append((on_delivery, topic, len(self.sent) - 1))

    def _deliver(self):
        while self.pending:
            cb, topic, offset = self.pending.pop(0)
            if cb:
                err = KafkaError(KafkaError._MSG_TIMED_OUT) if self.fail_delivery else None
                cb(err, FakeKafkaMessage(None, topic=topic, partition=0, offset=offset))

    def poll(self, timeout=0):
        self._deliver()
        return 0

    def flush(self, timeout=None):
        self.flushed += 1
        self.events.append("producer.flush")
        self._deliver()
        return 0


class FakeConsumer:
    """poll() returns scripted items (messages / None / error messages), then None forever."""

    def __init__(self, script, events=None, assign_after_polls=0):
        self.script, self.events = list(script), events if events is not None else []
        self.subscribed, self.commits, self.closed = None, 0, False
        self._on_assign, self._polls, self._assign_after = None, 0, assign_after_polls

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.subscribed, self._on_assign = topics, on_assign

    def poll(self, timeout=None):
        self._polls += 1
        if self._on_assign and self._polls > self._assign_after:        # like the real client: delivered inside poll()
            cb, self._on_assign = self._on_assign, None
            cb(self, [type("TP", (), {"partition": 0})()])
        if self._polls <= self._assign_after:
            return None
        return self.script.pop(0) if self.script else None

    def commit(self, asynchronous=True):
        self.commits += 1
        self.events.append("consumer.commit")

    def close(self):
        self.closed = True
