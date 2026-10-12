"""Metric definitions. Label values are drawn from small CLOSED sets (HTTP method, route TEMPLATE, status code,
outcome/reason enums). Never use machine ids, usernames, request ids, raw paths or error text as labels: they are
unbounded (memory growth) and may be sensitive.
"""
from __future__ import annotations

import ipaddress
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.process_collector import ProcessCollector

log = logging.getLogger("observability")

_KNOWN_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
_HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_MSG_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 30.0)


class ApiMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        ProcessCollector(registry=self.registry)
        self.http_requests = Counter("http_requests_total", "HTTP requests handled", ["method", "route", "status"],
                                     registry=self.registry)
        self.http_duration = Histogram("http_request_duration_seconds", "HTTP request latency", ["method", "route"],
                                       buckets=_HTTP_BUCKETS, registry=self.registry)
        self.login_attempts = Counter("auth_login_attempts_total", "Login attempts", ["result"], registry=self.registry)
        for result in ("success", "failure"):
            self.login_attempts.labels(result=result)                      # export 0 from the start

    def observe_request(self, method: str, route: str, status: int, seconds: float) -> None:
        method = method if method in _KNOWN_METHODS else "OTHER"
        self.http_requests.labels(method=method, route=route, status=str(status)).inc()
        self.http_duration.labels(method=method, route=route).observe(seconds)

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST


class ConsumerMetrics:
    """Counters for the persisting consumer. ``outcome``: persisted | duplicate | dead_lettered | halted."""
    OUTCOMES = ("persisted", "duplicate", "dead_lettered", "halted")
    DLQ_REASONS = ("invalid_schema", "prediction_failed")
    KINDS = ("prediction", "alert")

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        ProcessCollector(registry=self.registry)
        r = self.registry
        self.messages = Counter("consumer_messages_total", "Kafka messages fully handled, by outcome", ["outcome"], registry=r)
        self.processing = Histogram("consumer_message_processing_seconds", "Time to handle one message (validate, predict, "
                                    "persist, publish, commit)", buckets=_MSG_BUCKETS, registry=r)
        self.dlq_messages = Counter("consumer_dlq_messages_total", "Messages confirmed published to the dead-letter topic",
                                    ["reason"], registry=r)
        self.dlq_retries = Counter("consumer_dlq_publish_retries_total", "Dead-letter publish retries", registry=r)
        self.dlq_failures = Counter("consumer_dlq_publish_failures_total", "Dead-letter publishes that failed after all "
                                    "retries (the consumer halts)", registry=r)
        self.db_retries = Counter("consumer_db_retries_total", "Database retries after transient errors", registry=r)
        self.downstream = Counter("consumer_downstream_events_total", "Downstream events (predictions/alerts topics) after "
                                  "bounded retries", ["kind", "result"], registry=r)
        self.publish_retries = Counter("consumer_downstream_publish_retries_total", "Downstream publish retries", registry=r)
        self.alerts_created = Counter("consumer_alerts_created_total", "New active alerts created", registry=r)
        self.offsets_committed = Counter("consumer_offsets_committed_total", "Kafka offsets committed", registry=r)
        self.last_heartbeat = Gauge("consumer_last_heartbeat_timestamp_seconds", "Unix time of the last poll-loop iteration",
                                    registry=r)
        for o in self.OUTCOMES:
            self.messages.labels(outcome=o)
        for reason in self.DLQ_REASONS:
            self.dlq_messages.labels(reason=reason)
        for kind in self.KINDS:
            for result in ("success", "failure"):
                self.downstream.labels(kind=kind, result=result)
        self.heartbeat()

    def heartbeat(self) -> None:
        self.last_heartbeat.set(time.time())

    def heartbeat_age_s(self) -> float:
        return time.time() - self.last_heartbeat._value.get()


class MetricsServer:
    """Tiny HTTP server for the consumer: ``/metrics`` (Prometheus text) and ``/healthz`` (poll loop still alive).

    Bound to ``127.0.0.1`` by default, so only processes in the same network namespace (the container's own health check, or a
    sidecar) can reach it. Binding elsewhere is an explicit decision (``CONSUMER_METRICS_ADDR``) and is logged as a warning.
    """

    def __init__(self, metrics: ConsumerMetrics, host: str = "127.0.0.1", port: int = 9101, max_heartbeat_age_s: float = 60.0):
        self.metrics, self.host, self._max_age = metrics, host, max_heartbeat_age_s
        if not ipaddress.ip_address(host).is_loopback:
            log.warning("consumer metrics endpoint bound to non-loopback address %s - make sure the network restricts access", host)
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:                                      # noqa: N802 (http.server API)
                if self.path == "/metrics":
                    self._send(200, generate_latest(outer.metrics.registry), CONTENT_TYPE_LATEST)
                elif self.path == "/healthz":
                    age = outer.metrics.heartbeat_age_s()
                    ok = age <= outer._max_age
                    self._send(200 if ok else 503, b"ok\n" if ok else b"stalled\n", "text/plain; charset=utf-8")
                else:
                    self._send(404, b"not found\n", "text/plain; charset=utf-8")

            def _send(self, status: int, body: bytes, ctype: str) -> None:
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args) -> None:                         # keep scrape noise out of the logs
                pass

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="consumer-metrics", daemon=True)

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> "MetricsServer":
        self._thread.start()
        log.info("consumer metrics on http://%s:%d/metrics (health: /healthz)", self.host, self.port)
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
