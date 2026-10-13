import logging
import urllib.error
import urllib.request
import sys
import pytest
from fastapi.testclient import TestClient
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import create_app
from db.config import ConfigurationError
from observability.metrics import ApiMetrics, ConsumerMetrics, MetricsServer
from streaming.db_consumer import PersistingSensorConsumer, ProcessingHalted, _metrics_endpoint
from tests.phase3.conftest import OVERSTRAIN, auth_headers
from tests.phase3.test_db_consumer import SETTINGS, op_err, outcome, payload
from tests.phase4.test_dlq import ScriptedProducer, build, kmsg


def parse(text):
    return {(s.name, tuple(sorted(s.labels.items()))): s.value for f in text_string_to_metric_families(text) for s in f.samples}


def value(samples, name, **labels):
    return samples.get((name, tuple(sorted(labels.items()))))


def label_values(text, label):
    return {s.labels[label] for f in text_string_to_metric_families(text) for s in f.samples if label in s.labels}


def dead_app():
    sf = sessionmaker(bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/db", connect_args={"connect_timeout": 1}))
    return create_app(session_factory=sf, kafka_check=None)


# ============================================================================================ API
def test_metrics_endpoint_is_public_prometheus_text_and_hidden_from_openapi():
    c = TestClient(dead_app())
    r = c.get("/metrics")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain") and "version=" in r.headers["content-type"]
    assert "http_requests_total" in r.text

# ProcessCollector reads /proc, which is available on Linux.
    if sys.platform.startswith("linux"):
        assert "process_" in r.text

    assert "/metrics" not in c.get("/openapi.json").json()["paths"]


def test_requests_are_counted_by_method_route_template_and_status():
    c = TestClient(dead_app(), headers=auth_headers("viewer"))
    c.get("/health"); c.get("/health")
    c.get("/api/v1/predictions/987654")                          # DB is dead -> 503
    s = parse(c.get("/metrics").text)
    assert value(s, "http_requests_total", method="GET", route="/health", status="200") == 2.0
    assert value(s, "http_requests_total", method="GET", route="/api/v1/predictions/{prediction_id}", status="503") == 1.0
    assert value(s, "http_request_duration_seconds_count", method="GET", route="/health") == 2.0


def test_cardinality_stays_bounded_whatever_clients_request():
    c = TestClient(dead_app(), headers=auth_headers("viewer"))

    def hammer(start, n):
        for i in range(start, start + n):
            c.get(f"/api/v1/predictions/{i}")                        # n different ids
            c.get(f"/random-path-{i}/x{i}")                          # n different unknown paths
            c.get(f"/api/v1/machines/M{i:04d}/alerts")               # n different machine ids
    hammer(0, 30)
    c.get("/metrics")                       # warm-up: a scrape only counts itself AFTER rendering, so /metrics' own series appears now
    first = c.get("/metrics").text
    hammer(1000, 90)                                                  # 270 more requests with brand-new ids / paths
    second = c.get("/metrics").text
    assert len(second.splitlines()) == len(first.splitlines())        # the number of series did not grow at all
    assert label_values(second, "route") == {"/api/v1/predictions/{prediction_id}", "/api/v1/machines/{machine_id}/alerts",
                                             "unmatched", "/metrics"}
    for leaked in ("M0042", "M1042", "random-path", "/predictions/1042"):
        assert leaked not in second


def test_exotic_http_methods_are_bucketed_not_new_series():
    m = ApiMetrics()
    for method in ("PROPFIND", "BREW", "XYZ"):
        m.observe_request(method, "unmatched", 405, 0.001)
    assert label_values(m.render()[0].decode(), "method") == {"OTHER"}


def test_no_sensitive_data_in_api_metrics():
    c = TestClient(dead_app())
    c.post("/api/v1/auth/login", json={"username": "someone-secret-name", "password": "hunter2hunter2"})
    c.get("/api/v1/predictions", headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"})
    text = c.get("/metrics").text
    for forbidden in ("someone-secret-name", "hunter2", "eyJ", "Bearer", "postgresql", "JWT_SECRET", "unit-test-secret"):
        assert forbidden not in text
    assert label_values(text, "status") <= {"200", "401", "422", "500", "503"}


def test_unhandled_errors_are_counted_as_500():
    app = dead_app()

    @app.get("/boom")
    def boom():
        raise RuntimeError("x")
    c = TestClient(app, raise_server_exceptions=False)
    c.get("/boom")
    assert value(parse(c.get("/metrics").text), "http_requests_total", method="GET", route="/boom", status="500") == 1.0


def test_each_app_has_its_own_registry_so_create_app_can_be_called_repeatedly():
    a, b = TestClient(dead_app()), TestClient(dead_app())
    a.get("/health")
    assert value(parse(b.get("/metrics").text), "http_requests_total", method="GET", route="/health", status="200") is None


def test_multi_worker_setting_logs_a_warning(monkeypatch, capsys):
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    dead_app()                       # create_app() installs its own log handler (stderr), so capture that, not caplog
    assert "single-worker" in capsys.readouterr().err


@pytest.mark.integration
@pytest.mark.postgres
def test_login_metrics_with_real_database(session_factory, make_user):
    c = TestClient(create_app(session_factory=session_factory, kafka_check=None))
    u, p = make_user("viewer")
    c.post("/api/v1/auth/login", json={"username": u, "password": p})
    c.post("/api/v1/auth/login", json={"username": u, "password": "wrong-password-1"})
    s = parse(c.get("/metrics").text)
    assert value(s, "auth_login_attempts_total", result="success") == 1.0 and value(s, "auth_login_attempts_total", result="failure") == 1.0


# ============================================================================================ consumer
def run_consumer(messages, behaviours=(), plan=lambda t, n: "ok", **kw):
    c, cons, prod, ing, events, sleeps = build(messages, behaviours, plan, **kw)
    return c, parse(c.metrics.registry and __import__("prometheus_client").generate_latest(c.metrics.registry).decode())


def exposition(c):
    from prometheus_client import generate_latest
    return generate_latest(c.metrics.registry).decode()


def test_consumer_metrics_cover_every_outcome():
    from services.ingestion_service import IngestionOutcome, PredictionError
    msgs = [kmsg(payload(), 0), kmsg(payload(), 1), kmsg(b"poison", 2), kmsg(payload(), 3), kmsg(payload(OVERSTRAIN), 4)]
    c, *_ = build(msgs, [outcome(), IngestionOutcome(duplicate=True), PredictionError("bad"), outcome(alert=True)])
    c.run(max_messages=5)
    s = parse(exposition(c))
    assert value(s, "consumer_messages_total", outcome="persisted") == 2.0
    assert value(s, "consumer_messages_total", outcome="duplicate") == 1.0
    assert value(s, "consumer_messages_total", outcome="dead_lettered") == 2.0
    assert value(s, "consumer_messages_total", outcome="halted") == 0.0
    assert value(s, "consumer_dlq_messages_total", reason="invalid_schema") == 1.0
    assert value(s, "consumer_dlq_messages_total", reason="prediction_failed") == 1.0
    assert value(s, "consumer_alerts_created_total") == 1.0
    assert value(s, "consumer_offsets_committed_total") == 5.0
    assert value(s, "consumer_message_processing_seconds_count") == 5.0
    assert value(s, "consumer_downstream_events_total", kind="prediction", result="success") == 0.0
    assert value(s, "consumer_downstream_events_total", kind="alert", result="success") == 0.0


def test_consumer_metrics_for_db_and_dlq_retries_without_inline_downstream_retries():
    plan = lambda t, n: "fail" if t in (SETTINGS.prediction_topic, SETTINGS.dlq_topic) and n == 1 else "ok"
    c, *_ = build([kmsg(payload(), 0), kmsg(b"poison", 1)], [op_err(), outcome()], plan=plan)
    c.run(max_messages=2)
    s = parse(exposition(c))
    assert value(s, "consumer_db_retries_total") == 1.0
    assert value(s, "consumer_downstream_publish_retries_total") == 0.0
    assert value(s, "consumer_dlq_publish_retries_total") == 1.0
    assert value(s, "consumer_downstream_events_total", kind="prediction", result="failure") == 0.0


def test_consumer_metrics_for_dlq_halt():
    h, *_ = build([kmsg(b"poison", 0)], plan=lambda t, n: "fail", dlq_retries=1)
    with pytest.raises(ProcessingHalted):
        h.run(max_messages=1)
    s = parse(exposition(h))
    assert value(s, "consumer_dlq_publish_failures_total") == 1.0 and value(s, "consumer_messages_total", outcome="halted") == 1.0


def test_consumer_metrics_have_only_closed_label_sets_and_no_machine_ids():
    msgs = [kmsg(payload(machine=f"MACHINE-{i:03d}", ts=f"2026-10-08T10:30:{i:02d}Z"), i) for i in range(30)]
    c, *_ = build(msgs, [outcome() for _ in range(30)])
    c.run(max_messages=30)
    text = exposition(c)
    assert "MACHINE-" not in text and "M001" not in text
    names = {k for f in text_string_to_metric_families(text) for s in f.samples for k in s.labels}
    assert names <= {"outcome", "reason", "kind", "result", "le"}
    assert label_values(text, "outcome") <= set(ConsumerMetrics.OUTCOMES)
    assert label_values(text, "reason") <= set(ConsumerMetrics.DLQ_REASONS)
    assert label_values(text, "kind") <= set(ConsumerMetrics.KINDS)


def test_heartbeat_advances_while_the_consumer_loops():
    c, *_ = build([])
    c.metrics.last_heartbeat.set(1.0)
    c.run(should_stop=iter([False, False, True]).__next__)
    assert c.metrics.heartbeat_age_s() < 5


# ============================================================================================ consumer HTTP endpoint
def fetch(url):
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_metrics_server_serves_metrics_and_health_on_loopback_only_by_default():
    m = ConsumerMetrics()
    server = MetricsServer(m, port=0).start()
    try:
        assert server._server.server_address[0] == "127.0.0.1"
        status, body = fetch(f"http://127.0.0.1:{server.port}/metrics")
        assert status == 200 and "consumer_messages_total" in body
        assert fetch(f"http://127.0.0.1:{server.port}/healthz") == (200, "ok\n")
        assert fetch(f"http://127.0.0.1:{server.port}/other")[0] == 404
    finally:
        server.stop()


def test_healthz_turns_503_when_the_poll_loop_stalls():
    m = ConsumerMetrics()
    server = MetricsServer(m, port=0, max_heartbeat_age_s=30).start()
    try:
        m.last_heartbeat.set(m.last_heartbeat._value.get() - 120)         # no heartbeat for two minutes
        assert fetch(f"http://127.0.0.1:{server.port}/healthz") == (503, "stalled\n")
        m.heartbeat()
        assert fetch(f"http://127.0.0.1:{server.port}/healthz")[0] == 200
    finally:
        server.stop()


def test_binding_to_a_non_loopback_address_is_logged(caplog):
    caplog.set_level(logging.WARNING)
    server = MetricsServer(ConsumerMetrics(), host="0.0.0.0", port=0)
    server._server.server_close()
    assert "non-loopback" in caplog.text


def test_metrics_endpoint_settings():
    assert _metrics_endpoint({}) == ("127.0.0.1", 9101)
    assert _metrics_endpoint({"CONSUMER_METRICS_PORT": "0"}) == ("127.0.0.1", None)
    assert _metrics_endpoint({"CONSUMER_METRICS_ADDR": "0.0.0.0", "CONSUMER_METRICS_PORT": "9200"}) == ("0.0.0.0", 9200)
    for bad in ({"CONSUMER_METRICS_PORT": "abc"}, {"CONSUMER_METRICS_PORT": "70000"}, {"CONSUMER_METRICS_ADDR": "localhost"}):
        with pytest.raises(ConfigurationError):
            _metrics_endpoint(bad)


def test_consumer_main_exits_2_on_bad_metrics_or_app_env_settings(monkeypatch):
    from streaming.db_consumer import main
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:q8Zr1VxLm3Tn7KbW@127.0.0.1:1/db")
    monkeypatch.setenv("CONSUMER_METRICS_PORT", "abc")
    assert main([]) == 2
    monkeypatch.setenv("CONSUMER_METRICS_PORT", "9101")
    monkeypatch.setenv("APP_ENV", "prod")
    assert main([]) == 2
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:change-me@127.0.0.1:1/db")
    assert main([]) == 2
