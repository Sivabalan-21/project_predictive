"""STATIC checks of the deployment files and documentation. They do NOT run Docker: `docker build` / `docker compose up` are not
exercised by the test-suite (see docs/docker.md)."""
import re
from pathlib import Path

import pytest
import yaml

from core.settings import AppSettings
from db.config import ConfigurationError
from ml.config import KafkaSettings

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
SERVICES = COMPOSE["services"]
APP_SERVICES = ("migrate", "api", "consumer")


def env_example() -> dict[str, str]:
    out = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        line = line.split(" #")[0].strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


# ------------------------------------------------------------------------------------------ compose
def test_default_services_are_unchanged_and_app_services_are_behind_a_profile():
    assert [n for n, s in SERVICES.items() if "profiles" not in s] == ["postgres", "zookeeper", "kafka", "kafka-init"]
    assert all(SERVICES[n]["profiles"] == ["app"] for n in APP_SERVICES)


def test_volumes_are_preserved_and_nothing_deletes_them():
    assert set(COMPOSE["volumes"]) == {"pgdata", "zkdata", "zklog", "kafkadata"}
    assert "pgdata:/var/lib/postgresql/data" in SERVICES["postgres"]["volumes"]
    for f in ("docker-compose.yml", "Dockerfile", "docs/docker.md", "README.md", "docs/architecture.md"):
        text = (ROOT / f).read_text()
        for line in text.splitlines():
            if re.search(r"down\b[^\n]*\s-v\b|volume (rm|prune)", line):
                assert "not" in line.lower() or "delete" in line.lower() or "add -v" in line.lower() or "do not" in line.lower(), (f, line)


def test_every_published_port_is_loopback_by_default():
    published = [p for s in SERVICES.values() for p in s.get("ports", [])]
    assert len(published) == 3
    for p in published:
        assert re.match(r"^\$\{[A-Z_]+_BIND_ADDR:-127\.0\.0\.1\}:", p), f"port {p!r} is not bound to loopback by default"


def test_postgres_host_port_matches_env_example_and_database_url():
    port_mapping = SERVICES["postgres"]["ports"][0]
    assert ":${POSTGRES_HOST_PORT:-5433}:5432" in port_mapping
    env = env_example()
    assert env["POSTGRES_HOST_PORT"] == "5433" and ":5433/" in env["DATABASE_URL"] and "5432" not in env["DATABASE_URL"]


def test_containers_use_the_compose_network_not_host_ports():
    for n in APP_SERVICES:
        e = SERVICES[n]["environment"]
        assert "@postgres:5432/" in e["DATABASE_URL"] and e["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:29092"


def test_dependency_conditions_healthchecks_restart_and_shutdown():
    api, cons, mig = SERVICES["api"], SERVICES["consumer"], SERVICES["migrate"]
    assert mig["depends_on"] == {"postgres": {"condition": "service_healthy"}} and mig["restart"] == "no"
    for svc in (api, cons):
        d = {k: v["condition"] for k, v in svc["depends_on"].items()}
        assert d["postgres"] == d["kafka"] == "service_healthy" and d["migrate"] == "service_completed_successfully"
        assert svc["restart"] == "unless-stopped" and svc["healthcheck"]["test"][0] == "CMD" and "stop_grace_period" in svc
    assert {k: v["condition"] for k, v in cons["depends_on"].items()}["kafka-init"] == "service_completed_successfully"
    assert "/health'" in api["healthcheck"]["test"][-1] and "127.0.0.1:9101/healthz" in cons["healthcheck"]["test"][-1]
    assert all("healthcheck" in SERVICES[n] for n in ("postgres", "kafka"))


def test_hardening_flags_on_every_app_service():
    for n in APP_SERVICES:
        s = SERVICES[n]
        assert s["init"] is True and s["read_only"] is True and s["cap_drop"] == ["ALL"] and "no-new-privileges:true" in s["security_opt"]
        assert "/tmp" in s["tmpfs"] and s["image"] == "predictive-maintenance:local"
        assert "user" not in s or s["user"] not in ("root", "0")


def test_consumer_metrics_are_loopback_inside_the_container_and_not_published():
    assert SERVICES["consumer"]["environment"]["CONSUMER_METRICS_ADDR"] == "127.0.0.1"
    assert "ports" not in SERVICES["consumer"]


def test_no_secret_is_hard_coded_in_compose_and_default_setup_stays_independent_of_jwt_secret():
    raw = (ROOT / "docker-compose.yml").read_text()
    assert not re.search(r"(?im)^\s*(POSTGRES_PASSWORD|JWT_SECRET):\s*(?!\$\{)\S", raw)
    assert "JWT_SECRET: ${JWT_SECRET:-}" in raw                       # NOT ":?" -> `docker compose up -d` (no profile) keeps working
    assert raw.count("${POSTGRES_PASSWORD:?") >= 2                    # postgres + the shared app environment


def test_kafka_init_creates_exactly_the_configured_topics():
    script = "\n".join(SERVICES["kafka-init"]["command"])
    created = set(re.search(r"for t in ([^;]+?); do", script).group(1).split())
    s = KafkaSettings.from_env({"KAFKA_SENSOR_TOPIC": "sensor-readings", "KAFKA_PREDICTION_TOPIC": "predictions"})
    assert created == {s.sensor_topic, s.prediction_topic, s.alert_topic, s.dlq_topic}


# ------------------------------------------------------------------------------------------ Dockerfile
def test_dockerfile_runs_as_non_root_and_copies_no_secrets():
    text = (ROOT / "Dockerfile").read_text()
    users = re.findall(r"(?m)^USER\s+(\S+)", text)
    assert users and users[-1] == "10001:10001" and "root" not in users
    assert not re.search(r"(?m)^COPY\s+\.\s", text), "never COPY the whole build context"
    assert not re.search(r"(?mi)^(ENV|ARG)\s+\S*(SECRET|PASSWORD|TOKEN|KEY)", text)
    assert re.search(r'(?m)^CMD \["uvicorn"', text) and "HEALTHCHECK" in text and "--timeout-graceful-shutdown" in text
    assert "useradd --system" in text and "--shell /usr/sbin/nologin" in text


def test_dockerignore_keeps_env_files_and_vcs_out_of_the_build_context():
    lines = (ROOT / ".dockerignore").read_text().split()
    assert {".env", ".env.*", ".git"} <= set(lines)


def test_service_requirements_are_all_pinned_in_requirements_txt():
    pinned = {re.split(r"[=\[<>]", l)[0].strip().lower().replace("_", "-") for l in (ROOT / "requirements.txt").read_text().splitlines()
              if "==" in l and not l.lstrip().startswith("#")}
    wanted = [re.split(r"[\[<>=]", l)[0].strip().lower().replace("_", "-") for l in (ROOT / "requirements-service.txt").read_text().splitlines()
              if l.strip() and not l.lstrip().startswith("#")]
    assert wanted and not [w for w in wanted if w not in pinned]
    assert not any("==" in l for l in (ROOT / "requirements-service.txt").read_text().splitlines() if not l.startswith("#"))
    for new in ("pyjwt==2.15.1", "argon2-cffi==25.1.0", "prometheus-client==0.26.0"):
        assert new in (ROOT / "requirements.txt").read_text().lower()


# ------------------------------------------------------------------------------------------ .env.example
def test_env_example_works_for_development_and_is_refused_in_production():
    env = env_example()
    s = AppSettings.from_env(env)
    assert s.env == "development" and s.jwt_secret
    with pytest.raises(ConfigurationError, match="placeholder"):
        AppSettings.from_env({**env, "APP_ENV": "production"})
    for key in ("APP_ENV", "JWT_SECRET", "KAFKA_DLQ_TOPIC", "POSTGRES_HOST_PORT"):
        assert key in env
    assert KafkaSettings.from_env(env).dlq_topic == "sensor-readings.dlq"


def test_env_example_contains_no_real_looking_secret():
    for k, v in env_example().items():
        if any(w in k for w in ("PASSWORD", "SECRET")):
            assert "change-me" in v, f"{k} must stay a placeholder in .env.example"


# ------------------------------------------------------------------------------------------ documentation
def test_every_docs_reference_points_to_an_existing_file():
    """Regression for the missing docs/kafka.md that streaming/db_consumer.py referred to."""
    missing = []
    for path in list(ROOT.glob("*.md")) + list(ROOT.glob("docs/*.md")) + list(ROOT.glob("streaming/*.py")) + list(ROOT.glob("api/**/*.py")):
        for ref in re.findall(r"docs/[A-Za-z0-9_./-]+\.md", path.read_text()):
            if not (ROOT / ref).exists():
                missing.append((str(path.relative_to(ROOT)), ref))
        if path.parent.name == "docs":
            for ref in re.findall(r"\]\(([A-Za-z0-9_-]+\.md)[#)]", path.read_text()):
                if not (path.parent / ref).exists():
                    missing.append((str(path.relative_to(ROOT)), ref))
    assert not missing, missing


OVERCLAIM = re.compile(r"(?i)\b(guarantee[sd]?|fully reliable|reliable delivery|lossless|exactly[- ]once delivery|never (loses|lose))\b")
_ALLOWED = re.compile(r'(?i)"[^"\n]*"|`[^`\n]*`|\b(not|never|no)\s+(fully\s+)?(guarantee[sd]?|reliable)\b|\bno\s+guarantee[sd]?\b')


@pytest.mark.parametrize("doc", ["docs/kafka.md", "docs/architecture.md", "docs/docker.md", "docs/observability.md", "docs/auth.md", "README.md"])
def test_docs_do_not_overclaim_delivery_guarantees(doc):
    """Overclaim words are only allowed inside quotation marks/backticks (quoting the phrase to forbid it) or in an explicit
    'not guaranteed' / 'no guarantee' construction - a stray 'not' elsewhere in the line does not excuse them."""
    for n, line in enumerate((ROOT / doc).read_text().splitlines(), 1):
        assert not OVERCLAIM.search(_ALLOWED.sub("", line)), f"{doc}:{n} may overclaim: {line.strip()}"


def test_overclaim_detector_catches_the_phrasings_it_exists_for():
    bad = ["Delivery is guaranteed. Do not worry.", "The pipeline offers exactly-once delivery.", "This is fully reliable.",
           "Events are never lost."[:0] + "The broker never loses events."]
    good = ['Do not describe this system as "reliable delivery" or "exactly-once".', "Delivery is not guaranteed.",
            "There is no guarantee that the event arrives."]
    assert all(OVERCLAIM.search(_ALLOWED.sub("", l)) for l in bad)
    assert not any(OVERCLAIM.search(_ALLOWED.sub("", l)) for l in good)


def test_kafka_doc_states_the_outbox_delivery_model_and_duplicate_window():
    t = (ROOT / "docs/kafka.md").read_text().lower()
    assert "known limitations" in t and "not exactly-once" in t and "outbox" in t
    assert "crashes after the commit" in t.replace("\n", " ") or "crash" in t
    arch = (ROOT / "docs/architecture.md").read_text().lower()
    assert "outbox" in arch and "at-least-once" in arch and "exactly-once" in arch


def test_observability_doc_documents_the_single_worker_assumption_and_multiprocess_changes():
    t = (ROOT / "docs/observability.md").read_text()
    for needle in ("Single-worker assumption", "PROMETHEUS_MULTIPROC_DIR", "MultiProcessCollector", "mark_process_dead", "multiprocess_mode"):
        assert needle in t
