# One image for the API, the Kafka consumer and the one-shot migration job (the command is chosen per service in
# docker-compose.yml). Runs as a non-root user with a root-owned, read-only application tree.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# libgomp1: OpenMP runtime for xgboost/scikit-learn. Current wheels vendor their own copy; kept as a cheap safety net
# (e.g. arm64 wheels). Nothing else is installed from apt.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first: this layer is rebuilt only when the requirement files change. Versions come from requirements.txt.
COPY requirements.txt requirements-service.txt ./
# (pip constraint files may not contain extras such as "psycopg[binary]", so they are stripped to build the constraints.)
RUN sed -E 's/\[[^]]*\]//' requirements.txt > /tmp/constraints.txt \
 && pip install -r requirements-service.txt -c /tmp/constraints.txt \
 && rm /tmp/constraints.txt

# Unprivileged runtime user (fixed uid/gid so volume permissions are predictable). No shell, no home directory.
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

# Application code: owned by root and not writable by the runtime user. Only what the service needs (see .dockerignore).
COPY alembic.ini ./
COPY alembic ./alembic
COPY api ./api
COPY auth ./auth
COPY core ./core
COPY db ./db
COPY ml ./ml
COPY observability ./observability
COPY services ./services
COPY streaming ./streaming

USER 10001:10001
EXPOSE 8000

# Liveness only (/health checks no dependency, so a database blip does not make the container "unhealthy").
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"]

# Exec form: uvicorn is the process that receives SIGTERM and drains in-flight requests (<= 20 s) before exiting.
CMD ["uvicorn", "api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "20", "--no-access-log"]
