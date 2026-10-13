
# One image for the API, Kafka consumer, producer and one-shot migration job.
# The command is chosen per service in docker-compose.yml.
# Runs as a non-root user with a root-owned, read-only application tree.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# OpenMP runtime for XGBoost and scikit-learn.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install service dependencies using the project constraints.
COPY requirements.txt requirements-service.txt ./
RUN sed -E 's/\[[^]]*\]//' requirements.txt > /tmp/constraints.txt \
 && pip install -r requirements-service.txt -c /tmp/constraints.txt \
 && rm /tmp/constraints.txt

# Create an unprivileged runtime user.
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app \
    --no-create-home --shell /usr/sbin/nologin app

# Application code.
COPY alembic.ini ./
COPY alembic ./alembic
COPY api ./api
COPY auth ./auth
COPY core ./core
COPY db ./db
COPY ml ./ml
COPY observability ./observability
COPY services ./services
COPY simulation ./simulation
COPY streaming ./streaming

USER 10001:10001

EXPOSE 8000

# API liveness check. Worker services override this in Compose.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"]

# Default application entry point.
CMD ["uvicorn", "api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "20", "--no-access-log"]
