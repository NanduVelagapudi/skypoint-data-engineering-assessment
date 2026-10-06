# One image for both Compose services: `pipeline` runs the default command,
# `tests` runs pytest. Runs as root on purpose (see DECISIONS.md, Docker).
FROM python:3.11.17-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# /app mirrors the repository layout, so config.REPO_ROOT and the tests'
# REPO_ROOT both resolve to /app.
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# The data pack is not copied: Compose mounts ./data read-only at /app/data.
COPY pytest.ini .env.example ./
COPY config/ config/
COPY sql/ sql/
COPY src/ src/
COPY tests/ tests/

# Exec form: python is PID 1, so its exit code is the container's.
CMD ["python", "-m", "pipeline.main"]
