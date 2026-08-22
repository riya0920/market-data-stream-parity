# syntax=docker/dockerfile:1
#
# UNBUILT: no Docker daemon was available where this was written, so
# `docker build` has never run against it. Treat it as a deployment sketch that
# has been reasoned about, not as a verified artifact.
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first: a source change must not invalidate this layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root. A root container that mounts a volume writes root-owned files onto
# the host, which is somebody else's afternoon.
RUN useradd --create-home --uid 10001 appuser \
 && chown -R appuser:appuser /app
USER appuser

# Batch project: no long-running service, so no healthcheck and no port.
# The default command runs the test suite, which is the only thing that makes
# sense as a container entrypoint for a job like this.
CMD ["python", "-m", "pytest", "tests", "-q"]

