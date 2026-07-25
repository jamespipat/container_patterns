# metrics-adapter: stdlib-only Python transform sidecar (:9102/metrics).
# Single-stage is sufficient because there is nothing to compile and no
# third-party wheels to install; python:3.12-slim is the runtime base.
FROM python:3.12-slim

# Fail fast, unbuffered stdout so logs reach the shipper/console promptly.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copied for parity with the other services' build shape; there are no
# third-party dependencies to install (see the file's comment).
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY adapter.py ./

# Run as a non-root user (uid 10001), no shell/login needed.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin adapter
USER 10001

EXPOSE 9102
ENTRYPOINT ["python", "adapter.py"]
