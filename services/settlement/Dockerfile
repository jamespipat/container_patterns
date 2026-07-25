# Multi-stage: install deps into a venv, copy into a slim runtime.
FROM python:3.12-slim AS build
WORKDIR /app
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install -r requirements.txt

FROM python:3.12-slim AS runtime
# Non-root user for least privilege.
RUN useradd --system --uid 10001 --no-create-home settlement
WORKDIR /app
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
COPY --from=build /opt/venv /opt/venv
COPY app.py .
USER settlement
EXPOSE 8090
ENTRYPOINT ["python", "app.py"]
