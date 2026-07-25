# syntax=docker/dockerfile:1
FROM python:3.12-slim AS build
WORKDIR /app
# Install deps into an isolated prefix so the runtime stage stays lean.
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
# Run as a dedicated non-root user.
RUN useradd --system --create-home --uid 10001 appuser
WORKDIR /app
COPY --from=build /install /usr/local
COPY merge.py app.py ./
USER appuser
EXPOSE 9090
ENTRYPOINT ["python", "app.py"]
