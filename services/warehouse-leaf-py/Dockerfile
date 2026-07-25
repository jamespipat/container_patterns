# warehouse-leaf-py: scatter/gather availability leaf (CONTRACTS.md section 8).
FROM python:3.12-slim

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app

# No third-party deps, but honour the manifest so builds stay reproducible.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

USER appuser
EXPOSE 8080
ENV SHARD_NAME=us-east \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "app.py"]
