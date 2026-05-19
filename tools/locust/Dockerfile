FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY tools/locust/requirements.txt /app/tools/locust/requirements.txt
RUN pip install --no-cache-dir -r /app/tools/locust/requirements.txt

# Keep runtime files needed by the current runner.
COPY tools/locust /app/tools/locust
COPY data/benchmarks /app/data/benchmarks

# Default command; Cloud Run Job args can override.
CMD ["python3", "tools/locust/run_locust_benchmarks.py", "--help"]
