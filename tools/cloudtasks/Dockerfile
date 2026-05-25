FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY tools/cloudtasks/requirements.txt /app/tools/cloudtasks/requirements.txt
RUN pip install --no-cache-dir -r /app/tools/cloudtasks/requirements.txt

COPY tools/cloudtasks /app/tools/cloudtasks

CMD ["python3", "tools/cloudtasks/run_cloudtasks_benchmarks.py", "--help"]
