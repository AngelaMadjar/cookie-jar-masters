FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# E3 adaptation: minimal runtime image for benchmark-upload job used to stage files and aggregate run artifacts.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY tools/upload-benchmarks/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY tools/upload-benchmarks /app/tools/upload-benchmarks

CMD ["python3", "tools/upload-benchmarks/run_upload_benchmarks.py"]
