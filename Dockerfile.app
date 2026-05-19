FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# System deps kept minimal; psycopg2-binary is used from requirements.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# App/runtime files
COPY app /app/app
COPY migrations /app/migrations
COPY run.py /app/run.py
COPY config.py /app/config.py
COPY gunicorn.conf.py /app/gunicorn.conf.py

# E2 requirement: keep only seed data in the app image
COPY data/seed /app/data/seed

EXPOSE 8080

CMD ["gunicorn", "-c", "gunicorn.conf.py", "run:app"]
