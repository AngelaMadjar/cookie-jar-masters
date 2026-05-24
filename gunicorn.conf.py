# Gunicorn loads this file when starting the Flask app with Gunicorn.
# It defines process/thread concurrency and network binding for serving HTTP.

# Concurrency configuration rationale:
# - workers=1 and threads=8 were selected empirically for the E1 local benchmark (and sequentially, for all benchmarks)
# - This provides bounded parallelism that still produces measurable queue/wait behavior under 80-request burst load.
# - Keeping these values fixed preserves a controlled baseline for KPI comparison across different workload shapes (T1-T7).
# - Higher thread counts (for example, 10 or 12) are valid alternatives, but they can increase lock contention noise.
# - Deadlock behavior was already observed at 8 threads during concurrent tracking_domain inserts; this was mitigated 
#   in the DAO using deadlock detection with exponential backoff retries.

bind = "0.0.0.0:8080"     # Listen on all interfaces at port 8080.
workers = 1               # Run a single worker process.
threads = 8               # Allow up to 8 request-handling threads in that worker.
worker_class = "gthread"  # Use Gunicorn's threaded worker implementation.
timeout = 0               # Disable request timeout (long requests are not force-killed).