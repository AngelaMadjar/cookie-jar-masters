# Benchmark Test Cases and Execution Notes

## Scope
This document defines benchmark test cases `T1` to `T7`, how each run is prepared for fair comparison, what results are produced, and how workloads are generated.

The benchmark focus is one-file-per-request ingestion:
- Endpoint: `POST /scan/ingest`
- Each request processes exactly one CSV file.
- Local app concurrency is fixed to `8`.
- Locust sends `80` requests (one per file), with `80` users and spawn rate `80`.
- Each test case is run 3 times.

## Test Case Definitions

### T1: `T1_original`
- Source: real-world baseline files.
- File count: `80`.
- Shape: original row counts and original tracker composition from source scans.
- Purpose: real-world baseline for external validity.

### T2: `T2_medium_existing_heavy`
- Rows per file: `2,000`.
- Intended composition: `10% new`, `10% failed`, `80% existing`.
- Purpose: medium-size, existing-heavy behavior (lookup/match/upsert path emphasis).

### T3: `T3_medium_new_heavy`
- Rows per file: `2,000`.
- Intended composition: `80% new`, `10% failed`, `10% existing`.
- Purpose: medium-size, insert-heavy behavior.

### T4: `T4_large_existing_heavy`
- Rows per file: `20,000`.
- Intended composition: `10% new`, `10% failed`, `80% existing`.
- Purpose: large-size, existing-heavy behavior.

### T5: `T5_large_new_heavy`
- Rows per file: `20,000`.
- Intended composition: `80% new`, `10% failed`, `10% existing`.
- Purpose: large-size, insert-heavy behavior.

### T6: `T6_skewed_existing_heavy`
- File count: `80` total.
- Shape: `79` small files (`~200` rows) + `1` large file (`200,000` rows).
- Intended composition: `10% new`, `10% failed`, `80% existing`.
- Purpose: straggler-effect + existing-heavy behavior.

### T7: `T7_skewed_new_heavy`
- File count: `80` total.
- Shape: `79` small files (`~200` rows) + `1` large file (`200,000` rows).
- Intended composition: `80% new`, `10% failed`, `10% existing`.
- Purpose: straggler-effect + insert-heavy behavior.

## Fairness and Repeatability Controls

Before every run (and every repeat of a run), the setup is reset in the same way:

1. Empty database:
- `POST /db/empty`

2. Reseed database to a fixed initial state:
- `POST /db/populate/cmp`
- `POST /db/populate/trackers`

3. Clear runtime monthly folders for the benchmark month:
- `data/monthly_tracker_audits/input/<month>`
- `data/monthly_tracker_audits/processed/<month>`
- `data/monthly_tracker_audits/failed/<month>`

4. Stage exactly `80` files for the selected test case into:
- `data/monthly_tracker_audits/input/<month>`

5. Run Locust burst (`80` users, `80` total file requests).

Rationale:
- This keeps schema, seed content, and filesystem state consistent for each repeat.
- It avoids carry-over effects from prior runs.
- It makes comparisons across test cases and repeats fair.

## What `locust_results` Contains

Each test case folder has:
- `locust_results/raw/*.json`: per-run raw records (one entry per request/file), including:
  - HTTP status, latency, error details
  - app response counters (`created_trackers`, `existing_trackers`, `failed_rows`)
  - timing fields (`queue_wait_time_sec`, `processing_time_sec`, `end_to_end_latency_sec`)
  - concurrency observations (`active_processing_requests`, `max_active_requests_seen`)

- `locust_results/aggregated/runs.csv`: one row per run/repeat with derived KPIs:
  - request counts (`requests_total`, `requests_ok`, `requests_failed`)
  - makespan and throughput (`total_processing_time_sec`, `files_per_sec`, `records_per_sec`)
  - p50/p95 timings
  - totals for created/existing/failed rows
  - observed active-processing maxima

- `locust_results/aggregated/summary.csv`: aggregate statistics across repeats for that test case:
  - mean/min/max for key KPIs from `runs.csv`.

## Workload Generation: `scripts/generate_benchmark_workloads.py`

Run:
```bash
python3 scripts/generate_benchmark_workloads.py
```

This regenerates `T2` to `T7` (not `T1`) and writes:
- `input/` files for each case
- `manifest/manifest.json` with expected counts and generated paths

### Category Definitions Used by Generator

The generator assigns each row to one of three categories:

1. Failed trackers
- `tracker_name` is rewritten as `_ga_failed_lookup_marker_*`.
- App validation routes these rows to failed output.
- Purpose: include controlled non-success rows with known behavior.

2. New trackers
- `tracker_name` is rewritten as `bench_new_<test_case>_<counter>`.
- Counter is monotonic per test case, so new keys are highly unique across files.
- Purpose: drive independent insert-heavy behavior and reduce accidental collisions.

3. Existing trackers
- Existing-key pool is built from **seed files only**.
- Pool is filtered to include only tracker names valid under ingestion validator rules.
- Medium cases (`T2`, `T3`) enforce no duplicate existing key within a file.
- For medium cases, if a file needs more unique existing rows than pool capacity, remaining rows are converted to `new`.
- Large and skewed cases (`T4`-`T7`) allow repeated existing keys within a file.
- Purpose: make existing rows correspond to preseeded DB keys while avoiding invalid seed keys and excessive artificial repetition.

### Why These Decisions Were Made

- Seed-only existing pool:
  - DB is reseeded from seed files before each run.
  - Using seed-only keys guarantees existing-intended keys can exist at run start.

- Validator-valid existing pool:
  - Some seed tracker names match blocked wildcard-prefix rules and would fail at ingestion.
  - Filtering them keeps intended failed ratio controlled.

- Unique new keys across files:
  - Better isolation of insert-heavy behavior.
  - Fewer accidental key conflicts.

- Hybrid uniqueness policy by scale:
  - Medium: unique existing keys within file to reduce artificial same-file repetition.
  - Large/skewed: repeated existing keys allowed to preserve existing-heavy workload identity when row counts greatly exceed unique seed-key capacity.
  - Medium shortfall only: overflow converts to new to preserve file size.

## Important Interpretation Note

Always report both:
- Intended composition (from manifest expected counts), and
- Realized composition (from run outputs: created/existing/failed totals).

Reason:
- Runtime behavior depends on ingestion rules and DB state transitions during concurrent execution.
- Realized counts are the authoritative basis for final performance interpretation.
