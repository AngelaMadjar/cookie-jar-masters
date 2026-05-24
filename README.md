# Cookie Jar Masters

Cookie Jar Masters is a file-ingestion benchmark project built around a real digital-marketing use case.  
It tests how a tracker-processing service behaves under controlled burst load as workload shape and file size change.

## Project Context (Business + Research)

This project is inspired by a GDPR transparency workflow. In practice, CMP scan files contain trackers detected on websites, and those trackers must be enriched with metadata so website users can review what data is collected.

In the original business process, monthly tracker scans arrive per domain. The ingestion service cross-references scan records against preseeded tracker data:
- if a tracker already exists, existing metadata is reused,
- if a tracker is new, it is inserted and can later be manually completed in a UI workflow.

This class project narrows scope to one technical slice: **ingesting and cross-referencing tracker scan files for performance/scalability benchmarking**.

All client-identifying names in files were anonymized. Any original client token is replaced by `CLIENT_X`.

## What the Service Does

The main endpoint is:

```http
POST /scan/ingest
```

One request processes exactly one scan file path.  
At row level:
- valid rows go through reference resolution and tracker existing/new handling,
- invalid rows are routed to failed output (row-level failure, not file-level failure).

File lifecycle is simple:
- file starts in `input/`,
- processed rows are written to `processed/`,
- failed rows are written to `failed/`,
- input file is removed after processing.

## Tech Stack and Architecture
- Backend: Flask
- Persistence: SQLAlchemy ORM
- Database: PostgreSQL
- Schema migrations: Alembic (Flask-Migrate)
- App server: Gunicorn

### Code Organization (Onion Style)
- `models/`: table schema and relationships
- `daos/`: database operations (queries, inserts, updates, associations)
- `services/`: business flow and orchestration
- `routes/`: HTTP API layer and request validation

### Ingestion Flow (High-Level)
- `IngestionService`: reads file, normalizes data, splits valid/invalid rows
- `ReferenceDataService`: resolves and prepares lookup/reference mappings
- `TrackerUpsertService`: matches existing trackers and inserts missing ones
- `PurposeService`: creates purpose links when purpose values are present
- `ReportingService`: builds output frames and count summaries
- `ScanLifecycleService`: writes processed/failed outputs and finalizes file lifecycle

## Fixed Variables and Rationale

To keep experiments comparable, runtime controls were fixed across test runs:
- local app concurrency limit: `8`
- Gunicorn threads: `8` (single worker)
- SQLAlchemy pool: `pool_size=8`, `max_overflow=0`
- burst shape: `80` concurrent incoming requests, one file per request

Rationale:
- `workers=1` and `threads=8` were selected empirically for the local benchmark and then held fixed as the baseline for all benchmark runs.
- This gives bounded parallelism while still producing measurable queue/wait behavior under an 80-request burst.
- Keeping these values fixed preserves controlled KPI comparison across workload shapes (`T1`-`T7`).
- Higher thread counts (for example, `10` or `12`) are valid alternatives, but they increase lock-contention noise.


## Data Layout and Ownership

`data/` contains all project datasets and benchmark artifacts.

- `data/seed/`  
  Source-of-truth seed CSVs used to initialize baseline DB state.

- `data/benchmarks/`  
  Benchmark case folders (`T1`–`T7`) with:
  - `input/`: benchmark files used as source workload,
  - `manifest/`: test-case file lists and expected counts,
  - `locust_results/`: per-case benchmark outputs (raw + aggregated).

- `data/monthly_tracker_audits/`  
  Runtime ingestion lifecycle data:
  - `input/<month>/`: files waiting to be processed,
  - `processed/<month>/`: processed-row outputs,
  - `failed/<month>/`: validation-failed-row outputs.

## Scripts Catalog (Project Utilities)

- `scripts/generate_benchmark_workloads.py`  
  Used to generate synthetic benchmark datasets (`T2`–`T7`) and manifests from `T1` baseline inputs.

- `scripts/anonymize_client_files.py`  
  Used to replace client-identifying names in file content and file names with `CLIENT_X`.

- `scripts/remove_scan_purpose_vendor_fields.py`  
  Used to clear purpose/description fields in scan inputs and keep benchmark focus on tracker cross-referencing behavior.

## Where to Read Next

This README is project-level context.

- For test-case design and generation details  
  See `TEST_CASES.md` (create/maintain this as the detailed test-case spec).

- For measured KPIs details
  See `KPIS.md`
  
- For experiment definitions, setup, and interpretation  
  See `EXPERIMENTS.md` (create/maintain this as the detailed experiment spec).
