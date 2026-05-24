# Experiments Guide

This file documents experiment-level behavior and interpretation notes.
It currently focuses on E3 caveats that are important for scientific reporting.

## Scope

- `E1`: local HTTP burst benchmark (baseline)
- `E2`: Cloud Run HTTP invoker benchmark
- `E3`: Cloud Run + Eventarc object-finalized benchmark (this document focus)

## E3 Runtime Model (What Is Different)

In E3, files are not submitted by a synchronous load-testing client.
Instead:

1. benchmark files are copied to `gs://e3-data-benchmarks/<test_case>/input/...`
2. each object-finalized event triggers `POST /scan/ingest` on Cloud Run
3. ingestion writes processed/failed outputs to `gs://e3-data-monthly-audit-trackers/...`
4. source trigger object is deleted only after successful persistence

This means E3 is event-driven and at-least-once delivery, unlike E1/E2 direct request dispatch.

## Why Files Are Copied Into `input/` In E3

Files are staged into an explicit `input/` prefix to preserve the same lifecycle concept used in E1/E2:

- `input -> processed/failed -> consumed`

This keeps the dataflow readable and comparable at a process level, even though transport is event-driven.

## Why Source Trigger Objects Are Deleted On Success

In E3 ingestion, source files are deleted only after processed/failed artifacts are written.

Reason:

- deleting before persistence creates a loss window if processing crashes mid-flight
- deleting after persistence gives consume-on-success semantics
- duplicate Eventarc deliveries then hit missing source and are safely acknowledged as skipped

This is the idempotency anchor for E3.

## E3 KPI Semantics

Per-invocation timing fields are still measured in the `/scan/ingest` handler, same measurement boundary as E2:

- `request_received_timestamp`
- `processing_start_timestamp`
- `processing_finished_timestamp`
- `response_finished_timestamp`

Derived:

- `queue_wait_time_sec`
- `processing_time_sec`
- `end_to_end_latency_sec`

## E3 Additional Reliability KPIs

Because Eventarc can deliver multiple attempts for one file, E3 adds:

- `files_succeeded_final`: unique files with final successful outcome
- `files_failed_final`: unique files without successful outcome by run end/timeout
- `attempt_failures_transient`: failed attempts that later succeeded

These separate business completion from delivery/retry noise.

## Completion Rule Used For E3 Aggregation

E3 summary should be written only when both are true:

1. trigger input prefix is empty (`gs://e3-data-benchmarks/<test_case>/input/`)
2. successful final file outcomes reach staged file count

This prevents early aggregation while retries are still in progress.

## Comparability Caveats vs E1/E2

E3 is scientifically comparable to E1/E2 at application timing boundary, but not transport-identical.

Important caveats to state in the report:

- E1/E2 are client-request driven.
- E3 is event-delivery driven (at-least-once).
- transient failures in E3 may later recover via retry.
- therefore, E3 must report final file outcomes and transient attempt failures separately.

## Recommended Reporting Language

When presenting E3 results, explicitly state:

- main performance KPIs are computed from final successful file outcomes
- retry behavior is reported via E3 reliability KPIs
- E3 includes delivery semantics overhead not present in direct-invocation models

This keeps interpretation honest and methodologically clear.
