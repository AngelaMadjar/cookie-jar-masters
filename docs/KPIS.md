# KPIs
This document defines the KPI sets used for cross-experiment comparison (`E1`-`E4`).

- **Core KPIs**: used for performance and scalability conclusions (throughput, response time, run duration, queueing/processing behavior, completion rates).
- **Control KPIs**: used to validate workload realization (new/existing/failed row mix) before interpreting Core KPI differences.

## Core KPIs 
| # | KPI | Description | Formula | Calculated in |
|---|---|---|---|---|
| 1 | `files_per_sec` | **Troughput**: how many files are completed per second in a run. | `requests_ok / total_processing_time_sec` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 2 | `records_per_sec` | **Throughput**: how many rows (processed + failed) are handled per second in a run. | `processed_records_total / total_processing_time_sec`, where `processed_records_total = sum(processed_rows + failed_rows)` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 3 | `end_to_end_latency_sec` | **Response time** for one request, from request receipt to response completion. | `response_finished_timestamp - request_received_timestamp` | - E1,E2,E3: `orchestrator.py` |
| 4 | `end_to_end_latency_p95_sec` | Tail **response time**: the value under which 95% of request **response times** fall (slowest 5% are above it). | `p95(end_to_end_latency_sec)` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 5 | `total_processing_time_sec` | Total run duration (makespan), from first successful request start timestamp to last successful request finish timestamp. | `max(response_finished_timestamp) - min(request_received_timestamp)` over successful records (fallback: `sum(end_to_end_latency_sec)`) | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 6 | `processing_time_sec` | Active processing time for one request, excluding queue wait. | `processing_finished_timestamp - processing_start_timestamp` | - E1,E2,E3: `orchestrator.py` |
| 7 | `processing_time_p95_sec` | Tail processing time per request: 95th percentile of processing duration. | `p95(processing_time_sec)` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 8 | `queue_wait_time_sec` | Time a request spends waiting before active processing starts (queue/scheduling delay). | `processing_start_timestamp - request_received_timestamp` | - E1,E2,E3: `orchestrator.py` |
| 9 | `queue_wait_time_p95_sec` | Tail queueing delay per request: 95th percentile of queue wait time. | `p95(queue_wait_time_sec)` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 10 | `requests_total` | Total number of file requests sent/recorded for the run. |  | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 11 | `requests_ok` | Number of file requests that completed successfully (`HTTP 200` in run records). |  | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |
| 12 | `requests_failed` | Number of file requests that did not complete successfully. | `requests_total - requests_ok` | - E1,E2: `run_locust_benchmarks.py`<br>- E3: `run_upload_benchmarks.py` |

## Control KPIs 
| # | KPI | Description | Calculated in |
|---|---|---|---|
| 1 | `created_trackers_total` | Total number of newly inserted trackers across successful requests in a run (realized write intensity). | - E1,E2: `reporting_service.py`, `run_locust_benchmarks.py`<br>- E3: `reporting_service.py`, `run_upload_benchmarks.py` |
| 2 | `existing_trackers_total` | Total number of rows matched/resolved through the existing-tracker path across successful requests. | - E1,E2: `reporting_service.py`, `run_locust_benchmarks.py`<br>- E3: `reporting_service.py`, `run_upload_benchmarks.py` |
| 3 | `failed_rows_total` | Total number of rows routed to failed output across successful requests in a run. | - E1,E2: `reporting_service.py`, `run_locust_benchmarks.py`<br>- E3: `reporting_service.py`, `run_upload_benchmarks.py` |

*Note: for large and skewed existing-heavy workloads, `existing_trackers_total` may not match the nominal `80%` target due to limited unique seed data and resulting tracker-key duplication. Implications for existing trackers are explained in `TEST_CASES.md`.*
