#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from statistics import median
from urllib.parse import quote

import requests
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.protobuf.timestamp_pb2 import Timestamp

try:
    from google.cloud import monitoring_v3
except Exception:  # pragma: no cover
    monitoring_v3 = None
try:
    from google.cloud import logging_v2
except Exception:  # pragma: no cover
    logging_v2 = None

"""
E3 benchmark staging and aggregation runner.

This utility orchestrates one event-driven benchmark run by:
1. resetting and reseeding the database via app endpoints,
2. clearing runtime output and trigger input prefixes,
3. copying selected benchmark files into the Eventarc trigger bucket,
4. waiting for asynchronous processing completion,
5. reading per-attempt raw records,
6. aggregating file-level KPI outputs and writing run artifacts to GCS.

It is E3-specific and assumes:
- trigger bucket: e3-data-benchmarks
- runtime/results bucket: e3-data-monthly-audit-trackers
- source benchmark bucket: e2-data/benchmarks
"""

"""
Command:
gcloud run jobs execute cookie-jar-upload-benchmarks-e3 \
  --region=europe-west1 \
  --wait \
  --args="^|^tools/upload-benchmarks/run_upload_benchmarks.py|--host=https://cookie-jar-app-e2-656924888958.europe-west1.run.app|--test-case=T1"
"""

# E3 adaptation: runtime output and results artifacts live here.
RUNTIME_BUCKET = "e3-data-monthly-audit-trackers"
SOURCE_BENCHMARK_BUCKET = "e2-data" # benchmark source-of-truth remains in e2-data/benchmarks/<test_case_folder>/.
SOURCE_BENCHMARK_ROOT_PREFIX = "benchmarks" # uploads into this bucket/prefix are the event source for E3 ingestion.
TRIGGER_BUCKET = "e3-data-benchmarks" # run artifacts are written under e3 runtime bucket results/<test_case_folder>/.
RESULTS_BUCKET = "e3-data-monthly-audit-trackers"
DEFAULT_MONTH = "2026-02"
CLOUD_RUN_SERVICE_NAME = os.getenv("CLOUD_RUN_SERVICE_NAME", "cookie-jar-app-e3")
CLOUD_RUN_REGION = os.getenv("CLOUD_RUN_REGION", "europe-west1")

# E3 adaptation: benchmark bucket folder names are descriptive (T1_original, T2_medium_existing_heavy, ...).
CASE_FOLDER_BY_ID = {
    "T1": "T1_original",
    "T2": "T2_medium_existing_heavy",
    "T3": "T3_medium_new_heavy",
    "T4": "T4_large_existing_heavy",
    "T5": "T5_large_new_heavy",
    "T6": "T6_skewed_existing_heavy",
    "T7": "T7_skewed_new_heavy",
}
CASE_ID_BY_FOLDER = {folder: case_id for case_id, folder in CASE_FOLDER_BY_ID.items()}

_STORAGE_CLIENT: storage.Client | None = None
_MONITORING_CLIENT = None
_LOGGING_CLIENT = None
_ID_TOKEN_CACHE: dict[str, str] = {}
READ_RETRY_ATTEMPTS = 6
READ_RETRY_SLEEP_SEC = 2


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser for one E3 benchmark run."""
    p = argparse.ArgumentParser(description="Stage E3 benchmark files and collect run artifacts.")
    p.add_argument("--host", default=os.getenv("APP_HOST", ""), help="E3 ingestion app base URL")
    p.add_argument("--test-case", required=True, choices=sorted(set(CASE_FOLDER_BY_ID) | set(CASE_ID_BY_FOLDER)))
    p.add_argument("--month", default=DEFAULT_MONTH)
    p.add_argument("--total-files", type=int, default=80)
    p.add_argument("--wait-timeout-sec", type=int, default=5400)
    p.add_argument("--poll-interval-sec", type=int, default=10)
    p.add_argument("--run-id", default="")
    return p


def _now_utc() -> str:
    """Return current UTC timestamp in compact run-id friendly format."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_case_identity(test_case_arg: str) -> tuple[str, str]:
    """Normalize user input test case into `(test_case_id, test_case_folder)`."""
    # E3 adaptation: allow either short test ids or descriptive folder names while keeping a canonical mapping.
    if test_case_arg in CASE_FOLDER_BY_ID:
        return test_case_arg, CASE_FOLDER_BY_ID[test_case_arg]
    if test_case_arg in CASE_ID_BY_FOLDER:
        return CASE_ID_BY_FOLDER[test_case_arg], test_case_arg
    raise ValueError(f"Unsupported test case: {test_case_arg}")


def _percentile(values: list[float], p: float) -> float:
    """Return rounded-index percentile from a numeric list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * p))
    return float(ordered[idx])


def _parse_utc_iso(ts: str | None) -> datetime | None:
    """Parse ISO timestamp into timezone-aware datetime, or None on invalid input."""
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _basename_from_file_path(file_path: str | None) -> str:
    """Extract filename from a path-like string."""
    if not file_path or not isinstance(file_path, str):
        return ""
    return file_path.rsplit("/", 1)[-1]


def _attempt_sort_key(record: dict) -> tuple[str, str, str]:
    """Build deterministic sort key for per-file attempt ordering."""
    received = str(record.get("request_received_timestamp") or "")
    finished = str(record.get("response_finished_timestamp") or "")
    processing = str(record.get("processing_finished_timestamp") or "")
    return (received, finished, processing)


def _group_records_by_file(records: list[dict]) -> dict[str, list[dict]]:
    """Group raw attempt records by filename and sort attempts chronologically."""
    grouped: dict[str, list[dict]] = {}
    for rec in records:
        filename = _basename_from_file_path(rec.get("file_path"))
        if not filename:
            continue
        grouped.setdefault(filename, []).append(rec)
    for filename in list(grouped.keys()):
        grouped[filename] = sorted(grouped[filename], key=_attempt_sort_key)
    return grouped


def _finalize_file_outcomes(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Convert attempt-level raw records into file-level final outcomes.

    Returns:
    - final_success_records: one successful terminal record per filename
    - final_failed_records: one terminal failed record per filename

    Why this is needed in E3:
    - Eventarc is at-least-once; one file can have multiple delivery attempts.
    - For comparability with E1/E2, request success/failure KPIs must be file-level,
      while final request outcome stays file-level.
    """
    grouped = _group_records_by_file(records)
    final_success_records: list[dict] = []
    final_failed_records: list[dict] = []

    for _, attempts in grouped.items():
        first_success_idx = None
        for idx, rec in enumerate(attempts):
            if rec.get("ok") is True:
                first_success_idx = idx
                break

        if first_success_idx is not None:
            final_success_records.append(attempts[first_success_idx])
        else:
            final_failed_records.append(attempts[-1])

    return final_success_records, final_failed_records


def _gcp_project_id() -> str | None:
    """Resolve current GCP project id from known environment variable names."""
    return os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or os.getenv("GCLOUD_PROJECT")


def _to_proto_timestamp(dt: datetime) -> Timestamp:
    """Convert datetime into protobuf Timestamp."""
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def storage_client() -> storage.Client:
    """Get cached GCS client."""
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def monitoring_client():
    """Get cached Cloud Monitoring client when available, otherwise None."""
    global _MONITORING_CLIENT
    if monitoring_v3 is None:
        return None
    if _MONITORING_CLIENT is None:
        _MONITORING_CLIENT = monitoring_v3.MetricServiceClient()
    return _MONITORING_CLIENT


def logging_client():
    """Get cached Cloud Logging client when available, otherwise None."""
    global _LOGGING_CLIENT
    if logging_v2 is None:
        return None
    if _LOGGING_CLIENT is None:
        _LOGGING_CLIENT = logging_v2.Client()
    return _LOGGING_CLIENT


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Parse `gs://bucket/blob` into `(bucket, blob)`."""
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// path, got: {gs_uri}")
    raw = gs_uri[len("gs://") :]
    bucket, sep, blob = raw.partition("/")
    if not sep or not bucket or not blob:
        raise ValueError(f"Invalid gs:// path: {gs_uri}")
    return bucket, blob


def download_text_gs(gs_uri: str) -> str:
    """Download text content from a GCS URI."""
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_text()


def upload_text_gs(gs_uri: str, content: str, content_type: str):
    """Upload text content to a GCS URI with the provided content type."""
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    storage_client().bucket(bucket_name).blob(blob_name).upload_from_string(content, content_type=content_type)


def list_blobs(bucket_name: str, prefix: str):
    """List GCS blobs for a bucket/prefix."""
    return storage_client().list_blobs(bucket_name, prefix=prefix)


def count_blobs(bucket_name: str, prefix: str) -> int:
    """Count blobs under a bucket/prefix."""
    return sum(1 for _ in list_blobs(bucket_name, prefix))


def delete_prefix(bucket_name: str, prefix: str) -> int:
    """Delete all blobs under a bucket/prefix and return deleted count."""
    deleted = 0
    bucket = storage_client().bucket(bucket_name)
    for blob in list_blobs(bucket_name, prefix):
        bucket.blob(blob.name).delete()
        deleted += 1
    return deleted


def _cloud_run_id_token(audience: str) -> str:
    """Fetch and cache an ID token for calling private Cloud Run endpoints."""
    cached = _ID_TOKEN_CACHE.get(audience)
    if cached:
        return cached
    metadata_url = (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
        f"?audience={quote(audience, safe='')}"
    )
    resp = requests.get(metadata_url, headers={"Metadata-Flavor": "Google"}, timeout=30)
    resp.raise_for_status()
    token = resp.text.strip()
    _ID_TOKEN_CACHE[audience] = token
    return token


def _cloud_run_auth_headers(host: str) -> dict[str, str]:
    """Build Authorization headers for private Cloud Run HTTP calls."""
    token = _cloud_run_id_token(host.rstrip("/"))
    return {"Authorization": f"Bearer {token}"}


def _post_ok(host: str, endpoint: str, payload: dict):
    """POST JSON to app endpoint and raise on non-200 response."""
    resp = requests.post(
        f"{host}{endpoint}",
        json=payload,
        headers=_cloud_run_auth_headers(host),
        timeout=1800,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"{endpoint} failed: {resp.status_code} {resp.text}")


def reset_and_seed_db(host: str):
    """Reset and reseed DB state through app endpoints."""
    print("Step 1/6: Reset and reseed DB")
    _post_ok(host, "/db/empty", {})
    _post_ok(host, "/db/populate/cmp", {})
    _post_ok(host, "/db/populate/trackers", {})


def clear_runtime_prefixes(month: str):
    """
    Clear runtime monthly output prefixes in GCS.

    E3 note:
    - source input for event ingestion is in trigger bucket
      gs://e3-data-benchmarks/<test_case>/input/
    - monthly runtime bucket is used for processed/failed outputs and results
    """
    print("Step 2/6: Clear runtime processed/failed prefixes")
    for prefix in (f"processed/{month}/", f"failed/{month}/"):
        deleted = delete_prefix(RUNTIME_BUCKET, prefix)
        print(f"  Cleared {deleted} objects from gs://{RUNTIME_BUCKET}/{prefix}")


def clear_trigger_input_prefix(test_case_folder: str):
    """Clear trigger bucket input prefix for one test case."""
    # E3 adaptation: remove prior staged files so one upload job execution represents one clean benchmark run.
    trigger_prefix = f"{test_case_folder}/input/"
    deleted = delete_prefix(TRIGGER_BUCKET, trigger_prefix)
    print(f"  Cleared {deleted} objects from gs://{TRIGGER_BUCKET}/{trigger_prefix}")


def stage_files(test_case_id: str, test_case_folder: str, run_id: str, month: str, total_files: int) -> list[str]:
    """
    Copy benchmark source CSV files into the trigger bucket input prefix.

    Uploaded objects include metadata needed by `/scan/ingest` normalization.
    Returns staged `gs://...` file paths.
    """
    print("Step 3/6: Copy test-case inputs from e2 benchmark bucket to e3 trigger bucket")
    source_prefix = f"{SOURCE_BENCHMARK_ROOT_PREFIX}/{test_case_folder}/input/"
    source_blobs = [b for b in list_blobs(SOURCE_BENCHMARK_BUCKET, source_prefix) if b.name.endswith(".csv")]
    source_blobs.sort(key=lambda b: b.name)
    if len(source_blobs) < total_files:
        raise ValueError(
            f"Source input contains only {len(source_blobs)} csv files under gs://{SOURCE_BENCHMARK_BUCKET}/{source_prefix}, "
            f"expected at least {total_files}"
        )

    selected = source_blobs[:total_files]
    dst_bucket = storage_client().bucket(TRIGGER_BUCKET)
    staged = []
    src_bucket = storage_client().bucket(SOURCE_BENCHMARK_BUCKET)
    for src_blob in selected:
        filename = src_blob.name.rsplit("/", 1)[-1]
        dst_blob_name = f"{test_case_folder}/input/{filename}"
        # E3 adaptation: copy source benchmark files into an explicit input/
        # prefix so runtime flow mirrors the local lifecycle semantics where
        # ingestion consumes from input and transitions files to processed/failed.
        # E3 adaptation: upload destination objects with metadata in the same write operation so finalize events carry run identity without patch races.
        payload = src_blob.download_as_bytes()
        target_blob = dst_bucket.blob(dst_blob_name)
        target_blob.metadata = {
            "test_case": test_case_id,
            "test_case_folder": test_case_folder,
            "run_id": run_id,
            "month": month,
        }
        target_blob.upload_from_string(payload, content_type=(src_blob.content_type or "text/csv"))
        staged.append(f"gs://{TRIGGER_BUCKET}/{dst_blob_name}")

    print(f"  Copied {len(staged)} files into gs://{TRIGGER_BUCKET}/{test_case_folder}/input/")
    return staged


def wait_until_complete(
    test_case_folder: str,
    run_id: str,
    month: str,
    staged_count: int,
    timeout_sec: int,
    poll_interval_sec: int,
):
    """
    Poll asynchronous completion until all staged files are successfully processed.

    Completion condition:
    - trigger input prefix is empty, and
    - final successful file outcomes reach staged file count.
    """
    print("Step 4/6: Wait for event-driven processing completion")
    start = time.time()
    raw_prefix = f"results/{test_case_folder}/raw/{run_id}/"
    trigger_prefix = f"{test_case_folder}/input/"
    while True:
        input_remaining = count_blobs(TRIGGER_BUCKET, trigger_prefix)
        raw_records_count = count_blobs(RESULTS_BUCKET, raw_prefix)
        records = read_run_records(test_case_folder, run_id)
        final_success_records, _ = _finalize_file_outcomes(records)
        final_success_files = len(final_success_records)
        elapsed = int(time.time() - start)
        print(
            f"  elapsed={elapsed}s input_remaining={input_remaining} raw_records={raw_records_count} final_success_files={final_success_files}/{staged_count}"
        )

        # E3 adaptation: run is complete only when all input work units are
        # consumed and we have one successful final outcome per staged file.
        if input_remaining == 0 and final_success_files >= staged_count:
            return

        if elapsed >= timeout_sec:
            raise TimeoutError(
                "Timed out waiting for processing completion "
                f"(input_remaining={input_remaining}, raw_records={raw_records_count}, final_success_files={final_success_files}, staged={staged_count})"
            )
        time.sleep(max(1, poll_interval_sec))


def _download_blob_text_with_retry(bucket_name: str, blob_name: str) -> str:
    """Download GCS object text with retry to tolerate transient listing/read races."""
    # E3 adaptation: raw blob generations can be replaced by duplicate event retries, so re-read latest object on transient 404.
    last_exc: Exception | None = None
    for attempt in range(1, READ_RETRY_ATTEMPTS + 1):
        try:
            return storage_client().bucket(bucket_name).blob(blob_name).download_as_text()
        except NotFound as exc:
            last_exc = exc
            if attempt >= READ_RETRY_ATTEMPTS:
                break
            time.sleep(READ_RETRY_SLEEP_SEC)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Failed to read gs://{bucket_name}/{blob_name}")


def read_run_records(test_case_folder: str, run_id: str) -> list[dict]:
    """Read all raw JSON attempt records for one run from GCS."""
    prefix = f"results/{test_case_folder}/raw/{run_id}/"
    records = []
    blob_names = sorted(
        blob.name for blob in list_blobs(RESULTS_BUCKET, prefix) if blob.name.lower().endswith(".json")
    )
    for blob_name in blob_names:
        payload = json.loads(_download_blob_text_with_retry(RESULTS_BUCKET, blob_name))
        records.append(payload)
    return records


def cloud_run_max_active_instances_observed(records: list[dict]) -> int | None:
    """
    Query Cloud Monitoring and return max active Cloud Run instances observed
    during the run time window.
    """
    if monitoring_v3 is None:
        return None

    project_id = _gcp_project_id()
    if not project_id:
        return None

    starts = [_parse_utc_iso(r.get("request_received_timestamp")) for r in records]
    finishes = [_parse_utc_iso(r.get("response_finished_timestamp")) for r in records]
    starts = [x for x in starts if x is not None]
    finishes = [x for x in finishes if x is not None]
    if not starts or not finishes:
        return None

    start_dt = min(starts)
    end_dt = max(finishes)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(seconds=1)
    end_dt = end_dt + timedelta(seconds=120)

    try:
        req = monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project_id}",
            filter=(
                'metric.type = "run.googleapis.com/container/instance_count" '
                'AND resource.type = "cloud_run_revision" '
                f'AND resource.labels.service_name = "{CLOUD_RUN_SERVICE_NAME}" '
                f'AND resource.labels.location = "{CLOUD_RUN_REGION}" '
                'AND metric.labels.state = "active"'
            ),
            interval=monitoring_v3.TimeInterval(
                start_time=_to_proto_timestamp(start_dt),
                end_time=_to_proto_timestamp(end_dt),
            ),
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        )
        client = monitoring_client()
        if client is None:
            return None
        series = client.list_time_series(request=req)

        max_seen: float | None = None
        for ts in series:
            for pt in ts.points:
                value = None
                if pt.value.int64_value is not None:
                    value = float(pt.value.int64_value)
                elif pt.value.double_value is not None:
                    value = float(pt.value.double_value)
                if value is None:
                    continue
                if max_seen is None or value > max_seen:
                    max_seen = value

        return int(max_seen) if max_seen is not None else None
    except Exception:
        return None


def cloud_run_platform_429_count(records: list[dict]) -> int | None:
    """
    Count platform-level Cloud Run HTTP 429 responses for /scan/ingest during the run window.

    This captures delivery attempts rejected before app-level handler execution,
    which are not represented by per-attempt raw records written in-route.
    """
    project_id = _gcp_project_id()
    if not project_id:
        return None

    starts = [_parse_utc_iso(r.get("request_received_timestamp")) for r in records]
    finishes = [_parse_utc_iso(r.get("response_finished_timestamp")) for r in records]
    starts = [x for x in starts if x is not None]
    finishes = [x for x in finishes if x is not None]
    if not starts or not finishes:
        return None

    # Expand window to include early rejected attempts before first successful in-app record.
    start_dt = min(starts) - timedelta(minutes=10)
    end_dt = max(finishes) + timedelta(minutes=5)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(minutes=1)

    client = logging_client()
    if client is None:
        return None

    flt = (
        'resource.type="cloud_run_revision"\n'
        f'resource.labels.service_name="{CLOUD_RUN_SERVICE_NAME}"\n'
        f'resource.labels.location="{CLOUD_RUN_REGION}"\n'
        f'httpRequest.requestMethod="POST"\n'
        'httpRequest.status=429\n'
        'httpRequest.requestUrl:"/scan/ingest"\n'
        f'timestamp>="{start_dt.isoformat().replace("+00:00", "Z")}"\n'
        f'timestamp<="{end_dt.isoformat().replace("+00:00", "Z")}"'
    )

    try:
        count = 0
        for _ in client.list_entries(
            resource_names=[f"projects/{project_id}"],
            filter_=flt,
            order_by=logging_v2.DESCENDING,
        ):
            count += 1
        return int(count)
    except Exception:
        return None


def aggregate_run_metrics(records: list[dict], run_id: str, test_case_id: str) -> dict:
    """
    Aggregate attempt-level raw records into file-level benchmark KPIs.

    Includes platform-level 429 count from Cloud Logging.
    """
    ok_records, final_failed_records = _finalize_file_outcomes(records)

    queue_wait = [float(r.get("queue_wait_time_sec", 0) or 0) for r in ok_records]
    processing = [float(r.get("processing_time_sec", 0) or 0) for r in ok_records]
    e2e = [float(r.get("end_to_end_latency_sec", 0) or 0) for r in ok_records]
    received_ts = [r.get("request_received_timestamp") for r in ok_records if r.get("request_received_timestamp")]
    finished_ts = [r.get("response_finished_timestamp") for r in ok_records if r.get("response_finished_timestamp")]

    makespan_sec = 0.0
    if received_ts and finished_ts:
        start = min(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in received_ts)
        end = max(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in finished_ts)
        makespan_sec = (end - start).total_seconds()
    elif e2e:
        makespan_sec = sum(e2e)

    files_count = len(ok_records)
    files_failed_final = len(final_failed_records)
    records_count = int(sum(int(r.get("processed_rows", 0) or 0) + int(r.get("failed_rows", 0) or 0) for r in ok_records))
    created_total = int(sum(int(r.get("created_trackers", 0) or 0) for r in ok_records))
    existing_total = int(sum(int(r.get("existing_trackers", 0) or 0) for r in ok_records))
    failed_total = int(sum(int(r.get("failed_rows", 0) or 0) for r in ok_records))
    active_parallel = [int(r.get("active_processing_requests", 0) or 0) for r in ok_records]
    max_active_seen = [int(r.get("max_active_requests_seen", 0) or 0) for r in ok_records]

    files_per_sec = (files_count / makespan_sec) if makespan_sec > 0 else 0.0
    records_per_sec = (records_count / makespan_sec) if makespan_sec > 0 else 0.0
    files_succeeded_final = files_count
    platform_429_count = cloud_run_platform_429_count(records)

    return {
        "run_id": run_id,
        "test_case": test_case_id,
        "repeat_index": 1,
        "locust_exit_code": 0 if files_failed_final == 0 else 1,
        "requests_total": files_succeeded_final + files_failed_final,
        "requests_ok": files_succeeded_final,
        "requests_failed": files_failed_final,
        "total_processing_time_sec": round(makespan_sec, 6),
        "files_per_sec": round(files_per_sec, 6),
        "records_per_sec": round(records_per_sec, 6),
        "processing_time_p50_sec": round(float(median(processing)) if processing else 0.0, 6),
        "processing_time_p95_sec": round(_percentile(processing, 0.95), 6),
        "end_to_end_latency_p95_sec": round(_percentile(e2e, 0.95), 6),
        "queue_wait_time_p95_sec": round(_percentile(queue_wait, 0.95), 6),
        "created_trackers_total": created_total,
        "existing_trackers_total": existing_total,
        "failed_rows_total": failed_total,
        "max_active_processing_requests_observed": int(max(active_parallel) if active_parallel else 0),
        "max_active_requests_seen_observed": int(max(max_active_seen) if max_active_seen else 0),
        "cloud_run_max_active_instances_observed": cloud_run_max_active_instances_observed(records),
        "platform_429_count": int(platform_429_count) if platform_429_count is not None else "",
    }


def write_outputs(test_case_folder: str, run_id: str, month: str, records: list[dict], run_row: dict):
    """Write raw run bundle JSON and aggregated CSV row artifacts to GCS."""
    print("Step 5/6: Write run artifacts")
    raw_bundle_uri = f"gs://{RESULTS_BUCKET}/results/{test_case_folder}/raw/{run_id}.json"
    upload_text_gs(
        raw_bundle_uri,
        json.dumps(
            {
                "test_case": run_row.get("test_case"),
                "run_id": run_id,
                "month": month,
                "requests_total": len(records),
                "records": records,
            },
            indent=2,
        ),
        content_type="application/json",
    )

    csv_uri = f"gs://{RESULTS_BUCKET}/results/{test_case_folder}/aggregated/{run_id}.csv"
    fields = list(run_row.keys())
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    writer.writerow(run_row)
    upload_text_gs(csv_uri, out.getvalue(), content_type="text/csv")

    print(f"  Raw bundle: {raw_bundle_uri}")
    print(f"  Aggregated row: {csv_uri}")


def main():
    """CLI entrypoint for one end-to-end E3 benchmark run."""
    args = build_parser().parse_args()
    if not args.host:
        raise ValueError("--host is required (or set APP_HOST env var)")
    if args.total_files <= 0:
        raise ValueError("--total-files must be > 0")

    run_id = args.run_id.strip() or f"run_{_now_utc()}"
    # E3 adaptation: normalize user input into short-id + folder-name pair for consistent manifests/results routing.
    test_case_id, test_case_folder = resolve_case_identity(args.test_case)

    reset_and_seed_db(args.host)
    clear_runtime_prefixes(args.month)
    clear_trigger_input_prefix(test_case_folder)
    staged_files = stage_files(test_case_id, test_case_folder, run_id, args.month, args.total_files)
    wait_until_complete(
        test_case_folder=test_case_folder,
        run_id=run_id,
        month=args.month,
        staged_count=len(staged_files),
        timeout_sec=args.wait_timeout_sec,
        poll_interval_sec=args.poll_interval_sec,
    )

    print("Step 6/6: Aggregate run metrics")
    records = read_run_records(test_case_folder, run_id)
    run_row = aggregate_run_metrics(records=records, run_id=run_id, test_case_id=test_case_id)
    write_outputs(test_case_folder=test_case_folder, run_id=run_id, month=args.month, records=records, run_row=run_row)
    print("Done.")


if __name__ == "__main__":
    main()
