#!/usr/bin/env python3
from __future__ import annotations

"""
E4 Cloud Tasks benchmark runner.

This script executes benchmark runs for T1-T7 by orchestrating the full flow:
1. reset + reseed database through app endpoints,
2. clear runtime GCS monthly folders,
3. stage benchmark files from benchmark/input into runtime input,
4. enqueue one Cloud Task per file (each task invokes /scan/ingest),
5. wait until runtime input is drained,
6. finalize per-file outcomes from attempt-level raw records, then write per-run
   raw JSON + aggregated CSV into GCS results folders.

Unlike E2, this script does not send synchronous HTTP burst traffic itself.
It measures the Cloud Tasks dispatch model by enqueueing work, collecting
attempt-level ingest records, and finalizing file outcomes similarly to E3.
"""

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from statistics import median
from urllib.parse import quote

import requests
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud import tasks_v2

CASE_TO_FOLDER = {
    "T1": "T1_original",
    "T2": "T2_medium_existing_heavy",
    "T3": "T3_medium_new_heavy",
    "T4": "T4_large_existing_heavy",
    "T5": "T5_large_new_heavy",
    "T6": "T6_skewed_existing_heavy",
    "T7": "T7_skewed_new_heavy",
}

_STORAGE_CLIENT: storage.Client | None = None
_TASKS_CLIENT: tasks_v2.CloudTasksClient | None = None
_ID_TOKEN_CACHE: dict[str, str] = {}

def build_parser() -> argparse.ArgumentParser:
    """Define CLI arguments for running E4 benchmark cases and repeats."""
    p = argparse.ArgumentParser(description="Run E4 Cloud Tasks benchmark for T1-T7.")
    p.add_argument("--host", required=True, help="Cloud Run app base URL (for example https://cookie-jar-app-e4-....run.app)")
    p.add_argument("--endpoint", default="/scan/ingest", help="Target endpoint path that each task will invoke")
    p.add_argument("--month", default="2026-02")
    p.add_argument("--test-cases", default="T1,T2,T3,T4,T5,T6,T7")
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--total-requests", type=int, default=80)
    p.add_argument("--benchmark-bucket", default="e4-data")
    p.add_argument("--runtime-bucket", default="e4-data")
    p.add_argument("--queue-project", default=os.getenv("GOOGLE_CLOUD_PROJECT", "dept-dinl-angela"))
    p.add_argument("--queue-location", default="europe-west1")
    p.add_argument("--queue-name", default="cookie-jar-e4-queue")
    p.add_argument(
        "--oidc-service-account-email",
        default="angela-masters@dept-dinl-angela.iam.gserviceaccount.com",
    )
    p.add_argument("--poll-interval-sec", type=int, default=5)
    p.add_argument("--wait-timeout-sec", type=int, default=7200)
    p.add_argument("--results-dir", default="", help="Optional override gs://... results root")
    return p


def _now_utc() -> str:
    """Return a UTC timestamp token used in stable run IDs."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _split_csv(text: str) -> list[str]:
    """Split comma-separated CLI values into a cleaned list."""
    return [x.strip() for x in text.split(",") if x.strip()]


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Parse a gs:// URI into (bucket, blob_name)."""
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// path, got: {gs_uri}")
    raw = gs_uri[len("gs://") :]
    bucket, sep, blob = raw.partition("/")
    if not sep or not bucket or not blob:
        raise ValueError(f"Invalid gs:// path: {gs_uri}")
    return bucket, blob


def storage_client() -> storage.Client:
    """Return singleton GCS client used for all storage operations."""
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def tasks_client() -> tasks_v2.CloudTasksClient:
    """Return singleton Cloud Tasks client used for enqueue operations."""
    global _TASKS_CLIENT
    if _TASKS_CLIENT is None:
        _TASKS_CLIENT = tasks_v2.CloudTasksClient()
    return _TASKS_CLIENT


def upload_text_gs(gs_uri: str, content: str, content_type: str = "application/json"):
    """Upload text content to a GCS object."""
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    storage_client().bucket(bucket_name).blob(blob_name).upload_from_string(content, content_type=content_type)


def download_text_gs(gs_uri: str) -> str:
    """Download object text content from GCS."""
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_text()


def download_text_gs_optional(gs_uri: str) -> str | None:
    """Download object text if present, else return None."""
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    blob = storage_client().bucket(bucket_name).blob(blob_name)
    try:
        return blob.download_as_text()
    except NotFound:
        return None


def delete_prefix_gs(bucket_name: str, prefix: str) -> int:
    """Delete all objects under a bucket prefix and return deleted count."""
    bucket = storage_client().bucket(bucket_name)
    deleted = 0
    for blob in storage_client().list_blobs(bucket, prefix=prefix):
        blob.delete()
        deleted += 1
    return deleted


def count_csv_objects(bucket_name: str, prefix: str) -> int:
    """Count CSV objects under a bucket prefix."""
    bucket = storage_client().bucket(bucket_name)
    total = 0
    for blob in storage_client().list_blobs(bucket, prefix=prefix):
        if blob.name.endswith(".csv"):
            total += 1
    return total


def _cloud_run_id_token(audience: str) -> str:
    """Fetch and cache an ID token from metadata server for Cloud Run auth."""
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
    """Build Authorization headers for calling private Cloud Run endpoints."""
    token = _cloud_run_id_token(host.rstrip("/"))
    return {"Authorization": f"Bearer {token}"}


def _post_ok(host: str, endpoint: str, payload: dict):
    """POST JSON payload to app endpoint and fail if status is not 200."""
    print(f"  -> POST {endpoint}")
    resp = requests.post(
        f"{host}{endpoint}",
        json=payload,
        headers=_cloud_run_auth_headers(host),
        timeout=1800,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"{endpoint} failed: {resp.status_code} {resp.text}")


def reset_and_seed_db(host: str):
    """Reset DB state and reseed CMP + tracker data before each run."""
    print("Step 1/5: Reset and reseed database")
    _post_ok(host, "/db/empty", {})
    _post_ok(host, "/db/populate/cmp", {})
    _post_ok(host, "/db/populate/trackers", {})
    print("  DB reset + seed completed.")


def clear_month_dirs(runtime_bucket: str, month: str):
    """Clear runtime input/processed/failed GCS folders for selected month."""
    print(f"Step 2/5: Clear monthly runtime prefixes in GCS for {month}")
    targets = [
        f"monthly_tracker_audits/input/{month}/",
        f"monthly_tracker_audits/processed/{month}/",
        f"monthly_tracker_audits/failed/{month}/",
    ]
    for prefix in targets:
        removed = delete_prefix_gs(runtime_bucket, prefix)
        print(f"  Cleared {removed} objects from gs://{runtime_bucket}/{prefix}")


def load_manifest(benchmark_bucket: str, case: str) -> list[str]:
    """Load benchmark manifest and return generated file paths for a test case."""
    case_folder = CASE_TO_FOLDER[case]
    manifest_path = f"gs://{benchmark_bucket}/benchmarks/{case_folder}/manifest/manifest.json"
    payload = json.loads(download_text_gs(manifest_path))
    paths = payload.get("generated_file_paths") or []
    if not paths:
        raise ValueError(f"No generated_file_paths in manifest: {manifest_path}")
    return [str(p) for p in paths]


def stage_files(file_paths: list[str], runtime_bucket: str, month: str, total_requests: int) -> list[str]:
    """Copy selected benchmark files into runtime input and return staged gs:// paths."""
    print(f"Step 3/5: Stage {total_requests} files into runtime input")
    if len(file_paths) < total_requests:
        raise ValueError(f"Manifest contains only {len(file_paths)} files, expected at least {total_requests}")

    chosen = file_paths[:total_requests]
    staged_paths: list[str] = []
    dst_bucket = storage_client().bucket(runtime_bucket)

    for src_gs in chosen:
        src_bucket_name, src_blob_name = parse_gs_uri(src_gs)
        src_bucket = storage_client().bucket(src_bucket_name)
        src_blob = src_bucket.blob(src_blob_name)
        if not src_blob.exists():
            raise FileNotFoundError(f"Source CSV missing in GCS: {src_gs}")

        filename = Path(src_blob_name).name
        dst_blob_name = f"monthly_tracker_audits/input/{month}/{filename}"
        src_bucket.copy_blob(src_blob, dst_bucket, new_name=dst_blob_name)
        staged_paths.append(f"gs://{runtime_bucket}/{dst_blob_name}")

    print(f"  Staged {len(staged_paths)} files into gs://{runtime_bucket}/monthly_tracker_audits/input/{month}/")
    return staged_paths


def enqueue_tasks(
    host: str,
    endpoint: str,
    queue_project: str,
    queue_location: str,
    queue_name: str,
    oidc_sa_email: str,
    staged_paths: list[str],
    test_case: str,
    run_id: str,
    month: str,
) -> list[str]:
    """
    Enqueue one Cloud Task per staged file.

    Each task sends POST request to <host><endpoint> with payload:
    file_path, test_case, run_id, month.
    """
    print("Step 4/5: Enqueue Cloud Tasks")
    queue_path = tasks_client().queue_path(queue_project, queue_location, queue_name)
    target_url = f"{host.rstrip('/')}{endpoint}"

    created: list[str] = []
    for file_path in staged_paths:
        payload = {
            "file_path": file_path,
            "test_case": test_case,
            "run_id": run_id,
            "month": month,
        }
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": target_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode("utf-8"),
                "oidc_token": {
                    "service_account_email": oidc_sa_email,
                    "audience": host.rstrip("/"),
                },
            }
        }
        created_task = tasks_client().create_task(parent=queue_path, task=task)
        created.append(created_task.name)

    print(f"  Enqueued {len(created)} tasks to {queue_name}.")
    return created


def wait_until_run_finalized(
    benchmark_bucket: str,
    case: str,
    run_id: str,
    runtime_bucket: str,
    month: str,
    staged_count: int,
    timeout_sec: int,
    poll_interval_sec: int,
) -> tuple[int, float, list[dict], list[dict], list[dict], int]:
    """
    Wait until the run has finalized file outcomes for all staged files.

    E4 parity with E3:
    - Do not finalize KPIs as soon as input folder drains.
    - Wait until per-attempt raw records can be finalized into file outcomes
      for all staged files (success or terminal failure).
    """
    print("Step 5/5: Wait for Cloud Tasks processing completion")
    input_prefix = f"monthly_tracker_audits/input/{month}/"
    start = time.time()
    polls = 0

    while True:
        remaining = count_csv_objects(runtime_bucket, input_prefix)
        attempt_records = load_attempt_records(benchmark_bucket, case, run_id)
        final_success_records, final_failed_records, attempt_failures_transient = _finalize_file_outcomes(attempt_records)
        finalized_files = len(final_success_records) + len(final_failed_records)
        polls += 1
        elapsed_sec = time.time() - start

        print(
            "  Poll "
            f"{polls}: remaining_input={remaining} finalized_files={finalized_files}/{staged_count} "
            f"success={len(final_success_records)} failed={len(final_failed_records)}"
        )

        if remaining == 0 and finalized_files >= staged_count:
            return (
                remaining,
                elapsed_sec,
                attempt_records,
                final_success_records,
                final_failed_records,
                attempt_failures_transient,
            )

        if elapsed_sec > timeout_sec:
            raise TimeoutError(
                "Timed out waiting for Cloud Tasks run finalization "
                f"(remaining_input={remaining}, finalized_files={finalized_files}, staged={staged_count})"
            )

        time.sleep(poll_interval_sec)


def _to_float(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except (TypeError, ValueError):
        return float(default)


def _percentile(values: list[float], p: float) -> float:
    """Replicate E2 percentile behavior (rounded-rank selection)."""
    if not values:
        return 0.0
    ordered = sorted(float(x) for x in values)
    idx = int(round((len(ordered) - 1) * p))
    return float(ordered[idx])


def _basename_from_file_path(file_path: str | None) -> str:
    """Extract filename from a path-like string."""
    if not file_path or not isinstance(file_path, str):
        return ""
    return file_path.rsplit("/", 1)[-1]


def _attempt_sort_key(record: dict) -> tuple[str, str, str]:
    """Build deterministic sort key for attempt ordering."""
    received = str(record.get("request_received_timestamp") or "")
    finished = str(record.get("response_finished_timestamp") or "")
    processing = str(record.get("processing_finished_timestamp") or "")
    return (received, finished, processing)


def _group_records_by_file(records: list[dict]) -> dict[str, list[dict]]:
    """
    Group attempt-level raw records by filename and sort attempts.

    E4 adaptation aligned with E3:
    one file can have multiple Cloud Tasks delivery attempts; we must group
    attempts per file before deriving final outcomes.
    """
    grouped: dict[str, list[dict]] = {}
    for rec in records:
        filename = _basename_from_file_path(rec.get("file_path"))
        if not filename:
            continue
        grouped.setdefault(filename, []).append(rec)
    for filename in list(grouped.keys()):
        grouped[filename] = sorted(grouped[filename], key=_attempt_sort_key)
    return grouped


def _finalize_file_outcomes(records: list[dict]) -> tuple[list[dict], list[dict], int]:
    """
    Convert attempt-level records into final file-level outcomes.

    Returns:
    - final_success_records: one success record per file (first success attempt)
    - final_failed_records: one terminal failed record per file (last attempt)
    - attempt_failures_transient: failed attempts that eventually succeeded

    E4 alignment with E3:
    Cloud Tasks can retry; this logic prevents retries from inflating request
    failures while still exposing transient attempt instability.
    """
    grouped = _group_records_by_file(records)
    final_success_records: list[dict] = []
    final_failed_records: list[dict] = []
    attempt_failures_transient = 0

    for _, attempts in grouped.items():
        first_success_idx = None
        for idx, rec in enumerate(attempts):
            if rec.get("ok") is True:
                first_success_idx = idx
                break

        if first_success_idx is not None:
            attempt_failures_transient += sum(1 for rec in attempts[:first_success_idx] if rec.get("ok") is not True)
            final_success_records.append(attempts[first_success_idx])
        else:
            final_failed_records.append(attempts[-1])

    return final_success_records, final_failed_records, attempt_failures_transient


def load_attempt_records(benchmark_bucket: str, case: str, run_id: str) -> list[dict]:
    """
    Load all attempt-level raw records for a run.

    E4 adaptation aligned with E3:
    route writes blobs under raw/<run_id>/<filename>/<attempt_id>.json, and we
    aggregate from those attempt records.
    """
    case_folder = CASE_TO_FOLDER[case]
    prefix = f"benchmarks/{case_folder}/cloudtasks_results/raw/{run_id}/"
    bucket = storage_client().bucket(benchmark_bucket)
    records: list[dict] = []
    blob_names = sorted(blob.name for blob in storage_client().list_blobs(bucket, prefix=prefix) if blob.name.endswith(".json"))
    for blob_name in blob_names:
        try:
            payload = json.loads(bucket.blob(blob_name).download_as_text())
            records.append(payload)
        except Exception:
            # Keep decode errors visible as failed attempts for reliability accounting.
            filename = Path(blob_name).parts[-2] if len(Path(blob_name).parts) >= 2 else Path(blob_name).name
            records.append(
                {
                    "file": filename,
                    "ok": False,
                    "status_code": 500,
                    "error": "failed_to_decode_record_blob",
                }
            )
    return records


def final_records_for_staged_paths(
    staged_paths: list[str],
    final_success_records: list[dict],
    final_failed_records: list[dict],
) -> list[dict]:
    """
    Return one finalized record per staged file, preserving staged order.

    Raises if any staged file has no finalized outcome record.
    """
    success_by_name = {_basename_from_file_path(r.get("file_path")): r for r in final_success_records}
    failed_by_name = {_basename_from_file_path(r.get("file_path")): r for r in final_failed_records}

    records: list[dict] = []
    missing: list[str] = []
    for staged_path in staged_paths:
        filename = Path(staged_path).name
        record = success_by_name.get(filename) or failed_by_name.get(filename)
        if record is None:
            missing.append(filename)
            continue
        records.append(record)

    if missing:
        raise RuntimeError(f"Missing finalized outcome records for staged files: {', '.join(missing[:10])}")

    return records


def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def build_run_row(case: str, run_id: str, repeat_index: int, records: list[dict]) -> dict:
    """
    Build the E1-E3-compatible one-row KPI aggregate for this run.

    E4 adaptation:
    This preserves the exact KPI column set used previously so cross-experiment
    comparison stays direct.

    KPI parity note:
    - Formulas mirror E2/E3 aggregate logic:
      requests_* from per-file outcomes, makespan from request/response
      timestamps, throughput from successful file count, p50/p95 from
      successful file timing vectors, and totals from successful file records.
    """
    ok_records = [r for r in records if bool(r.get("ok"))]
    requests_ok = len(ok_records)
    requests_failed = max(0, len(records) - requests_ok)

    start_ts = [_parse_iso(r.get("request_received_timestamp")) for r in ok_records]
    end_ts = [_parse_iso(r.get("response_finished_timestamp")) for r in ok_records]
    start_ts = [x for x in start_ts if x is not None]
    end_ts = [x for x in end_ts if x is not None]
    if start_ts and end_ts:
        total_processing_time_sec = max(0.0, (max(end_ts) - min(start_ts)).total_seconds())
    else:
        total_processing_time_sec = sum(_to_float(r.get("end_to_end_latency_sec"), 0.0) for r in ok_records)

    processing_values = [_to_float(r.get("processing_time_sec"), 0.0) for r in ok_records]
    end_to_end_values = [_to_float(r.get("end_to_end_latency_sec"), 0.0) for r in ok_records]
    queue_wait_values = [_to_float(r.get("queue_wait_time_sec"), 0.0) for r in ok_records]

    total_rows = 0
    created_total = 0
    existing_total = 0
    failed_rows_total = 0
    max_active_processing = 0
    max_active_seen = 0
    for record in ok_records:
        processed_rows = int(_to_float(record.get("processed_rows"), 0))
        failed_rows = int(_to_float(record.get("failed_rows"), 0))
        created = int(_to_float(record.get("created_trackers"), 0))
        existing = int(_to_float(record.get("existing_trackers"), 0))
        active_processing = int(_to_float(record.get("active_processing_requests"), 0))
        active_seen = int(_to_float(record.get("max_active_requests_seen"), 0))

        total_rows += processed_rows + failed_rows
        created_total += created
        existing_total += existing
        failed_rows_total += failed_rows
        max_active_processing = max(max_active_processing, active_processing)
        max_active_seen = max(max_active_seen, active_seen)

    # KPI parity with E2/E3:
    # files_per_sec is based on successfully processed files (requests_ok),
    # not total submitted requests.
    files_per_sec = (requests_ok / total_processing_time_sec) if total_processing_time_sec > 0 else 0.0
    records_per_sec = (total_rows / total_processing_time_sec) if total_processing_time_sec > 0 else 0.0

    return {
        "run_id": run_id,
        "test_case": case,
        "repeat_index": repeat_index,
        "locust_exit_code": 0 if requests_failed == 0 else 1,
        "requests_total": len(records),
        "requests_ok": requests_ok,
        "requests_failed": requests_failed,
        "total_processing_time_sec": round(total_processing_time_sec, 6),
        "files_per_sec": round(files_per_sec, 6),
        "records_per_sec": round(records_per_sec, 6),
        "processing_time_p50_sec": round(float(median(processing_values)) if processing_values else 0.0, 6),
        "processing_time_p95_sec": round(_percentile(processing_values, 0.95), 6),
        "end_to_end_latency_p95_sec": round(_percentile(end_to_end_values, 0.95), 6),
        "queue_wait_time_p95_sec": round(_percentile(queue_wait_values, 0.95), 6),
        "created_trackers_total": created_total,
        "existing_trackers_total": existing_total,
        "failed_rows_total": failed_rows_total,
        "max_active_processing_requests_observed": max_active_processing,
        "max_active_requests_seen_observed": max_active_seen,
    }


def _serialize_csv_rows(fieldnames: list[str], rows: list[dict]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return out.getvalue()


def case_results_dir(case: str, results_root_override: str, benchmark_bucket: str) -> str:
    """Resolve output directory for per-run benchmark artifacts."""
    if results_root_override.strip():
        return results_root_override.rstrip("/")
    return f"gs://{benchmark_bucket}/benchmarks/{CASE_TO_FOLDER[case]}/cloudtasks_results"


def write_case_outputs(case_dir: str, case: str, run_row: dict, raw_payload: dict):
    """Persist E1-E3-compatible raw + aggregated outputs to GCS."""
    run_id = run_row["run_id"]

    raw_path = f"{case_dir}/raw/{run_id}.json"
    upload_text_gs(raw_path, json.dumps(raw_payload, indent=2), content_type="application/json")

    run_csv_path = f"{case_dir}/aggregated/{run_id}.csv"
    run_fields = list(run_row.keys())
    upload_text_gs(run_csv_path, _serialize_csv_rows(run_fields, [run_row]), content_type="text/csv")


def main():
    """Run benchmark orchestration for selected test cases and repeats."""
    args = build_parser().parse_args()

    test_cases = _split_csv(args.test_cases)
    for case in test_cases:
        if case not in CASE_TO_FOLDER:
            raise ValueError(f"Unknown test case: {case}")

    if args.total_requests <= 0:
        raise ValueError("--total-requests must be > 0")
    if args.repeats <= 0:
        raise ValueError("--repeats must be > 0")

    for case in test_cases:
        for repeat_index in range(1, args.repeats + 1):
            run_id = f"run_{_now_utc()}"
            case_dir = case_results_dir(case, args.results_dir, args.benchmark_bucket)
            print(f"\n=== {case} repeat {repeat_index} ({run_id}) ===")

            reset_and_seed_db(args.host)
            clear_month_dirs(args.runtime_bucket, args.month)
            manifest_paths = load_manifest(args.benchmark_bucket, case)
            staged_paths = stage_files(manifest_paths, args.runtime_bucket, args.month, args.total_requests)

            enqueue_start = datetime.now(timezone.utc)
            task_names = enqueue_tasks(
                host=args.host,
                endpoint=args.endpoint,
                queue_project=args.queue_project,
                queue_location=args.queue_location,
                queue_name=args.queue_name,
                oidc_sa_email=args.oidc_service_account_email,
                staged_paths=staged_paths,
                test_case=case,
                run_id=run_id,
                month=args.month,
            )
            enqueue_end = datetime.now(timezone.utc)

            (
                remaining,
                wait_sec,
                attempt_records,
                final_success_records,
                final_failed_records,
                attempt_failures_transient,
            ) = wait_until_run_finalized(
                benchmark_bucket=args.benchmark_bucket,
                case=case,
                run_id=run_id,
                runtime_bucket=args.runtime_bucket,
                month=args.month,
                staged_count=len(staged_paths),
                timeout_sec=args.wait_timeout_sec,
                poll_interval_sec=args.poll_interval_sec,
            )

            # E3 parity: derive run KPIs from finalized file outcomes only
            # (one terminal record per staged file).
            records = final_records_for_staged_paths(
                staged_paths=staged_paths,
                final_success_records=final_success_records,
                final_failed_records=final_failed_records,
            )
            run_row = build_run_row(
                case=case,
                run_id=run_id,
                repeat_index=repeat_index,
                records=records,
            )

            raw_payload = {
                "test_case": case,
                "run_id": run_id,
                "month": args.month,
                "requests_total": args.total_requests,
                "requests_started": len(staged_paths),
                "requests_completed": len(records),
                "records": records,
                "attempt_records": attempt_records,
                "cloudtasks_meta": {
                    "tasks_enqueued": len(task_names),
                    "input_drained": 1,
                    "remaining_input_files": remaining,
                    "drain_wait_sec": round(wait_sec, 6),
                    "enqueue_start_timestamp": enqueue_start.isoformat(),
                    "enqueue_finished_timestamp": enqueue_end.isoformat(),
                    "runtime_bucket": args.runtime_bucket,
                    "queue_name": args.queue_name,
                    "attempt_failures_transient": attempt_failures_transient,
                    "files_succeeded_final": len(final_success_records),
                    "files_failed_final": len(final_failed_records),
                    "files_finalized_total": len(records),
                    "files_staged_total": len(staged_paths),
                },
            }

            write_case_outputs(case_dir, case, run_row, raw_payload)
            print(f"  Raw JSON (GCS): {case_dir}/raw/{run_id}.json")
            print(f"  Aggregated CSV (GCS): {case_dir}/aggregated/{run_id}.csv")


if __name__ == "__main__":
    main()
