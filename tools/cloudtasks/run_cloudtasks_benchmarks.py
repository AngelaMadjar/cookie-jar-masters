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
6. write per-run raw JSON + aggregated CSV into GCS results folders.

Unlike E2, this script does not send synchronous HTTP burst traffic itself.
It measures the Cloud Tasks dispatch model by enqueueing work and observing
completion via input-drain + processed/failed counts.
"""

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import quote

import requests
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


def wait_until_input_drained(runtime_bucket: str, month: str, timeout_sec: int, poll_interval_sec: int) -> tuple[bool, int, float]:
    """Poll runtime input folder until no CSV remains or timeout occurs."""
    print("Step 5/5: Wait for runtime input queue to drain")
    input_prefix = f"monthly_tracker_audits/input/{month}/"
    start = time.time()
    polls = 0

    while True:
        remaining = count_csv_objects(runtime_bucket, input_prefix)
        polls += 1
        print(f"  Poll {polls}: remaining files in input = {remaining}")
        if remaining == 0:
            return True, remaining, time.time() - start
        if (time.time() - start) > timeout_sec:
            return False, remaining, time.time() - start
        time.sleep(poll_interval_sec)


def case_results_dir(case: str, results_root_override: str, benchmark_bucket: str) -> str:
    """Resolve output directory for per-run benchmark artifacts."""
    if results_root_override.strip():
        return results_root_override.rstrip("/")
    return f"gs://{benchmark_bucket}/benchmarks/{CASE_TO_FOLDER[case]}/cloudtasks_results"


def write_case_outputs(case_dir: str, run_row: dict, raw_payload: dict):
    """Persist raw run payload JSON and one-row aggregated CSV to GCS."""
    run_id = run_row["run_id"]

    raw_path = f"{case_dir}/raw/{run_id}.json"
    upload_text_gs(raw_path, json.dumps(raw_payload, indent=2), content_type="application/json")

    run_csv_path = f"{case_dir}/aggregated/{run_id}.csv"
    out = StringIO()
    fields = list(run_row.keys())
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    writer.writerow(run_row)
    upload_text_gs(run_csv_path, out.getvalue(), content_type="text/csv")


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
            run_id = f"{case}_r{repeat_index}_{_now_utc()}"
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

            completed, remaining, wait_sec = wait_until_input_drained(
                runtime_bucket=args.runtime_bucket,
                month=args.month,
                timeout_sec=args.wait_timeout_sec,
                poll_interval_sec=args.poll_interval_sec,
            )

            processed_count = count_csv_objects(args.runtime_bucket, f"monthly_tracker_audits/processed/{args.month}/")
            failed_count = count_csv_objects(args.runtime_bucket, f"monthly_tracker_audits/failed/{args.month}/")

            run_row = {
                "run_id": run_id,
                "test_case": case,
                "repeat_index": repeat_index,
                "requests_total": args.total_requests,
                "tasks_enqueued": len(task_names),
                "input_drained": int(completed),
                "remaining_input_files": remaining,
                "drain_wait_sec": round(wait_sec, 6),
                "enqueue_start_timestamp": enqueue_start.isoformat(),
                "enqueue_finished_timestamp": enqueue_end.isoformat(),
                "processed_files_count": processed_count,
                "failed_files_count": failed_count,
                "runtime_bucket": args.runtime_bucket,
                "queue_name": args.queue_name,
            }

            raw_payload = {
                "run": run_row,
                "task_names": task_names,
                "staged_paths": staged_paths,
            }

            write_case_outputs(case_dir, run_row, raw_payload)
            print(f"  Raw JSON (GCS): {case_dir}/raw/{run_id}.json")
            print(f"  Aggregated CSV (GCS): {case_dir}/aggregated/{run_id}.csv")

            if not completed:
                raise RuntimeError(
                    f"Timed out waiting for input to drain for run {run_id}. "
                    f"remaining_input_files={remaining}"
                )


if __name__ == "__main__":
    main()
