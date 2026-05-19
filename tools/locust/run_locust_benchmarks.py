#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from statistics import median

import requests
from google.cloud import storage


CASE_TO_FOLDER = {
    "T1": "T1_original",
    "T2": "T2_medium_existing_heavy",
    "T3": "T3_medium_new_heavy",
    "T4": "T4_large_existing_heavy",
    "T5": "T5_large_new_heavy",
    "T6": "T6_skewed_existing_heavy",
    "T7": "T7_skewed_new_heavy",
}

GCS_BUCKET = "e2-data"
RUNTIME_INPUT_PREFIX = "monthly_tracker_audits/input"
RUNTIME_PROCESSED_PREFIX = "monthly_tracker_audits/processed"
RUNTIME_FAILED_PREFIX = "monthly_tracker_audits/failed"
_STORAGE_CLIENT: storage.Client | None = None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run Locust benchmarks for T1-T7 with automated DB reset and file staging.")
    p.add_argument("--host", default="http://127.0.0.1:8080", help="Ingestion app base URL")
    p.add_argument("--month", default="2026-02")
    p.add_argument("--test-cases", default="T1,T2,T3,T4,T5,T6,T7")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--users", type=int, default=80)
    p.add_argument("--spawn-rate", type=int, default=80)
    p.add_argument("--total-requests", type=int, default=80)
    p.add_argument("--ui", action="store_true", help="Run locust in web UI mode with autostart/autoquit.")
    p.add_argument("--results-dir", default="", help="Optional override for results root. If unset, writes under each case folder.")
    p.add_argument("--locust-bin", default="python3 -m locust")
    return p


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _split_csv(text: str) -> list[str]:
    return [x.strip() for x in text.split(",") if x.strip()]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * p))
    return float(ordered[idx])


def storage_client() -> storage.Client:
    # E2 GCS adaptation: runner now orchestrates benchmark artifacts directly in GCS.
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    # E2 GCS adaptation: parse gs:// URIs instead of local filesystem paths.
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// path, got: {gs_uri}")
    raw = gs_uri[len("gs://"):]
    bucket, sep, blob = raw.partition("/")
    if not sep or not bucket or not blob:
        raise ValueError(f"Invalid gs:// path: {gs_uri}")
    return bucket, blob


def upload_text_gs(gs_uri: str, content: str, content_type: str = "application/json"):
    # E2 GCS adaptation: persist runner artifacts to GCS.
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    storage_client().bucket(bucket_name).blob(blob_name).upload_from_string(content, content_type=content_type)


def download_text_gs(gs_uri: str) -> str:
    # E2 GCS adaptation: load manifests/aggregates directly from GCS.
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_text()


def delete_prefix_gs(bucket_name: str, prefix: str) -> int:
    # E2 GCS adaptation: clear monthly input/processed/failed using GCS prefix deletion.
    bucket = storage_client().bucket(bucket_name)
    deleted = 0
    for blob in storage_client().list_blobs(bucket, prefix=prefix):
        blob.delete()
        deleted += 1
    return deleted


def _post_ok(host: str, endpoint: str, payload: dict):
    print(f"  -> POST {endpoint}")
    resp = requests.post(f"{host}{endpoint}", json=payload, timeout=1800)
    if resp.status_code != 200:
        raise RuntimeError(f"{endpoint} failed: {resp.status_code} {resp.text}")


def reset_and_seed_db(host: str):
    print("Step 1/5: Reset and reseed database")
    _post_ok(host, "/db/empty", {})
    _post_ok(host, "/db/populate/cmp", {})
    _post_ok(host, "/db/populate/trackers", {})
    print("  DB reset + seed completed.")


def clear_month_dirs(month: str):
    # E2 GCS adaptation: cleanup moved from local monthly directories to GCS prefixes.
    print(f"Step 2/5: Clear monthly runtime prefixes in GCS for {month}")
    targets = [
        f"{RUNTIME_INPUT_PREFIX}/{month}/",
        f"{RUNTIME_PROCESSED_PREFIX}/{month}/",
        f"{RUNTIME_FAILED_PREFIX}/{month}/",
    ]
    for prefix in targets:
        removed = delete_prefix_gs(GCS_BUCKET, prefix)
        print(f"  Cleared {removed} objects from gs://{GCS_BUCKET}/{prefix}")


def load_manifest(case: str) -> list[str]:
    case_folder = CASE_TO_FOLDER[case]
    # E2 GCS adaptation: manifests are read from GCS to avoid container-local benchmark dependencies.
    manifest_path = f"gs://{GCS_BUCKET}/benchmarks/{case_folder}/manifest/manifest.json"
    payload = json.loads(download_text_gs(manifest_path))
    paths = payload.get("generated_file_paths") or []
    if len(paths) == 0:
        raise ValueError(f"No generated_file_paths in manifest: {manifest_path}")
    return [str(p) for p in paths]


def stage_files(file_paths: list[str], month: str, total_requests: int) -> list[str]:
    # E2 GCS adaptation: file staging moved from local copy into local input to GCS copy into GCS input.
    print(f"Step 3/5: Stage {total_requests} files into GCS monthly input")
    if len(file_paths) < total_requests:
        raise ValueError(f"Manifest contains only {len(file_paths)} files, expected at least {total_requests}")
    chosen = file_paths[:total_requests]
    staged_paths: list[str] = []
    bucket = storage_client().bucket(GCS_BUCKET)
    for src_gs in chosen:
        src_bucket_name, src_blob_name = parse_gs_uri(src_gs)
        src_bucket = storage_client().bucket(src_bucket_name)
        src_blob = src_bucket.blob(src_blob_name)
        if not src_blob.exists():
            raise FileNotFoundError(f"Source CSV missing in GCS: {src_gs}")
        filename = Path(src_blob_name).name
        dst_blob_name = f"{RUNTIME_INPUT_PREFIX}/{month}/{filename}"
        src_bucket.copy_blob(src_blob, bucket, new_name=dst_blob_name)
        staged_paths.append(f"gs://{GCS_BUCKET}/{dst_blob_name}")
    print(f"  Staged {len(staged_paths)} files into gs://{GCS_BUCKET}/{RUNTIME_INPUT_PREFIX}/{month}/")
    return staged_paths


def case_results_dir(case: str, results_root_override: str) -> str:
    # E2 GCS adaptation: default benchmark result root is now in GCS.
    if results_root_override.strip():
        return results_root_override.rstrip("/")
    return f"gs://{GCS_BUCKET}/benchmarks/{CASE_TO_FOLDER[case]}/locust_results"


def run_locust(args, run_spec_payload: dict, raw_json_path: str) -> int:
    env = os.environ.copy()
    env["LOCUST_RUN_SPEC_JSON"] = json.dumps(run_spec_payload)
    env["LOCUST_RAW_JSON_PATH"] = raw_json_path
    env["LOCUST_MONTH"] = args.month
    env["LOCUST_RUNTIME_FILE_LIST_PATH"] = "tools/locust/runtime_input_files_2026-02.json"

    locustfile = Path("tools/locust/locustfile.py")
    cmd = [
        *shlex.split(args.locust_bin),
        "-f",
        str(locustfile),
        "--host",
        args.host,
        "-u",
        str(args.users),
        "-r",
        str(args.spawn_rate),
    ]
    if args.ui:
        cmd.extend(["--autostart", "--autoquit", "3"])
    else:
        cmd.append("--headless")
    print("Step 4/5: Run Locust load test")
    print(f"  Command: {' '.join(cmd)}")
    completed = subprocess.run(cmd, env=env, check=False)
    return int(completed.returncode)


def load_raw_records(path: str) -> list[dict]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload.get("records") or []


def aggregate_run_metrics(records: list[dict], run_id: str, test_case: str, repeat_index: int, locust_exit_code: int) -> dict:
    ok_records = [r for r in records if r.get("ok")]

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
    records_count = int(sum(int(r.get("processed_rows", 0) or 0) + int(r.get("failed_rows", 0) or 0) for r in ok_records))
    created_total = int(sum(int(r.get("created_trackers", 0) or 0) for r in ok_records))
    existing_total = int(sum(int(r.get("existing_trackers", 0) or 0) for r in ok_records))
    failed_total = int(sum(int(r.get("failed_rows", 0) or 0) for r in ok_records))
    active_parallel = [int(r.get("active_processing_requests", 0) or 0) for r in ok_records]
    max_active_seen = [int(r.get("max_active_requests_seen", 0) or 0) for r in ok_records]

    files_per_sec = (files_count / makespan_sec) if makespan_sec > 0 else 0.0
    records_per_sec = (records_count / makespan_sec) if makespan_sec > 0 else 0.0

    return {
        "run_id": run_id,
        "test_case": test_case,
        "repeat_index": repeat_index,
        "locust_exit_code": locust_exit_code,
        "requests_total": len(records),
        "requests_ok": files_count,
        "requests_failed": len(records) - files_count,
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
    }


def write_case_outputs(case_dir: str, run_row: dict, records_payload: dict):
    # E2 GCS adaptation: benchmark outputs are written to GCS instead of local benchmark folders.
    run_id = run_row["run_id"]
    raw_path = f"{case_dir}/raw/{run_id}.json"
    upload_text_gs(raw_path, json.dumps(records_payload, indent=2), content_type="application/json")

    runs_csv = f"{case_dir}/aggregated/runs.csv"
    fields = list(run_row.keys())
    rows: list[dict] = []
    try:
        existing_csv = download_text_gs(runs_csv)
        rows = list(csv.DictReader(StringIO(existing_csv)))
    except Exception:
        rows = []
    rows.append(run_row)

    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    upload_text_gs(runs_csv, out.getvalue(), content_type="text/csv")


def write_case_summary(case_dir: str):
    # E2 GCS adaptation: summary aggregation is loaded/saved via GCS CSV objects.
    runs_csv = f"{case_dir}/aggregated/runs.csv"
    try:
        rows = list(csv.DictReader(StringIO(download_text_gs(runs_csv))))
    except Exception:
        rows = []
    if not rows:
        return
    numeric_keys = [
        "total_processing_time_sec",
        "files_per_sec",
        "records_per_sec",
        "processing_time_p50_sec",
        "processing_time_p95_sec",
        "end_to_end_latency_p95_sec",
        "queue_wait_time_p95_sec",
        "created_trackers_total",
        "existing_trackers_total",
        "failed_rows_total",
        "max_active_processing_requests_observed",
        "max_active_requests_seen_observed",
    ]

    summary = {
        "test_case": rows[0]["test_case"],
        "runs": len(rows),
    }
    for k in numeric_keys:
        vals = [float(r.get(k, 0) or 0) for r in rows]
        summary[f"{k}_mean"] = round(sum(vals) / len(vals), 6)
        summary[f"{k}_min"] = round(min(vals), 6)
        summary[f"{k}_max"] = round(max(vals), 6)

    summary_path = f"{case_dir}/aggregated/summary.csv"
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=list(summary.keys()))
    writer.writeheader()
    writer.writerow(summary)
    upload_text_gs(summary_path, out.getvalue(), content_type="text/csv")


def main():
    args = build_parser().parse_args()
    test_cases = _split_csv(args.test_cases)
    for c in test_cases:
        if c not in CASE_TO_FOLDER:
            raise ValueError(f"Unknown test case: {c}")
    if args.total_requests <= 0:
        raise ValueError("--total-requests must be > 0")
    if args.repeats <= 0:
        raise ValueError("--repeats must be > 0")

    for case in test_cases:
        case_dir = case_results_dir(case, args.results_dir)
        manifest_paths = load_manifest(case)

        for repeat_index in range(1, args.repeats + 1):
            run_id = f"{case}_r{repeat_index}_{_now_utc()}"
            print(f"\n=== {case} repeat {repeat_index} ({run_id}) ===")
            reset_and_seed_db(args.host)
            clear_month_dirs(args.month)
            stage_files(manifest_paths, args.month, args.total_requests)

            # Locust writes run details locally; runner then uploads persisted artifacts to GCS.
            raw_json_path = f"/tmp/{run_id}.json"
            run_spec_payload = {
                "test_case": case,
                "run_id": run_id,
            }
            exit_code = run_locust(args, run_spec_payload, raw_json_path)
            if exit_code != 0:
                print(f"  Locust exited with code {exit_code}. Aggregating partial/failed run from raw results.")

            if not Path(raw_json_path).exists():
                raise RuntimeError(
                    f"Raw results not found for run {run_id} at {raw_json_path}. "
                    f"Locust exit code was {exit_code}."
                )
            records_payload = json.loads(Path(raw_json_path).read_text(encoding="utf-8"))
            records = records_payload.get("records") or []
            run_row = aggregate_run_metrics(records, run_id, case, repeat_index, exit_code)
            print("Step 5/5: Write results")
            write_case_outputs(case_dir, run_row, records_payload)
            print(
                "  Run KPI snapshot: "
                f"makespan={run_row['total_processing_time_sec']}s, "
                f"files/s={run_row['files_per_sec']}, "
                f"records/s={run_row['records_per_sec']}, "
                f"max_parallel={run_row['max_active_processing_requests_observed']}"
            )
            print(f"  Raw JSON (local temp): {raw_json_path}")
            print(f"  Raw JSON (GCS): {case_dir}/raw/{run_id}.json")

        write_case_summary(case_dir)
        print(f"Completed case {case}. Results: {case_dir}")
        print(f"  Aggregated CSV: {case_dir}/aggregated/runs.csv")
        print(f"  Summary CSV: {case_dir}/aggregated/summary.csv")

    if args.results_dir.strip():
        print(f"\nAll done. Results root override: {args.results_dir}")
    else:
        print("\nAll done. Results written under gs://e2-data/benchmarks/<case>/locust_results.")


if __name__ == "__main__":
    main()
