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
from google.cloud import storage
from google.protobuf.timestamp_pb2 import Timestamp

try:
    from google.cloud import monitoring_v3
except Exception:  # pragma: no cover
    monitoring_v3 = None

RUNTIME_BUCKET = "e3-data-monthly-audit-trackers"
BENCHMARK_BUCKET = "e3-data-benchmarks"
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
_ID_TOKEN_CACHE: dict[str, str] = {}


def build_parser() -> argparse.ArgumentParser:
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
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_case_identity(test_case_arg: str) -> tuple[str, str]:
    # E3 adaptation: allow either short test ids or descriptive folder names while keeping a canonical mapping.
    if test_case_arg in CASE_FOLDER_BY_ID:
        return test_case_arg, CASE_FOLDER_BY_ID[test_case_arg]
    if test_case_arg in CASE_ID_BY_FOLDER:
        return CASE_ID_BY_FOLDER[test_case_arg], test_case_arg
    raise ValueError(f"Unsupported test case: {test_case_arg}")


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * p))
    return float(ordered[idx])


def _parse_utc_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _gcp_project_id() -> str | None:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or os.getenv("GCLOUD_PROJECT")


def _to_proto_timestamp(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def storage_client() -> storage.Client:
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def monitoring_client():
    global _MONITORING_CLIENT
    if monitoring_v3 is None:
        return None
    if _MONITORING_CLIENT is None:
        _MONITORING_CLIENT = monitoring_v3.MetricServiceClient()
    return _MONITORING_CLIENT


def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// path, got: {gs_uri}")
    raw = gs_uri[len("gs://") :]
    bucket, sep, blob = raw.partition("/")
    if not sep or not bucket or not blob:
        raise ValueError(f"Invalid gs:// path: {gs_uri}")
    return bucket, blob


def download_text_gs(gs_uri: str) -> str:
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_text()


def upload_text_gs(gs_uri: str, content: str, content_type: str):
    bucket_name, blob_name = parse_gs_uri(gs_uri)
    storage_client().bucket(bucket_name).blob(blob_name).upload_from_string(content, content_type=content_type)


def list_blobs(bucket_name: str, prefix: str):
    return storage_client().list_blobs(bucket_name, prefix=prefix)


def count_blobs(bucket_name: str, prefix: str) -> int:
    return sum(1 for _ in list_blobs(bucket_name, prefix))


def delete_prefix(bucket_name: str, prefix: str) -> int:
    deleted = 0
    bucket = storage_client().bucket(bucket_name)
    for blob in list_blobs(bucket_name, prefix):
        bucket.blob(blob.name).delete()
        deleted += 1
    return deleted


def _cloud_run_id_token(audience: str) -> str:
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
    token = _cloud_run_id_token(host.rstrip("/"))
    return {"Authorization": f"Bearer {token}"}


def _post_ok(host: str, endpoint: str, payload: dict):
    resp = requests.post(
        f"{host}{endpoint}",
        json=payload,
        headers=_cloud_run_auth_headers(host),
        timeout=1800,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"{endpoint} failed: {resp.status_code} {resp.text}")


def reset_and_seed_db(host: str):
    print("Step 1/6: Reset and reseed DB")
    _post_ok(host, "/db/empty", {})
    _post_ok(host, "/db/populate/cmp", {})
    _post_ok(host, "/db/populate/trackers", {})


def clear_runtime_prefixes(month: str):
    print("Step 2/6: Clear runtime input/processed/failed prefixes")
    for prefix in (f"input/{month}/", f"processed/{month}/", f"failed/{month}/"):
        deleted = delete_prefix(RUNTIME_BUCKET, prefix)
        print(f"  Cleared {deleted} objects from gs://{RUNTIME_BUCKET}/{prefix}")


def load_manifest_paths(test_case_folder: str) -> list[str]:
    manifest_uri = f"gs://{BENCHMARK_BUCKET}/{test_case_folder}/manifest/manifest.json"
    payload = json.loads(download_text_gs(manifest_uri))
    paths = payload.get("generated_file_paths") or []
    if not paths:
        raise ValueError(f"No generated_file_paths found in {manifest_uri}")
    return [str(x) for x in paths]


def stage_files(test_case_id: str, test_case_folder: str, run_id: str, month: str, total_files: int) -> list[str]:
    print("Step 3/6: Copy test-case inputs into runtime input prefix")
    manifest_paths = load_manifest_paths(test_case_folder)
    if len(manifest_paths) < total_files:
        raise ValueError(f"Manifest contains only {len(manifest_paths)} files, expected at least {total_files}")

    selected = manifest_paths[:total_files]
    dst_bucket = storage_client().bucket(RUNTIME_BUCKET)
    staged = []
    for src_uri in selected:
        src_bucket_name, src_blob_name = parse_gs_uri(src_uri)
        src_bucket = storage_client().bucket(src_bucket_name)
        src_blob = src_bucket.blob(src_blob_name)
        if not src_blob.exists():
            raise FileNotFoundError(f"Source file missing: {src_uri}")

        filename = src_blob_name.rsplit("/", 1)[-1]
        dst_blob_name = f"input/{month}/{filename}"
        copied = src_bucket.copy_blob(src_blob, dst_bucket, new_name=dst_blob_name)
        # E3 adaptation: attach both id and folder metadata so event handler writes raw artifacts to correct folder.
        copied.metadata = {
            "test_case": test_case_id,
            "test_case_folder": test_case_folder,
            "run_id": run_id,
            "month": month,
        }
        copied.patch()
        staged.append(f"gs://{RUNTIME_BUCKET}/{dst_blob_name}")

    print(f"  Copied {len(staged)} files into gs://{RUNTIME_BUCKET}/input/{month}/")
    return staged


def wait_until_complete(
    test_case_folder: str,
    run_id: str,
    month: str,
    staged_count: int,
    timeout_sec: int,
    poll_interval_sec: int,
):
    print("Step 4/6: Wait for event-driven processing completion")
    start = time.time()
    raw_prefix = f"{test_case_folder}/results/raw/{run_id}/"
    while True:
        input_remaining = count_blobs(RUNTIME_BUCKET, f"input/{month}/")
        raw_records_count = count_blobs(BENCHMARK_BUCKET, raw_prefix)
        elapsed = int(time.time() - start)
        print(
            f"  elapsed={elapsed}s input_remaining={input_remaining} raw_records={raw_records_count}/{staged_count}"
        )

        if input_remaining == 0 and raw_records_count >= staged_count:
            return

        if elapsed >= timeout_sec:
            raise TimeoutError(
                "Timed out waiting for processing completion "
                f"(input_remaining={input_remaining}, raw_records={raw_records_count}, staged={staged_count})"
            )
        time.sleep(max(1, poll_interval_sec))


def read_run_records(test_case_folder: str, run_id: str) -> list[dict]:
    prefix = f"{test_case_folder}/results/raw/{run_id}/"
    records = []
    for blob in list_blobs(BENCHMARK_BUCKET, prefix):
        payload = json.loads(blob.download_as_text())
        records.append(payload)
    return records


def cloud_run_max_active_instances_observed(records: list[dict]) -> int | None:
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


def aggregate_run_metrics(records: list[dict], run_id: str, test_case_id: str) -> dict:
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
    failed_requests = len(records) - files_count

    return {
        "run_id": run_id,
        "test_case": test_case_id,
        "repeat_index": 1,
        "locust_exit_code": 0 if failed_requests == 0 else 1,
        "requests_total": len(records),
        "requests_ok": files_count,
        "requests_failed": failed_requests,
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
    }


def write_outputs(test_case_folder: str, run_id: str, month: str, records: list[dict], run_row: dict):
    print("Step 5/6: Write run artifacts")
    raw_bundle_uri = f"gs://{BENCHMARK_BUCKET}/{test_case_folder}/results/raw/{run_id}.json"
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

    csv_uri = f"gs://{BENCHMARK_BUCKET}/{test_case_folder}/results/aggregated/{run_id}.csv"
    fields = list(run_row.keys())
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fields)
    writer.writeheader()
    writer.writerow(run_row)
    upload_text_gs(csv_uri, out.getvalue(), content_type="text/csv")

    print(f"  Raw bundle: {raw_bundle_uri}")
    print(f"  Aggregated row: {csv_uri}")


def main():
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

