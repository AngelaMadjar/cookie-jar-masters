import json
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, request
from google.api_core.exceptions import NotFound
from google.cloud import storage

from app.services.tracker_ingestion import TrackerIngestionOrchestrator

bp = Blueprint("tracker_ingestion", __name__, url_prefix="/scan")

CASE_TO_FOLDER = {
    "T1": "T1_original",
    "T2": "T2_medium_existing_heavy",
    "T3": "T3_medium_new_heavy",
    "T4": "T4_large_existing_heavy",
    "T5": "T5_large_new_heavy",
    "T6": "T6_skewed_existing_heavy",
    "T7": "T7_skewed_new_heavy",
}
_STORAGE_CLIENT = None


def _storage_client():
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT


def _safe_record_blob_name(test_case: str, run_id: str, file_path: str, attempt_id: str) -> str:
    """
    E4 adaptation (aligned to E3 raw layout):
    Build per-attempt object path so retries do not overwrite each other.

    Path shape:
    benchmarks/<case_folder>/cloudtasks_results/raw/<run_id>/<filename>/<attempt_id>.json
    """
    case_folder = CASE_TO_FOLDER.get(test_case)
    if not case_folder:
        return ""
    filename = Path(file_path).name
    return f"benchmarks/{case_folder}/cloudtasks_results/raw/{run_id}/{filename}/{attempt_id}.json"


def _cloudtasks_attempt_id() -> str:
    """
    Build a stable attempt identifier from Cloud Tasks headers.

    Why:
    - E4 is asynchronous and can retry deliveries.
    - We persist one raw record per attempt, like E3, to separate transient
      attempt failures from final file outcomes.
    """
    task_name = (request.headers.get("X-CloudTasks-TaskName") or "").strip()
    retry_count = (request.headers.get("X-CloudTasks-TaskRetryCount") or "0").strip()
    execution_count = (request.headers.get("X-CloudTasks-TaskExecutionCount") or "0").strip()
    eta = (request.headers.get("X-CloudTasks-TaskETA") or "").strip()

    # Keep object name filesystem-safe.
    safe_task_name = task_name.replace("/", "_").replace(":", "_").replace(" ", "_") if task_name else "manual"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    if eta:
        return f"{safe_task_name}-r{retry_count}-e{execution_count}-eta{eta}-{ts}"
    return f"{safe_task_name}-r{retry_count}-e{execution_count}-{ts}"


def _write_e4_request_record(file_path: str, test_case: str, run_id: str, attempt_id: str, record: dict):
    """
    E4 adaptation:
    Persist one per-attempt request record in GCS so Cloud Tasks runs can be
    aggregated into the same KPI schema used by E1-E3.

    Why this exists:
    - In E2 (Locust), the benchmark runner receives each HTTP response directly
      and can aggregate per-file KPIs from the client side.
    - In E4 (Cloud Tasks), the enqueuer submits tasks asynchronously and does
      not receive each /scan/ingest response payload.
    - Therefore, /scan/ingest must persist raw attempt records so the E4
      runner can finalize outcomes per file (E3-style) and then reconstruct
      file-level metrics (queue, processing, latency, created/existing/failed
      counts) using the same formulas as E1-E3.
    """
    blob_name = _safe_record_blob_name(test_case=test_case, run_id=run_id, file_path=file_path, attempt_id=attempt_id)
    if not blob_name:
        return

    bucket_name = "e4-data"
    _storage_client().bucket(bucket_name).blob(blob_name).upload_from_string(
        json.dumps(record, ensure_ascii=True),
        content_type="application/json",
    )


@bp.post("/ingest")
def ingest_file():
    """
    Ingest exactly one scan file and return per-file KPI summary.

    Expected JSON payload:
    - file_path: source file path/URI
    - test_case: benchmark case label (for example T1-T7)
    - run_id: benchmark run identifier
    - month: workload month folder (default: "2026-02")

    Shared validation:
    - Reject missing/invalid required fields with 400.

    Branch difference:
    - E1 validates local filesystem paths under monthly input folder.
    - E4 (this branch) validates GCS URIs and requires file_path under
      `gs://e4-data/`.

    E4 KPI parity behavior:
    - This route writes one per-attempt success/failure record to GCS.
    - The Cloud Tasks benchmark runner later reads those records and computes
      the same aggregated KPI columns used in E1/E2/E3.

    On success, delegates to TrackerIngestionOrchestrator and returns a JSON
    summary containing count and timing/concurrency KPIs.
    """
    request_received_timestamp = datetime.now(timezone.utc)
    payload = request.get_json(silent=True) or {}
    file_path = payload.get("file_path")
    test_case = payload.get("test_case")
    run_id = payload.get("run_id")
    month = payload.get("month", "2026-02")
    attempt_id = _cloudtasks_attempt_id()

    if not file_path or not isinstance(file_path, str):
        return jsonify({"error": "bad request", "details": "file_path is required"}), 400
    if not test_case or not isinstance(test_case, str):
        return jsonify({"error": "bad request", "details": "test_case is required"}), 400
    if not run_id or not isinstance(run_id, str):
        return jsonify({"error": "bad request", "details": "run_id is required"}), 400
    if not month or not isinstance(month, str):
        return jsonify({"error": "bad request", "details": "month is required"}), 400

    # E4-specific path policy: only gs:// URIs under the experiment bucket prefix
    # are accepted by this route.
    if not file_path.startswith("gs://"):
        return jsonify(
            {
                "error": "bad request",
                "details": "file_path must be a gs:// URI",
            }
        ), 400
    if not file_path.startswith("gs://e4-data/"):
        return jsonify(
            {
                "error": "bad request",
                "details": "file_path must be under gs://e4-data/",
            }
        ), 400

    try:
        result = TrackerIngestionOrchestrator.ingest_single_file(
            file_path=file_path,
            test_case=test_case,
            run_id=run_id,
            month=month,
            request_received_timestamp=request_received_timestamp,
        )
        # E4 adaptation:
        # Save one successful attempt record for KPI aggregation by the
        # Cloud Tasks benchmark runner. This is required because Cloud Tasks
        # enqueuer does not synchronously receive this response body.
        record = dict(result)
        record["ok"] = True
        record["status_code"] = 200
        record["file_path"] = file_path
        _write_e4_request_record(
            file_path=file_path,
            test_case=test_case,
            run_id=run_id,
            attempt_id=attempt_id,
            record=record,
        )
        return jsonify(result), 200
    except NotFound:
        # E4 async duplicate handling:
        # if a retry/duplicate arrives after another attempt already moved the
        # file, acknowledge as skipped (idempotent success) like E3.
        return jsonify({"status": "skipped", "reason": "source object not found", "file_path": file_path}), 200
    except Exception as exc:
        # E4 adaptation:
        # Save one failed attempt record as well; this lets the runner apply
        # E3-style finalization (transient-attempt failures vs final failures)
        # instead of losing retry history in asynchronous dispatch.
        failure_record = {
            "ok": False,
            "status_code": 500,
            "error": str(exc),
            "file_path": file_path,
            "test_case": test_case,
            "run_id": run_id,
            "month": month,
            "request_received_timestamp": request_received_timestamp.isoformat(),
        }
        try:
            _write_e4_request_record(
                file_path=file_path,
                test_case=test_case,
                run_id=run_id,
                attempt_id=attempt_id,
                record=failure_record,
            )
        except Exception:
            # Do not mask ingestion failure with record-write failure.
            pass
        return jsonify({"error": "internal error", "details": str(exc)}), 500
