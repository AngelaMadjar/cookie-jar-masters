from datetime import datetime, timezone
from pathlib import PurePosixPath
import json

from flask import Blueprint, jsonify, request
from google.api_core.exceptions import NotFound

from app.services.tracker_ingestion import TrackerIngestionOrchestrator
from app.services.tracker_ingestion.scan_lifecycle_service import ScanLifecycleService

bp = Blueprint("tracker_ingestion", __name__, url_prefix="/scan")

# E3 adaptation: use the monthly-audit bucket as runtime and results storage for E3 runs.
E3_RUNTIME_BUCKET = "e3-data-monthly-audit-trackers"
E3_RESULTS_BUCKET = "e3-data-monthly-audit-trackers"
DEFAULT_MONTH = "2026-02"
# E3 adaptation: benchmark bucket uses descriptive case-folder names instead of short ids.
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




def _infer_month_from_object_name(object_name: str) -> str:
    parts = PurePosixPath(object_name).parts
    if len(parts) >= 2 and parts[0] == "input":
        return parts[1]
    return DEFAULT_MONTH


def _benchmark_case_folder(test_case: str, test_case_folder: str | None = None) -> str:
    # E3 adaptation: allow either short test id (T1..T7) or explicit folder name from event metadata.
    if test_case_folder:
        return test_case_folder
    return CASE_FOLDER_BY_ID.get(test_case, test_case)


def _safe_record_blob_name(test_case_folder: str, run_id: str, file_path: str) -> str:
    filename = file_path.rsplit("/", 1)[-1]
    return f"results/{test_case_folder}/raw/{run_id}/{filename}.json"


def _write_raw_record(test_case: str, run_id: str, file_path: str, record: dict, test_case_folder: str | None = None):
    # E3 adaptation: persist per-file processing records under runtime bucket results/<test_case>/raw/.
    blob_name = _safe_record_blob_name(
        test_case_folder=_benchmark_case_folder(test_case, test_case_folder),
        run_id=run_id,
        file_path=file_path,
    )
    bucket = ScanLifecycleService.storage_client().bucket(E3_RESULTS_BUCKET)
    bucket.blob(blob_name).upload_from_string(
        json.dumps(record),
        content_type="application/json",
    )


def _event_payload_to_file_path(payload: dict) -> tuple[str | None, str | None, dict]:
    event_data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(event_data, dict):
        return None, None, {}

    bucket = event_data.get("bucket")
    object_name = event_data.get("name")
    metadata = event_data.get("metadata") if isinstance(event_data.get("metadata"), dict) else {}
    if not bucket or not object_name:
        return None, None, metadata
    return f"gs://{bucket}/{object_name}", object_name, metadata


def _event_case_folder_and_run_id(object_name: str) -> tuple[str | None, str | None]:
    # E3 adaptation: infer test-case folder and run id directly from object path to avoid metadata timing races.
    parts = PurePosixPath(object_name).parts
    if len(parts) >= 3 and parts[1] == "input":
        case_folder = parts[0]
        run_id = parts[2] if len(parts) >= 4 else None
        return case_folder, run_id
    return None, None


def _normalize_ingest_payload(payload: dict) -> tuple[dict | None, bool, dict | None, int | None]:
    # E3 adaptation: allow the canonical /scan/ingest endpoint to accept both HTTP benchmark payloads and GCS finalize events.
    file_path = payload.get("file_path")
    if file_path and isinstance(file_path, str):
        return (
            {
                "file_path": file_path,
                "test_case": payload.get("test_case"),
                "test_case_folder": payload.get("test_case_folder"),
                "run_id": payload.get("run_id"),
                "month": payload.get("month", DEFAULT_MONTH),
            },
            False,
            None,
            None,
        )

    file_path, object_name, metadata = _event_payload_to_file_path(payload)
    if not file_path or not object_name:
        return None, False, {"error": "bad request", "details": "file_path is required"}, 400

    # E3 adaptation: process only benchmark input object finalizations to avoid loops from results/processed/failed writes.
    if "/input/" not in object_name:
        return None, True, {"status": "ignored", "file_path": file_path}, 200
    # E3 adaptation: ignore non-csv objects (for example marker files) on the event path.
    if not object_name.lower().endswith(".csv"):
        return None, True, {"status": "ignored", "file_path": file_path}, 200

    case_folder_from_path, run_id_from_path = _event_case_folder_and_run_id(object_name)
    month = str(metadata.get("month") or _infer_month_from_object_name(object_name) or DEFAULT_MONTH)
    test_case_folder = str(
        metadata.get("test_case_folder")
        or case_folder_from_path
        or _benchmark_case_folder(str(metadata.get("test_case") or "unknown"))
    )
    test_case = str(metadata.get("test_case") or CASE_ID_BY_FOLDER.get(test_case_folder, "unknown"))
    run_id = str(
        metadata.get("run_id")
        or run_id_from_path
        or f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    return (
        {
            "file_path": file_path,
            "test_case": test_case,
            "test_case_folder": test_case_folder,
            "run_id": run_id,
            "month": month,
        },
        True,
        None,
        None,
    )


@bp.post("/ingest")
def ingest_file():
    request_received_timestamp = datetime.now(timezone.utc)
    payload = request.get_json(silent=True) or {}
    normalized_payload, is_storage_event, early_body, early_status = _normalize_ingest_payload(payload)
    if early_body is not None:
        return jsonify(early_body), int(early_status or 400)

    file_path = normalized_payload.get("file_path") if normalized_payload else None
    test_case = normalized_payload.get("test_case") if normalized_payload else None
    # E3 adaptation: optional folder hint keeps benchmark artifacts in the expected GCS case folder.
    test_case_folder = normalized_payload.get("test_case_folder") if normalized_payload else None
    run_id = normalized_payload.get("run_id") if normalized_payload else None
    month = normalized_payload.get("month") if normalized_payload else None

    if not file_path or not isinstance(file_path, str):
        return jsonify({"error": "bad request", "details": "file_path is required"}), 400
    if not test_case or not isinstance(test_case, str):
        return jsonify({"error": "bad request", "details": "test_case is required"}), 400
    if not run_id or not isinstance(run_id, str):
        return jsonify({"error": "bad request", "details": "run_id is required"}), 400
    if not month or not isinstance(month, str):
        return jsonify({"error": "bad request", "details": "month is required"}), 400

    try:
        result = TrackerIngestionOrchestrator.ingest_single_file(
            file_path=file_path,
            test_case=test_case,
            run_id=run_id,
            month=month,
            request_received_timestamp=request_received_timestamp,
        )
        _write_raw_record(
            test_case=test_case,
            run_id=run_id,
            file_path=file_path,
            record={
                "file_path": file_path,
                "status_code": 200,
                "ok": True,
                "error": None,
                **result,
            },
            test_case_folder=test_case_folder,
        )
        if is_storage_event:
            # E3 adaptation: Eventarc callers only need an acknowledgment payload and stable status.
            return jsonify({"status": "processed", "file_path": file_path, "run_id": run_id}), 200
        return jsonify(result), 200
    except NotFound:
        if is_storage_event:
            # E3 adaptation: duplicate finalize events are expected, so missing source object is treated as safe skip.
            return jsonify({"status": "skipped", "reason": "source object not found", "file_path": file_path}), 200
        _write_raw_record(
            test_case=test_case,
            run_id=run_id,
            file_path=file_path,
            record={
                "file_path": file_path,
                "status_code": 500,
                "ok": False,
                "error": "source object not found",
                "test_case": test_case,
                "run_id": run_id,
                "month": month,
                "request_received_timestamp": request_received_timestamp.isoformat(),
            },
            test_case_folder=test_case_folder,
        )
        return jsonify({"error": "internal error", "details": "source object not found"}), 500
    except Exception as exc:
        _write_raw_record(
            test_case=test_case,
            run_id=run_id,
            file_path=file_path,
            record={
                "file_path": file_path,
                "status_code": 500,
                "ok": False,
                "error": str(exc),
                "test_case": test_case,
                "run_id": run_id,
                "month": month,
                "request_received_timestamp": request_received_timestamp.isoformat(),
            },
            test_case_folder=test_case_folder,
        )
        return jsonify({"error": "internal error", "details": str(exc)}), 500


@bp.post("/events/storage-finalized")
def on_storage_finalized():
    # E3 adaptation: keep backward-compatible event path while delegating to the canonical /scan/ingest handler.
    return ingest_file()
