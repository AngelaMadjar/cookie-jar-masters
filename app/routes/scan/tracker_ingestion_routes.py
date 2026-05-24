from datetime import datetime, timezone
import json

from flask import Blueprint, jsonify, request
from google.api_core.exceptions import NotFound

from app.services.tracker_ingestion import TrackerIngestionOrchestrator
from app.services.tracker_ingestion.scan_lifecycle_service import ScanLifecycleService

bp = Blueprint("tracker_ingestion", __name__, url_prefix="/scan")

# E3 adaptation:
# - source input objects are in trigger bucket: gs://e3-data-benchmarks/<test_case>/input/...
# - per-attempt raw KPI artifacts are written to: gs://e3-data-monthly-audit-trackers/results/...
E3_RESULTS_BUCKET = "e3-data-monthly-audit-trackers"


def _safe_record_blob_name(test_case_folder: str, run_id: str, file_path: str, attempt_id: str) -> str:
    """
    Build the raw record object path for one delivery attempt of one file.

    Path shape:
    results/<test_case_folder>/raw/<run_id>/<filename>/<attempt_id>.json
    """
    filename = file_path.rsplit("/", 1)[-1]
    # E3 adaptation: persist each delivery attempt as a separate raw artifact so
    # we can distinguish transient delivery failures from final file outcomes.
    return f"results/{test_case_folder}/raw/{run_id}/{filename}/{attempt_id}.json"


def _write_raw_record(test_case_folder: str, run_id: str, file_path: str, attempt_id: str, record: dict):
    """
    Persist one attempt-level processing record to GCS as JSON.

    This writes both successful and failed attempts so E3 can compute
    file-final outcomes separately from transient delivery failures.
    """
    # E3 adaptation: persist attempt-level processing records under
    # results/<test_case>/raw/<run_id>/<filename>/<attempt_id>.json.
    # This keeps E2-style KPI payload fields per delivery while preserving
    # retry history needed for E3 reliability metrics.
    blob_name = _safe_record_blob_name(
        test_case_folder=test_case_folder,
        run_id=run_id,
        file_path=file_path,
        attempt_id=attempt_id,
    )
    bucket = ScanLifecycleService.storage_client().bucket(E3_RESULTS_BUCKET)
    bucket.blob(blob_name).upload_from_string(
        json.dumps(record),
        content_type="application/json",
    )


def _event_payload_to_file_path(payload: dict) -> tuple[str | None, str | None, dict]:
    """
    Extract `gs://bucket/object` and object metadata from Eventarc payload.

    Supports both canonical CloudEvent body shape (`data` object) and
    legacy direct-data payload shape.
    """
    event_data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(event_data, dict):
        return None, None, {}

    bucket = event_data.get("bucket")
    object_name = event_data.get("name")
    metadata = event_data.get("metadata") if isinstance(event_data.get("metadata"), dict) else {}
    if not bucket or not object_name:
        return None, None, metadata
    return f"gs://{bucket}/{object_name}", object_name, metadata


def _normalize_ingest_payload(payload: dict) -> tuple[dict | None, dict | None, int | None]:
    """
    Validate and normalize Eventarc payload into the ingestion contract.

    Returns:
    - normalized payload dict on success
    - or early response body + status code for ignored/bad requests
    """
    # E3 contract: /scan/ingest accepts GCS object-finalized event payloads only.
    # Like E2, this route still normalizes into the same ingest contract:
    # file_path + test_case + run_id + month (plus E3 test_case_folder routing).
    file_path, object_name, metadata = _event_payload_to_file_path(payload)
    if not file_path or not object_name:
        return (
            None,
            {
                "error": "bad request",
                "details": "Expected GCS finalize payload with data.bucket and data.name",
            },
            400,
        )

    # Double guard (1/2): Eventarc trigger is bucket-level; 
    # ignore finalized objects that are not benchmark input files.
    if "/input/" not in object_name:
        return None, {"status": "ignored", "reason": "non-input object", "file_path": file_path}, 200
    # Double guard (2/2): ignore finalized non-CSV objects under input/ to
    # keep ingest processing limited to expected scan files.
    if not object_name.lower().endswith(".csv"):
        return None, {"status": "ignored", "reason": "non-csv object", "file_path": file_path}, 200

    required_metadata_fields = ("test_case", "test_case_folder", "run_id", "month")
    missing = [
        key for key in required_metadata_fields
        if not isinstance(metadata.get(key), str) or not metadata.get(key).strip()
    ]
    if missing:
        return (
            None,
            {
                "error": "bad request",
                "details": f"Missing required object metadata: {', '.join(missing)}",
            },
            400,
        )

    return (
        {
            "file_path": file_path,
            "test_case": metadata["test_case"].strip(),
            "test_case_folder": metadata["test_case_folder"].strip(),
            "run_id": metadata["run_id"].strip(),
            "month": metadata["month"].strip(),
        },
        None,
        None,
    )


@bp.post("/ingest")
def ingest_file():
    """
    Event-driven ingestion endpoint.

    Accepts only GCS object-finalized payloads and processes CSV inputs under
    input/ prefixes. Request-level response is an acknowledgment; detailed
    per-file KPI records are persisted as JSON under:
    results/<test_case_folder>/raw/<run_id>/<filename>/<attempt_id>.json

    KPI/timing semantics (aligned with E2):
    - request_received_timestamp: captured at route entry
    - processing_start_timestamp: set inside orchestrator before file processing
    - processing_finished_timestamp: set when business processing finishes
    - response_finished_timestamp: set just before response return
    - queue_wait_time_sec, processing_time_sec, end_to_end_latency_sec are
      computed from the above timestamps in the same way as E2.

    Eventarc delivery model note:
    - Eventarc is at-least-once delivery, so the same finalized object event
      may be delivered more than once.
    - This handler is written to be idempotent for duplicates. After the first
      successful processing deletes/moves the source object, later duplicate
      deliveries hit NotFound and are acknowledged as "skipped" (HTTP 200)
      instead of treated as failures.
    """
    request_received_timestamp = datetime.now(timezone.utc)
    payload = request.get_json(silent=True) or {}
    normalized_payload, early_body, early_status = _normalize_ingest_payload(payload)
    if early_body is not None:
        return jsonify(early_body), int(early_status or 400)

    file_path = normalized_payload.get("file_path") if normalized_payload else None
    test_case = normalized_payload.get("test_case") if normalized_payload else None
    test_case_folder = normalized_payload.get("test_case_folder") if normalized_payload else None
    run_id = normalized_payload.get("run_id") if normalized_payload else None
    month = normalized_payload.get("month") if normalized_payload else None
    # E3 adaptation: CloudEvent ID is unique per Eventarc delivery attempt and
    # is used to correlate retries for reliability KPI analysis.
    attempt_id = (request.headers.get("Ce-Id") or request.headers.get("ce-id") or "").strip()
    if not attempt_id:
        attempt_id = datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%S%fZ")

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
            test_case_folder=test_case_folder,
            run_id=run_id,
            file_path=file_path,
            attempt_id=attempt_id,
            record={
                "file_path": file_path,
                "status_code": 200,
                "ok": True,
                "error": None,
                **result,
            },
        )
        # Eventarc caller needs stable ack semantics (200 on handled delivery),
        # while full per-file/per-attempt KPI details are stored in raw records.
        return jsonify({"status": "processed", "file_path": file_path, "run_id": run_id}), 200
    except NotFound:
        # At-least-once delivery handling:
        # A duplicate event can arrive after another attempt already consumed
        # the source object. We acknowledge this duplicate as a no-op skip
        # (idempotent behavior), which prevents false failures from retries.
        return jsonify({"status": "skipped", "reason": "source object not found", "file_path": file_path}), 200
    except Exception as exc:
        _write_raw_record(
            test_case_folder=test_case_folder,
            run_id=run_id,
            file_path=file_path,
            attempt_id=attempt_id,
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
        )
        return jsonify({"error": "internal error", "details": str(exc)}), 500
