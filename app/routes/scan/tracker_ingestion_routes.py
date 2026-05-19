from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from app.services.tracker_ingestion import TrackerIngestionOrchestrator

bp = Blueprint("tracker_ingestion", __name__, url_prefix="/scan")


@bp.post("/ingest")
def ingest_file():
    request_received_timestamp = datetime.now(timezone.utc)
    payload = request.get_json(silent=True) or {}
    file_path = payload.get("file_path")
    test_case = payload.get("test_case")
    run_id = payload.get("run_id")
    month = payload.get("month", "2026-02")

    if not file_path or not isinstance(file_path, str):
        return jsonify({"error": "bad request", "details": "file_path is required"}), 400
    if not test_case or not isinstance(test_case, str):
        return jsonify({"error": "bad request", "details": "test_case is required"}), 400
    if not run_id or not isinstance(run_id, str):
        return jsonify({"error": "bad request", "details": "run_id is required"}), 400
    if not month or not isinstance(month, str):
        return jsonify({"error": "bad request", "details": "month is required"}), 400

    # E2 adjustment: local filesystem validation was replaced with strict GCS path validation.
    if not file_path.startswith("gs://"):
        return jsonify(
            {
                "error": "bad request",
                "details": "file_path must be a gs:// URI",
            }
        ), 400
    if not file_path.startswith("gs://e2-data/"):
        return jsonify(
            {
                "error": "bad request",
                "details": "file_path must be under gs://e2-data/",
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
        return jsonify(result), 200
    except Exception as exc:
        return jsonify({"error": "internal error", "details": str(exc)}), 500
