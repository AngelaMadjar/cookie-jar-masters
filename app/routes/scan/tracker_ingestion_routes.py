from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, request

from app.services.tracker_ingestion import TrackerIngestionOrchestrator
from app.services.tracker_ingestion.scan_lifecycle_service import ScanLifecycleService

bp = Blueprint("tracker_ingestion", __name__, url_prefix="/scan")


@bp.post("/ingest")
def ingest_file():
    """
    Ingests exactly one staged scan CSV file and returns per-file KPI summary.

    Expected JSON payload:
    - file_path: absolute/relative path to a CSV staged under
      data/monthly_tracker_audits/input/<month>
    - test_case: benchmark case label (for example T1-T7)
    - run_id: benchmark run identifier
    - month: workload month folder (default: "2026-02")

    Validation behavior:
    - Rejects missing/invalid fields (400)
    - Rejects non-existent files (400)
    - Rejects file paths outside the expected monthly input folder (400)

    On success, delegates to TrackerIngestionOrchestrator and returns a JSON
    summary including count and timing/concurrency KPIs.
    """
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

    source_path = Path(file_path)
    if not source_path.exists() or not source_path.is_file():
        return jsonify({"error": "bad request", "details": f"file not found: {file_path}"}), 400
    expected_input_dir = ScanLifecycleService.month_input_dir(month).resolve()
    try:
        source_resolved = source_path.resolve()
        source_resolved.relative_to(expected_input_dir)
    except Exception:
        return jsonify(
            {
                "error": "bad request",
                "details": f"file_path must be under {expected_input_dir}",
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
