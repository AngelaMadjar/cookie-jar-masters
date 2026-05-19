from flask import Blueprint, jsonify, request
from app.services.database.seed_service import SeedService
from app.services.database.cleanup_service import CleanupService

bp = Blueprint("db", __name__, url_prefix="/db")


@bp.post("/populate/trackers")
def populate_trackers():
    try:
        payload = request.get_json(silent=True) or {}
        csv_paths = payload.get("csv_paths")
        if not csv_paths:
            csv_paths = [
                "data/seed/Master Cookie list & purposes (Global) - Jan 24(1P cookies) - Final.csv",
                "data/seed/Master Cookie list & purposes (Global) - Jan 24(3P cookies) - Final.csv",
            ]

        result = SeedService.populate_from_csv_files(csv_paths)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/populate/cmp")
def populate_cmp():
    try:
        payload = request.get_json(silent=True) or {}
        cmp_mappings_paths = payload.get("cmp_mappings_paths")
        if not cmp_mappings_paths:
            cmp_mappings_paths = ["data/seed/cmp_domain_locale_mapping(Locales).csv"]

        result = SeedService.populate_cmp_mappings_from_files(cmp_mappings_paths)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/empty")
def empty_all():
    try:
        result = CleanupService.empty_all()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/trackers/empty")
def empty_trackers():
    try:
        result = CleanupService.empty_trackers_only()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/vendors/empty")
def empty_vendors():
    try:
        result = CleanupService.empty_vendors_only()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500
