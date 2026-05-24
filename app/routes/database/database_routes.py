from flask import Blueprint, jsonify, request
from app.services.database.seed_service import SeedService
from app.services.database.cleanup_service import CleanupService

bp = Blueprint("db", __name__, url_prefix="/db")


@bp.post("/populate/trackers")
def populate_trackers():
    """
    Seeds tracker-related tables from one or more CSV files.

    Optional JSON payload:
    - csv_paths: list[str]

    The endpoint invokes seeding with these two default files:
    - data/seed/Master Cookie list & purposes (Global) - Jan 24(1P cookies) - Final.csv
    - data/seed/Master Cookie list & purposes (Global) - Jan 24(3P cookies) - Final.csv
    """
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
    """
    Seeds CMP translation mapping tables from one or more CSV files.

    Optional JSON payload:
    - cmp_mappings_paths: list[str]

    The endpoint invokes seeding with:
    - data/seed/cmp_domain_locale_mapping(Locales).csv
    """
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
    """
    Deletes all rows across tracker/vendor/CMP-related tables.
    """
    try:
        result = CleanupService.empty_all()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/trackers/empty")
def empty_trackers():
    """
    Deletes tracker-side data while leaving other domains untouched.
    """
    try:
        result = CleanupService.empty_trackers_only()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500


@bp.post("/vendors/empty")
def empty_vendors():
    """
    Deletes vendor-side data while leaving other domains untouched.
    """
    try:
        result = CleanupService.empty_vendors_only()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": "internal error", "details": str(e)}), 500
