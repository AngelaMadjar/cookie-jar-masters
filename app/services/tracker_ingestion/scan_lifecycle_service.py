from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage


class ScanLifecycleService:
    BASE_DIR = Path("data/monthly_tracker_audits")
    BENCHMARKS_DIR = Path("data/benchmarks")
    _storage_client: storage.Client | None = None
    RUNTIME_RESULTS_BUCKET = "e3-data-monthly-audit-trackers"

    @staticmethod
    def storage_client() -> storage.Client:
        # E3 adaptation: lifecycle operations use GCS object storage during cloud benchmarks.
        if ScanLifecycleService._storage_client is None:
            ScanLifecycleService._storage_client = storage.Client()
        return ScanLifecycleService._storage_client

    @staticmethod
    def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
        if not gs_uri.startswith("gs://"):
            raise ValueError(f"Expected gs:// path, got: {gs_uri}")
        path = gs_uri[len("gs://") :]
        bucket, sep, blob = path.partition("/")
        if not sep or not bucket or not blob:
            raise ValueError(f"Invalid gs:// path: {gs_uri}")
        return bucket, blob

    @staticmethod
    def read_csv_from_gcs(gs_uri: str) -> pd.DataFrame:
        bucket_name, blob_name = ScanLifecycleService.parse_gs_uri(gs_uri)
        blob = ScanLifecycleService.storage_client().bucket(bucket_name).blob(blob_name)
        payload = blob.download_as_bytes()
        last_exc = None
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(BytesIO(payload), sep=",", dtype=str, encoding=encoding, keep_default_na=False)
            except Exception as exc:  # pragma: no cover
                last_exc = exc
        raise last_exc

    @staticmethod
    def month_input_dir(month: str) -> Path:
        return ScanLifecycleService.BASE_DIR / "input" / month

    @staticmethod
    def month_processed_dir(month: str) -> Path:
        return ScanLifecycleService.BASE_DIR / "processed" / month

    @staticmethod
    def month_failed_dir(month: str) -> Path:
        return ScanLifecycleService.BASE_DIR / "failed" / month

    @staticmethod
    def list_input_files(month: str) -> list[Path]:
        in_dir = ScanLifecycleService.month_input_dir(month)
        if not in_dir.exists():
            return []
        return sorted([p for p in in_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])

    @staticmethod
    def clear_month_csvs(month: str):
        dirs = [
            ScanLifecycleService.month_input_dir(month),
            ScanLifecycleService.month_processed_dir(month),
            ScanLifecycleService.month_failed_dir(month),
        ]
        for folder in dirs:
            folder.mkdir(parents=True, exist_ok=True)
            for existing in folder.glob("*.csv"):
                existing.unlink(missing_ok=True)

    @staticmethod
    def stage_workload_input(month: str, workload: str) -> int:
        source_dir = ScanLifecycleService.BENCHMARKS_DIR / workload / "input" / month
        if not source_dir.exists():
            raise FileNotFoundError(f"Workload input folder not found: {source_dir}")

        source_files = sorted([p for p in source_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])
        if not source_files:
            raise FileNotFoundError(f"No csv files found in workload input folder: {source_dir}")

        ScanLifecycleService.clear_month_csvs(month)
        target_dir = ScanLifecycleService.month_input_dir(month)

        for source_file in source_files:
            (target_dir / source_file.name).write_bytes(source_file.read_bytes())

        return len(source_files)

    @staticmethod
    def persist_results(month: str, source_file_gs_uri: str, processed_df: pd.DataFrame, failed_df: pd.DataFrame):
        # E3 adaptation: source files can originate from benchmark trigger bucket, but processed/failed outputs live in runtime bucket.
        bucket_name, source_blob_name = ScanLifecycleService.parse_gs_uri(source_file_gs_uri)
        filename = source_blob_name.rsplit("/", 1)[-1]

        processed_blob_name = f"processed/{month}/{filename}"
        failed_blob_name = f"failed/{month}/{filename}"

        results_bucket = ScanLifecycleService.storage_client().bucket(ScanLifecycleService.RUNTIME_RESULTS_BUCKET)
        if not processed_df.empty:
            results_bucket.blob(processed_blob_name).upload_from_string(
                processed_df.to_csv(index=False),
                content_type="text/csv",
            )

        if not failed_df.empty:
            results_bucket.blob(failed_blob_name).upload_from_string(
                failed_df.to_csv(index=False),
                content_type="text/csv",
            )

        try:
            ScanLifecycleService.storage_client().bucket(bucket_name).blob(source_blob_name).delete()
        except NotFound:
            # E3 adaptation: event retries can race after source delete, so missing object is treated as benign.
            pass
