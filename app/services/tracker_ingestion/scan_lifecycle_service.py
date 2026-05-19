from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from google.cloud import storage


class ScanLifecycleService:
    BASE_DIR = Path("data/monthly_tracker_audits")
    BENCHMARKS_DIR = Path("data/benchmarks")
    _storage_client: storage.Client | None = None

    @staticmethod
    def storage_client() -> storage.Client:
        # E2 adjustment: lifecycle operations now run against GCS instead of local monthly folders.
        if ScanLifecycleService._storage_client is None:
            ScanLifecycleService._storage_client = storage.Client()
        return ScanLifecycleService._storage_client

    @staticmethod
    def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
        # E2 adjustment: helper introduced to replace Path-based local parsing with gs:// parsing.
        if not gs_uri.startswith("gs://"):
            raise ValueError(f"Expected gs:// path, got: {gs_uri}")
        path = gs_uri[len("gs://"):]
        bucket, sep, blob = path.partition("/")
        if not sep or not bucket or not blob:
            raise ValueError(f"Invalid gs:// path: {gs_uri}")
        return bucket, blob

    @staticmethod
    def read_csv_from_gcs(gs_uri: str) -> pd.DataFrame:
        # E2 adjustment: helper introduced to replace local pd.read_csv(file_path) with GCS object reads.
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
        # E2 adjustment: local file move/write flow is replaced with GCS output + source object delete.
        # This method is part of the adaptation from local folder lifecycle to GCS object lifecycle.
        bucket_name, source_blob_name = ScanLifecycleService.parse_gs_uri(source_file_gs_uri)
        source_parts = source_blob_name.split("/")
        filename = source_parts[-1]
        source_prefix = f"monthly_tracker_audits/input/{month}/"
        if not source_blob_name.startswith(source_prefix):
            raise ValueError(
                f"Expected input object under {source_prefix}, got: {source_file_gs_uri}"
            )
        processed_blob_name = f"monthly_tracker_audits/processed/{month}/{filename}"
        failed_blob_name = f"monthly_tracker_audits/failed/{month}/{filename}"
        bucket = ScanLifecycleService.storage_client().bucket(bucket_name)
        if not processed_df.empty:
            processed_blob = bucket.blob(processed_blob_name)
            processed_blob.upload_from_string(processed_df.to_csv(index=False), content_type="text/csv")

        if not failed_df.empty:
            failed_blob = bucket.blob(failed_blob_name)
            failed_blob.upload_from_string(failed_df.to_csv(index=False), content_type="text/csv")

        bucket.blob(source_blob_name).delete()
