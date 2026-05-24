from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from google.cloud import storage


class ScanLifecycleService:
    """
    Lifecycle service for scan-file ingestion artifacts.

    e1-local branch behavior:
    - files are read from monthly local input folders
    - processed/failed CSVs are written to monthly local output folders
    - consumed input files are removed from local filesystem

    e2-cloudrun-http branch behavior:
    - input scans are read from GCS objects (gs:// URIs)
    - processed/failed CSVs are uploaded to GCS output prefixes
    - consumed source objects are deleted from GCS input prefix

    e3-cloudrun-eventdriven branch behavior:
    - scans are finalized objects in a dedicated trigger bucket
      (e3-data-benchmarks) watched by Eventarc, copied into /input
      folder in e3-data-monthly-audit-trackers
    - input/ processed/failed outputs are written to a separate 
      runtime/results bucket (e3-data-monthly-audit-trackers)
    - consumed source objects are deleted after outputs are persisted

    Why e3 splits input and output buckets:
    - Eventarc is configured on object-finalized events for the e3-data-benchmarks bucket.
    - If ingestion also wrote artifacts back into that same bucket, each write
      would generate additional finalize events.
    - Those artifact-generated events are not real input work and can cause
      noisy re-invocations or accidental recursive processing patterns.
    - Keeping outputs in a separate bucket makes the event flow deterministic:
      only staged input files can trigger ingestion.
    """

    BASE_DIR = Path("data/monthly_tracker_audits")
    BENCHMARKS_DIR = Path("data/benchmarks")
    _storage_client: storage.Client | None = None
    RUNTIME_RESULTS_BUCKET = "e3-data-monthly-audit-trackers"

    @staticmethod
    def storage_client() -> storage.Client:
        """
        Returns a cached Google Cloud Storage client instance.
        """
        if ScanLifecycleService._storage_client is None:
            ScanLifecycleService._storage_client = storage.Client()
        return ScanLifecycleService._storage_client

    @staticmethod
    def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
        """
        Parse a gs:// URI into (bucket_name, blob_name).
        """
        if not gs_uri.startswith("gs://"):
            raise ValueError(f"Expected gs:// path, got: {gs_uri}")
        path = gs_uri[len("gs://") :]
        bucket, sep, blob = path.partition("/")
        if not sep or not bucket or not blob:
            raise ValueError(f"Invalid gs:// path: {gs_uri}")
        return bucket, blob

    @staticmethod
    def read_csv_from_gcs(gs_uri: str) -> pd.DataFrame:
        """
        Download a CSV from GCS and parse it with encoding fallback.

        Tries utf-8-sig first, then latin-1.
        """
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
        """
        Returns monthly local input folder path.

        Kept for local utilities (for example benchmark staging helpers).
        """
        return ScanLifecycleService.BASE_DIR / "input" / month

    @staticmethod
    def month_processed_dir(month: str) -> Path:
        """
        Returns monthly local processed folder path.
        """
        return ScanLifecycleService.BASE_DIR / "processed" / month

    @staticmethod
    def month_failed_dir(month: str) -> Path:
        """
        Returns monthly local failed folder path.
        """
        return ScanLifecycleService.BASE_DIR / "failed" / month

    @staticmethod
    def list_input_files(month: str) -> list[Path]:
        """
        Lists CSV files currently present in local monthly input folder.
        """
        in_dir = ScanLifecycleService.month_input_dir(month)
        if not in_dir.exists():
            return []
        return sorted([p for p in in_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])

    @staticmethod
    def clear_month_csvs(month: str):
        """
        Ensure local monthly folders exist, then remove local CSV files from
        input/processed/failed folders.
        """
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
        """
        Copy benchmark workload CSV files into local monthly input folder.

        This helper is used for local benchmark staging flow and is not part of
        GCS ingest request handling.
        """
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
        """
        Persist per-file ingestion outputs in GCS and delete consumed source.

        E3-specific behavior:
        - source_file_gs_uri can come from an Eventarc trigger bucket
          (not restricted to monthly_tracker_audits/input/<month>/)
        - non-empty processed_df is uploaded to
          gs://e3-data-monthly-audit-trackers/processed/<month>/<filename>
        - non-empty failed_df is uploaded to
          gs://e3-data-monthly-audit-trackers/failed/<month>/<filename>
        - source object is deleted after output persistence
        - unlike E2, output objects are not written under
          monthly_tracker_audits/processed|failed/... path prefixes

        Rationale for cross-bucket write:
        - the trigger bucket emits object-finalized events that invoke ingestion
        - writing processed/failed artifacts back to that same trigger bucket
          would create additional finalized events
        - a separate results bucket prevents accidental recursive invocations
          and keeps event volume bounded and attributable to real input files

        At-least-once delivery/idempotency note:
        - source object deletion after successful persistence is intentional
        - if Eventarc redelivers the same finalized event, the source object is
          already gone, and route-level NotFound handling turns that duplicate
          invocation into a safe acknowledged skip

        Why source is deleted from input only after processed/failed write:
        - the input object is the single source-of-truth unit of work for one file
        - deleting it before outputs are durable creates a data-loss window
          (copy/read done but persistence failed)
        - deleting after output persistence gives "consume-on-success" behavior,
          which preserves reliability under retries and crashes
        """
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

        # Keep e2 lifecycle semantics: remove consumed source file after outputs are persisted.
        # This marks the file as fully consumed and prevents duplicate reprocessing
        # on later at-least-once Eventarc redeliveries.
        ScanLifecycleService.storage_client().bucket(bucket_name).blob(source_blob_name).delete()
