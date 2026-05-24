from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

class ScanLifecycleService:
    """
    Filesystem lifecycle service for monthly scan ingestion artifacts.

    Manages where input files are read from, where processed/failed outputs are
    written, and how benchmark workloads are staged into monthly runtime folders.

    Important behavior:
    - Validation failures are row-level (from ValidationService tracker-name
      rules), not file-level.
    - For each ingested source file, failed rows are written under the same
      filename in the monthly /failed folder.
    - Valid/processed rows from that same source file are written under the
      same filename in the monthly /processed folder.
    - Therefore, a file can produce both processed and failed outputs; it is
      not treated as entirely failed just because some rows fail validation.
    """

    BASE_DIR = Path("data/monthly_tracker_audits")
    BENCHMARKS_DIR = Path("data/benchmarks")

    @staticmethod
    def month_input_dir(month: str) -> Path:
        """
        Returns the monthly input folder path.
        """
        return ScanLifecycleService.BASE_DIR / "input" / month

    @staticmethod
    def month_processed_dir(month: str) -> Path:
        """
        Returns the monthly processed-output folder path.
        """
        return ScanLifecycleService.BASE_DIR / "processed" / month

    @staticmethod
    def month_failed_dir(month: str) -> Path:
        """
        Returns the monthly failed-output folder path.
        """
        return ScanLifecycleService.BASE_DIR / "failed" / month

    @staticmethod
    def list_input_files(month: str) -> list[Path]:
        """
        Lists CSV input files currently staged for a month.
        """
        in_dir = ScanLifecycleService.month_input_dir(month)
        if not in_dir.exists():
            return []
        return sorted([p for p in in_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])

    @staticmethod
    def clear_month_csvs(month: str):
        """
        Ensures monthly input/processed/failed folders exist, then removes all
        CSV files from those folders.
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
        Copies workload CSV files into the monthly runtime input folder.

        Source path:
            data/benchmarks/<workload>/input/<month>

        Before staging, monthly input/processed/failed CSVs are cleared to keep
        run state deterministic. Returns number of staged files.
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
            shutil.copy2(source_file, target_dir / source_file.name)

        return len(source_files)

    @staticmethod
    def persist_results(month: str, source_file: Path, processed_df: pd.DataFrame, failed_df: pd.DataFrame):
        """
        Persists ingestion outputs for a single source file.

        - processed rows are written to monthly processed folder (if non-empty)
        - failed rows are written to monthly failed folder (if non-empty)
        - source input file is removed from monthly input folder after handling
        """
        processed_dir = ScanLifecycleService.month_processed_dir(month)
        failed_dir = ScanLifecycleService.month_failed_dir(month)
        processed_dir.mkdir(parents=True, exist_ok=True)
        failed_dir.mkdir(parents=True, exist_ok=True)

        if not processed_df.empty:
            processed_df.to_csv(processed_dir / source_file.name, index=False)

        if not failed_df.empty:
            failed_df.to_csv(failed_dir / source_file.name, index=False)

        source_file.unlink(missing_ok=True)
