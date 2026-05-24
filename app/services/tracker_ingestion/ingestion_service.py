from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.services.tracker_ingestion.scan_lifecycle_service import ScanLifecycleService
from app.services.tracker_ingestion.validation_service import ValidationService


@dataclass
class IngestionResult:
    """
    Normalized ingestion payload returned by IngestionService.

    - file_path/source_name/cmp_name describe source file context
    - good_df contains rows that continue through ingestion
    - bad_df contains validation-rejected rows

    E2 note:
    - file_path is a gs:// URI string (not a local Path object as in E1).
    """
    file_path: str
    source_name: str
    cmp_name: str
    good_df: pd.DataFrame
    bad_df: pd.DataFrame


class IngestionService:
    """
    Reads and normalizes scan CSV files before persistence services run.

    Responsibilities:
    - load CSV data
    - normalize source column names and missing required fields
    - clean string placeholders/whitespace
    - split rows into valid/invalid groups via ValidationService

    E2 branch changes vs E1:
    - input file_path is handled as a gs:// URI string
    - CSV loading is delegated to ScanLifecycleService.read_csv_from_gcs(...)
      instead of reading local files with pandas directly
    """

    COLUMN_RENAMES = {
        "Tracking Domain": "tracking_domain",
        "Consent Category": "consent_category",
        "New Consent Category": "consent_category",
        "Old Consent Category": "old_consent_category",
        "Vendor Name": "vendor_name",
        "Tracker Type": "tracker_type",
        "Tracker Name": "tracker_name",
        "Tracker Purpose": "tracker_purpose",
        "Tracker Purpose (Source)": "tracker_purpose_source",
        "Vendor Description": "vendor_description",
        "Tracker Duration": "tracker_duration",
    }

    REQUIRED_COLUMNS = [
        "tracking_domain",
        "consent_category",
        "vendor_name",
        "tracker_type",
        "tracker_name",
        "tracker_purpose",
        "vendor_description",
        "tracker_duration",
    ]

    @staticmethod
    def _extract_cmp_name(file_path: str) -> str:
        """
        Derives CMP name from filename prefix before the first '-'.

        For gs:// URIs, it extracts the final object filename first.
        """
        stem = file_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return stem.split("-")[0].strip() or "unknown_cmp"

    @staticmethod
    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes CSV columns and cleans textual values.

        - renames known source columns to internal field names
        - creates missing required columns with None
        - strips whitespace from string columns
        - converts empty strings and '-' placeholders to None
        """
        df = df.rename(columns=IngestionService.COLUMN_RENAMES)

        for col in IngestionService.REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        string_cols = df.select_dtypes(include=["object"]).columns
        if len(string_cols) > 0:
            df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())
        df = df.replace({"": None, "-": None})
        return df

    @staticmethod
    def load_and_validate(file_path: str) -> IngestionResult:
        """
        End-to-end ingestion preparation for a single GCS file.

        Returns IngestionResult with normalized metadata and good/bad row split.
        """
        source_name = file_path.rsplit("/", 1)[-1]
        cmp_name = IngestionService._extract_cmp_name(file_path)

        # E2-specific behavior: scan content is loaded from GCS object storage.
        raw_df = ScanLifecycleService.read_csv_from_gcs(file_path)
        normalized_df = IngestionService._normalize(raw_df)

        good_df, bad_df = ValidationService.split_by_tracker_name_pattern(normalized_df)
        return IngestionResult(
            file_path=file_path,
            source_name=source_name,
            cmp_name=cmp_name,
            good_df=good_df,
            bad_df=bad_df,
        )
