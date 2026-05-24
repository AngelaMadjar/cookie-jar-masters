from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from app.services.tracker_ingestion.validation_service import ValidationService

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

@dataclass
class IngestionResult:
    """
    Normalized ingestion payload returned by IngestionService.

    - file_path/source_name/cmp_name describe the source file context
    - good_df contains rows that can continue through ingestion
    - bad_df contains validation-rejected rows
    """
    file_path: Path
    source_name: str
    cmp_name: str
    good_df: pd.DataFrame
    bad_df: pd.DataFrame


class IngestionService:
    """
    Reads and normalizes scan CSV files before persistence services run.

    Responsibilities:
    - read CSV with encoding fallback
    - normalize source column names and missing required fields
    - clean string placeholders/whitespace
    - split rows into valid/invalid groups via ValidationService
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
    def _extract_cmp_name(file_path: Path) -> str:
        """
        Derives CMP name from the filename prefix before the first '-'.
        """
        # Local fallback convention: derive CMP from filename prefix before first dash.
        stem = file_path.stem
        return stem.split("-")[0].strip() or "unknown_cmp"

    @staticmethod
    def _read_csv(path: Path) -> pd.DataFrame:
        """
        Reads a CSV file using utf-8-sig first, then latin-1 as fallback.
        """
        last_exc = None
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(path, sep=",", dtype=str, encoding=encoding, keep_default_na=False)
            except Exception as exc:  # pragma: no cover - fallback path
                last_exc = exc
        raise last_exc

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
    def load_and_validate(file_path: Path) -> IngestionResult:
        """
        End-to-end ingestion preparation for a single file.

        Returns IngestionResult with:
        - normalized file metadata
        - rows split into good_df/bad_df by validation rules
        """
        source_name = file_path.name
        cmp_name = IngestionService._extract_cmp_name(file_path)

        raw_df = IngestionService._read_csv(file_path)
        normalized_df = IngestionService._normalize(raw_df)

        good_df, bad_df = ValidationService.split_by_tracker_name_pattern(normalized_df)
        return IngestionResult(
            file_path=file_path,
            source_name=source_name,
            cmp_name=cmp_name,
            good_df=good_df,
            bad_df=bad_df,
        )
