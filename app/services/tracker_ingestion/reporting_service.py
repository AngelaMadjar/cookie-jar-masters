from __future__ import annotations

import pandas as pd

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

class ReportingService:
    """
    Builds ingestion output dataframes and response summary fields.

    This service formats processed/failed row outputs and computes base
    per-file count KPIs:
    - processed_rows
    - failed_rows
    - created_trackers
    - existing_trackers

    Timing/concurrency KPIs (for example queue_wait_time_sec, processing_time_sec,
    end_to_end_latency_sec, active_processing_requests) are added later by the
    ingestion orchestrator.
    """

    @staticmethod
    def build_output_frames(good_df: pd.DataFrame, bad_df: pd.DataFrame):
        """
        Returns processed and failed output frames from validated inputs.

        - processed_df is a copy of good_df
        - failed_df is a copy of bad_df with a normalized error reason column
        """
        processed_df = good_df.copy()
        failed_df = bad_df.copy()

        if not failed_df.empty:
            failed_df["error"] = "tracker_name_pattern_not_applied"

        return processed_df, failed_df

    @staticmethod
    def summarize(file_name: str, processed_df: pd.DataFrame, failed_df: pd.DataFrame, created: int, existing: int, purpose_links: int):
        """
        Builds base per-file ingestion counters for API response payload.

        Note: purpose_links is accepted for compatibility with caller flow but
        is not currently emitted in the returned summary dictionary.
        """
        return {
            "file": file_name,
            "processed_rows": int(len(processed_df)),
            "failed_rows": int(len(failed_df)),
            "created_trackers": int(created),
            "existing_trackers": int(existing),
        }
