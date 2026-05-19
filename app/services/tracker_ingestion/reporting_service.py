from __future__ import annotations

import pandas as pd


class ReportingService:
    @staticmethod
    def build_output_frames(good_df: pd.DataFrame, bad_df: pd.DataFrame):
        processed_df = good_df.copy()
        failed_df = bad_df.copy()

        if not failed_df.empty:
            failed_df["error"] = "tracker_name_pattern_not_applied"

        return processed_df, failed_df

    @staticmethod
    def summarize(file_name: str, processed_df: pd.DataFrame, failed_df: pd.DataFrame, created: int, existing: int, purpose_links: int):
        return {
            "file": file_name,
            "processed_rows": int(len(processed_df)),
            "failed_rows": int(len(failed_df)),
            "created_trackers": int(created),
            "existing_trackers": int(existing),
        }
