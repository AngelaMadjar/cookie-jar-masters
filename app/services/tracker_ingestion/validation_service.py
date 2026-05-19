from __future__ import annotations

import pandas as pd


class ValidationService:
    TRACKER_NAME_PATTERNS = [
        "_ak_abt_*",
        "_ga_*",
        "_hjSession_*",
        "_hjSessionUser_*",
        "AMCV_*",
        "AMCVS_*",
        "signifyd_id_*",
        "ttcsid_*",
    ]

    @staticmethod
    def classify_tracker_name(tracker_name: str | None):
        if not tracker_name:
            return None

        for pattern in ValidationService.TRACKER_NAME_PATTERNS:
            prefix = pattern.replace("*", "")
            if tracker_name.startswith(prefix):
                return tracker_name == pattern

        return None

    @staticmethod
    def split_by_tracker_name_pattern(df: pd.DataFrame):
        values = df["tracker_name"].astype(str)
        results = values.apply(ValidationService.classify_tracker_name)

        bad_mask = results == False
        bad_df = df[bad_mask].copy()
        good_df = df[~bad_mask].copy()

        return good_df, bad_df
