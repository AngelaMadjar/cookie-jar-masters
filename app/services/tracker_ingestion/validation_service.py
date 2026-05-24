from __future__ import annotations

import pandas as pd

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

class ValidationService:
    """
    Validation helpers for tracker-ingestion row filtering.

    The service currently validates tracker_name values against a configured
    wildcard-pattern list and separates accepted vs rejected rows.
    """

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
        """
        Classifies a tracker name against wildcard-style prefix patterns.

        Return semantics:
        - True:  name exactly equals a listed wildcard literal (e.g. "_ga_*")
        - False: name starts with a blocked prefix but is not the literal
        - None:  no blocking prefix match or empty value
        """
        if not tracker_name:
            return None

        for pattern in ValidationService.TRACKER_NAME_PATTERNS:
            prefix = pattern.replace("*", "")
            if tracker_name.startswith(prefix):
                return tracker_name == pattern

        return None

    @staticmethod
    def split_by_tracker_name_pattern(df: pd.DataFrame):
        """
        Splits rows into good and bad frames using tracker_name classification.

        Rows classified as False are treated as failed (bad_df). All other rows
        (True or None) are retained in good_df.
        """
        values = df["tracker_name"].astype(str)
        results = values.apply(ValidationService.classify_tracker_name)

        bad_mask = results == False
        bad_df = df[bad_mask].copy()
        good_df = df[~bad_mask].copy()

        return good_df, bad_df
