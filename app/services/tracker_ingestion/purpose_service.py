from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from app.daos.tracker.tracker_purposes_dao import TrackerPurposesDAO
from app.services.tracker_ingestion.reference_data_service import ReferenceMaps


class PurposeService:
    @staticmethod
    def _now_utc():
        return datetime.now(timezone.utc)

    @staticmethod
    def _purpose_id_for_row(row: pd.Series, refs: ReferenceMaps):
        purpose_text = row.get("tracker_purpose")
        if purpose_text:
            return refs.tracker_purposes.get(purpose_text)
        return None

    @staticmethod
    def create_purpose_links(df: pd.DataFrame, refs: ReferenceMaps, tracker_map: dict[tuple[str, int, int], int]):
        links = []

        for _, row in df.iterrows():
            vendor_name = row.get("vendor_name")
            vendor_id = refs.vendors.get(vendor_name) if vendor_name else None

            domain = row.get("tracking_domain")
            domain_id = refs.tracking_domains.get((domain, vendor_id)) if domain else None
            type_id = refs.tracker_types.get(row.get("tracker_type"))

            if not (domain_id and type_id):
                continue

            key = (row.get("tracker_name"), type_id, domain_id)
            tracker_id = tracker_map.get(key)
            purpose_id = PurposeService._purpose_id_for_row(row, refs)

            if not tracker_id or not purpose_id:
                continue

            links.append(
                {
                    "tracker_id": tracker_id,
                    "tracker_purpose_id": purpose_id,
                    "created_at": PurposeService._now_utc(),
                    "is_current": True,
                }
            )

        TrackerPurposesDAO.bulk_insert_tracker_purpose_links(links)
        return len(links)
