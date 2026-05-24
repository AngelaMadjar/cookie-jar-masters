from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from app.daos.tracker.tracker_cmp_dao import TrackerCmpDAO
from app.daos.tracker.tracker_dao import TrackerDAO
from app.services.tracker_ingestion.reference_data_service import ReferenceMaps

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

@dataclass
class UpsertResult:
    """
    Result payload from tracker upsert execution.

    - tracker_map: tracker identity key -> tracker_id for existing and inserted
    - new_tracker_ids: set of tracker IDs created in this run
    - created_count: number of newly created trackers
    - existing_count: number of rows resolved to already-existing trackers
    """
    tracker_map: dict[tuple[str, int, int], int]
    new_tracker_ids: set[int]
    created_count: int
    existing_count: int


class TrackerUpsertService:
    """
    Resolves ingestion rows into tracker records using upsert-like logic.

    Identity key used for matching/insertion:
        (tracker_name, tracker_type_id, tracking_domain_id)

    Existing keys are reused; missing keys are inserted. CMP links are ensured
    for both existing and newly created trackers.
    """

    @staticmethod
    def _now_utc():
        """
        Returns the current UTC timestamp.
        """
        return datetime.now(timezone.utc)

    @staticmethod
    def upsert_trackers(
        df: pd.DataFrame,
        refs: ReferenceMaps,
        cmp_name: str,
        source_name: str,
    ) -> UpsertResult:
        """
        Upserts tracker records from normalized ingestion rows.

        Flow:
        1. Resolve FK IDs for each row from ReferenceMaps.
        2. Build candidate tracker identity keys.
        3. Detection step: query DB once for those keys to build an
           existing-key -> tracker_id map.
        4. Row-handling step for keys found in that map:
           - count as existing
           - ensure tracker<->cmp link exists
        5. Row-handling step for keys not found in that map:
           - prepare insert rows
           - bulk insert trackers
           - reload inserted IDs by key
           - bulk link new trackers to cmp

        Returns UpsertResult with combined tracker_map and created/existing
        counters used by downstream services and reporting.
        """
        cmp_id = refs.cmps.get(cmp_name)
        source_id = refs.tracker_sources.get(source_name)

        prepared_rows: list[dict] = []
        candidate_keys: set[tuple[str, int, int]] = set()

        for _, r in df.iterrows():
            vendor_name = r.get("vendor_name")
            vendor_id = refs.vendors.get(vendor_name) if vendor_name else None

            domain = r.get("tracking_domain")
            domain_id = refs.tracking_domains.get((domain, vendor_id)) if domain else None

            type_id = refs.tracker_types.get(r.get("tracker_type"))
            category_id = refs.tracker_categories.get(r.get("consent_category"))
            tracker_name = r.get("tracker_name")

            if not (domain_id and type_id and category_id and source_id and tracker_name):
                continue

            key = (tracker_name, type_id, domain_id)
            prepared_rows.append(
                {
                    "key": key,
                    "tracker_name": tracker_name,
                    "tracker_duration": r.get("tracker_duration"),
                    "tracker_type_id": type_id,
                    "tracker_category_id": category_id,
                    "tracker_source_id": source_id,
                    "vendor_id": vendor_id,
                    "tracking_domain_id": domain_id,
                }
            )
            candidate_keys.add(key)

        tracker_map = TrackerDAO.get_trackers_by_keys(candidate_keys)

        new_rows = []
        keys_to_insert = []
        existing_count = 0

        for row in prepared_rows:
            key = row["key"]
            tracker_id = tracker_map.get(key)
            if tracker_id:
                existing_count += 1
                if cmp_id and not TrackerCmpDAO.get(tracker_id, cmp_id):
                    TrackerCmpDAO.create(tracker_id, cmp_id, True)
                continue

            keys_to_insert.append(key)
            new_rows.append(
                {
                    "tracker_name": row["tracker_name"],
                    "tracker_duration": row["tracker_duration"],
                    "last_modified": TrackerUpsertService._now_utc(),
                    "tracker_type_id": row["tracker_type_id"],
                    "tracker_category_id": row["tracker_category_id"],
                    "tracker_source_id": row["tracker_source_id"],
                    "vendor_id": row["vendor_id"],
                    "tracking_domain_id": row["tracking_domain_id"],
                }
            )

        TrackerDAO.bulk_insert_trackers(new_rows)

        inserted_map = TrackerDAO.get_trackers_by_keys(set(keys_to_insert))
        tracker_map.update(inserted_map)
        new_tracker_ids = {inserted_map[k] for k in keys_to_insert if k in inserted_map}

        if cmp_id and new_tracker_ids:
            links = [{"tracker_id": tid, "cmp_id": cmp_id, "is_active": True} for tid in new_tracker_ids]
            TrackerCmpDAO.bulk_insert_tracker_cmp(links)

        return UpsertResult(
            tracker_map=tracker_map,
            new_tracker_ids=new_tracker_ids,
            created_count=len(new_tracker_ids),
            existing_count=existing_count,
        )
