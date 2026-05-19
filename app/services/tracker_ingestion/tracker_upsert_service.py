from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from app.daos.tracker.tracker_cmp_dao import TrackerCmpDAO
from app.daos.tracker.tracker_dao import TrackerDAO
from app.services.tracker_ingestion.reference_data_service import ReferenceMaps


@dataclass
class UpsertResult:
    tracker_map: dict[tuple[str, int, int], int]
    new_tracker_ids: set[int]
    created_count: int
    existing_count: int


class TrackerUpsertService:
    @staticmethod
    def _now_utc():
        return datetime.now(timezone.utc)

    @staticmethod
    def upsert_trackers(
        df: pd.DataFrame,
        refs: ReferenceMaps,
        cmp_name: str,
        source_name: str,
    ) -> UpsertResult:
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
