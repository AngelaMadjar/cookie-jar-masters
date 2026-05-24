from sqlalchemy import update, func, tuple_
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker import Tracker
from app.models.tracker.tracker_category import TrackerCategory
from app.models.tracker.tracker_type import TrackerType
from app.models.tracker.tracking_domain import TrackingDomain
from app.models.vendor.vendor import Vendor
from app.models.tracker.tracker_purposes import TrackerPurposes
from app.models.tracker.tracker_purpose import TrackerPurpose
from typing import Optional


class TrackerDAO:
    """
        Data Access Object for core tracker records.

        This DAO encapsulates insert, lookup, listing, and maintenance operations
        for Tracker rows, using the tracker identity key:
            (tracker_name, tracker_type_id, tracking_domain_id)
    """

    TRACKER_KEY_QUERY_CHUNK_SIZE = 5000

    @staticmethod
    def bulk_insert_trackers(trackers: list[dict]):
        """
        Inserts tracker rows in bulk.

        Each input row should include:
            tracker_name, tracker_type_id, tracking_domain_id,
            tracker_category_id, tracker_source_id, vendor_id,
            tracker_duration, last_modified

        Existing rows are preserved via ON CONFLICT DO NOTHING on the tracker
        unique key (tracker_name, tracker_type_id, tracking_domain_id).
        """
        if not trackers:
            return

        query = insert(Tracker).values(trackers).on_conflict_do_nothing(
            index_elements=["tracker_name", "tracker_type_id", "tracking_domain_id"]  # unique constraint
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_trackers():
        """
        Returns all trackers as a lookup map.

        Output format:
            {(tracker_name, tracker_type_id, tracking_domain_id): tracker_id}
        """
        return {
            (t.tracker_name, t.tracker_type_id, t.tracking_domain_id): t.tracker_id
            for t in Tracker.query.all()
        }

    @staticmethod
    def get_trackers_by_keys(keys: set[tuple[str, int, int]]):
        """
        Returns tracker IDs for the provided identity keys only.

        Output format:
            {(tracker_name, tracker_type_id, tracking_domain_id): tracker_id}

        Keys are queried in chunks (TRACKER_KEY_QUERY_CHUNK_SIZE) to avoid
        oversized SQL IN clauses.
        """
        if not keys:
            return {}

        key_list = list(keys)
        result_map: dict[tuple[str, int, int], int] = {}

        for i in range(0, len(key_list), TrackerDAO.TRACKER_KEY_QUERY_CHUNK_SIZE):
            chunk = key_list[i : i + TrackerDAO.TRACKER_KEY_QUERY_CHUNK_SIZE]
            rows = (
                db.session.query(
                    Tracker.tracker_id,
                    Tracker.tracker_name,
                    Tracker.tracker_type_id,
                    Tracker.tracking_domain_id,
                )
                .filter(
                    tuple_(
                        Tracker.tracker_name,
                        Tracker.tracker_type_id,
                        Tracker.tracking_domain_id,
                    ).in_(chunk)
                )
                .all()
            )

            result_map.update(
                {
                    (r.tracker_name, r.tracker_type_id, r.tracking_domain_id): r.tracker_id
                    for r in rows
                }
            )

        return result_map
    
    @staticmethod
    def get_by_id(tracker_id: int) -> Optional[int]:
        """
        Returns a tracker entity by primary key, or None if not found.
        """
        return db.session.get(Tracker, tracker_id)
    
    @staticmethod
    def delete_all():
        """
        Deletes all tracker rows and commits the transaction.
        """
        Tracker.query.delete(synchronize_session=False)
        db.session.commit()

    @staticmethod
    def delete_by_id(tracker_id: int) -> int:
        """
        Deletes the tracker row.
        Returns number of deleted rows (0 or 1).
        """
        count = Tracker.query.filter_by(tracker_id=tracker_id).delete(
            synchronize_session=False
        )
        return count or 0

    @staticmethod
    def touch_last_modified(tracker_id: int, ts):
        """
        Updates only the last_modified timestamp for a tracker row.

        Note: this method executes the update but does not commit.
        """
        db.session.execute(
            update(Tracker)
            .where(Tracker.tracker_id == tracker_id)
            .values(last_modified=ts)
        )
        
    @staticmethod
    def list_trackers(limit: int = 500) -> list[dict]:
        """
        Returns a flattened tracker view for inspection/reporting.

        Includes joined fields for domain, category, vendor, type, tracker
        name, and current purpose (if present), limited by `limit`.
        """
        q = (
            db.session.query(
                Tracker.tracker_id.label("tracker_id"),
                TrackingDomain.tracking_domain.label("domain"),
                TrackerCategory.tracker_category.label("category"),
                Vendor.vendor_name.label("vendor"),
                TrackerType.tracker_type.label("type"),
                Tracker.tracker_name.label("name"),
                TrackerPurpose.tracker_purpose.label("purpose"),
            )
            .select_from(Tracker)
            .join(TrackingDomain, Tracker.tracking_domain_id == TrackingDomain.tracking_domain_id)
            .join(TrackerCategory, Tracker.tracker_category_id == TrackerCategory.tracker_category_id)
            .join(TrackerType, Tracker.tracker_type_id == TrackerType.tracker_type_id)
            .outerjoin(Vendor, Tracker.vendor_id == Vendor.vendor_id)
            .outerjoin(
                TrackerPurposes,
                (TrackerPurposes.tracker_id == Tracker.tracker_id) & (TrackerPurposes.is_current.is_(True))
            )
            .outerjoin(TrackerPurpose, TrackerPurpose.tracker_purpose_id == TrackerPurposes.tracker_purpose_id)
            .limit(limit)
        )

        return [
            {
                "tracker_id": r.tracker_id,
                "domain": r.domain,
                "category": r.category,
                "vendor": r.vendor,
                "type": r.type,
                "name": r.name,
                "purpose": r.purpose,
            }
            for r in q.all()
        ]
    
    @staticmethod
    def count_trackers() -> int:
        """
        Returns the total number of tracker rows.
        """
        return int(db.session.query(func.count(Tracker.tracker_id)).scalar() or 0)

    @staticmethod
    def get_sample_tracker_id():
        """
        Returns the smallest tracker_id in the table, or None if empty.
        """
        return (
            db.session.query(Tracker.tracker_id)
            .order_by(Tracker.tracker_id)
            .limit(1)
            .scalar()
        )
