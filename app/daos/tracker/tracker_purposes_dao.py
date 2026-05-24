from datetime import datetime, timezone
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_purposes import TrackerPurposes
from typing import Optional


def now_utc():
    """
    Returns the current UTC timestamp.
    """
    return datetime.now(timezone.utc)


class TrackerPurposesDAO:
    """
        Data Access Object for tracker-purpose association records.

        This DAO manages the TrackerPurposes table, which links trackers to
        purposes and tracks current vs historical associations via is_current.
    """

    @staticmethod
    def set_all_not_current(tracker_ids: list[int]):
        """
        Marks all current tracker purposes as not current for the given tracker_ids.
        """
        if not tracker_ids:
            return
        TrackerPurposes.query.filter(
            TrackerPurposes.tracker_id.in_(tracker_ids),
            TrackerPurposes.is_current.is_(True)
        ).update({"is_current": False}, synchronize_session=False)
        db.session.commit()

    @staticmethod
    def bulk_insert_tracker_purpose_links(links: list[dict]):
        """
        Inserts tracker-purpose association rows in bulk.

        If created_at is missing, it is filled with the current UTC timestamp.
        Existing rows are preserved via ON CONFLICT DO NOTHING on
        (tracker_id, tracker_purpose_id, is_current).
        """

        if not links:
            return

        for l in links:
            if "created_at" not in l:
                l["created_at"] = now_utc()

        query = insert(TrackerPurposes).values(links).on_conflict_do_nothing(
            index_elements=["tracker_id", "tracker_purpose_id", "is_current"] # unique constraint
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_current_purpose_id(tracker_id: int) -> Optional[int]:
        """
        Returns the current tracker_purpose_id for a tracker, or None if absent.
        """
        return db.session.execute(
            select(TrackerPurposes.tracker_purpose_id).where(
                TrackerPurposes.tracker_id == tracker_id,
                TrackerPurposes.is_current.is_(True),
            )
        ).scalar_one_or_none()
    
    @staticmethod
    def set_not_current(tracker_id: int) -> int:
        """
        Marks all current tracker purposes as not current for this tracker.
        Returns the number of rows updated.
        """
        result = db.session.execute(
            update(TrackerPurposes)
            .where(
                TrackerPurposes.tracker_id == tracker_id,
                TrackerPurposes.is_current.is_(True),
            )
            .values(is_current=False)
        )
        return result.rowcount or 0
    
    @staticmethod
    def insert_current(tracker_id: int, tracker_purpose_id: int, created_at):
        """
        Inserts a new current tracker-purpose link row.

        UNIQUE constraint:
            (tracker_id, tracker_purpose_id, is_current)
        """
        query = (
            insert(TrackerPurposes)
            .values([{
                "tracker_id": tracker_id,
                "tracker_purpose_id": tracker_purpose_id,
                "created_at": created_at,
                "is_current": True
            }])
            .on_conflict_do_nothing(
                index_elements=["tracker_id", "tracker_purpose_id", "is_current"]
            )
        )
        db.session.execute(query)

    @staticmethod
    def delete_all():
        """
        Deletes all tracker-purpose association rows and commits the transaction.
        """
        TrackerPurposes.query.delete(synchronize_session=False)
        db.session.commit()

    @staticmethod
    def delete_by_tracker_id(tracker_id: int) -> int:
        """
        Deletes all TrackerPurposes rows for a tracker.
        Returns number of deleted rows.
        """
        count = TrackerPurposes.query.filter_by(tracker_id=tracker_id).delete(
            synchronize_session=False
        )
        return count or 0
