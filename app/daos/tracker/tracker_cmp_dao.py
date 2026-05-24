from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app import db
from app.models.tracker.tracker_cmp import TrackerCmp


class TrackerCmpDAO:
    """
        Data Access Object for tracker-to-CMP association records.

        This DAO manages TrackerCmp rows that indicate whether a tracker is linked
        (and active) within a given CMP context.
    """

    @staticmethod
    def bulk_insert_tracker_cmp(links: list[dict]):
        """
        Inserts tracker-CMP association rows in bulk.

        Existing rows are preserved via ON CONFLICT DO NOTHING on
        (tracker_id, cmp_id).
        """
        if not links:
            return

        query = insert(TrackerCmp).values(links).on_conflict_do_nothing(
            index_elements=["tracker_id", "cmp_id"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get(tracker_id: int, cmp_id: int):
        """
        Returns a tracker-CMP association row for the given IDs, or None.
        """
        return db.session.execute(
            select(TrackerCmp).where(
                TrackerCmp.tracker_id == tracker_id,
                TrackerCmp.cmp_id == cmp_id,
            )
        ).scalar_one_or_none()

    @staticmethod
    def create(tracker_id: int, cmp_id: int, is_active: bool = True):
        """
        Inserts a single tracker-CMP association row.

        If the row already exists for (tracker_id, cmp_id), the insert is
        ignored due to ON CONFLICT DO NOTHING.
        """
        query = insert(TrackerCmp).values(
            [{"tracker_id": tracker_id, "cmp_id": cmp_id, "is_active": is_active}]
        ).on_conflict_do_nothing(index_elements=["tracker_id", "cmp_id"])
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def delete_all():
        """
        Deletes all tracker-CMP association rows and commits the transaction.
        """
        TrackerCmp.query.delete(synchronize_session=False)
        db.session.commit()
