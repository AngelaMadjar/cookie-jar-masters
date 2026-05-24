from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_type import TrackerType


class TrackerTypeDAO:
    """
        Data Access Object for tracker type lookup values.

        This DAO centralizes persistence operations for the TrackerType model,
        which stores normalized tracker-technology/type labels used by Tracker rows.
    """

    @staticmethod
    def bulk_insert_tracker_types(tracker_types: list[str]):
        """
        Inserts tracker type values in bulk.

        Null values are ignored. Existing rows are preserved via PostgreSQL
        ON CONFLICT DO NOTHING on the unique tracker_type key.
        """
        rows = [{"tracker_type": t} for t in tracker_types if t is not None]
        if not rows:
            return
        query = insert(TrackerType).values(rows).on_conflict_do_nothing(
            index_elements=["tracker_type"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_tracker_types():
        """
        Returns all tracker types as a lookup map.

        Output format:
            {tracker_type_text: tracker_type_id}
        """
        return {t.tracker_type: t.tracker_type_id for t in TrackerType.query.all()}

    @staticmethod
    def delete_all():
        """
        Deletes all tracker type rows and commits the transaction.
        """
        TrackerType.query.delete(synchronize_session=False)
        db.session.commit()
