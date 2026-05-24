from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_source import TrackerSource


class TrackerSourceDAO:
    """
        Data Access Object for tracker source lookup values.

        This DAO manages persistence operations for the TrackerSource model,
        which records the origin/source label associated with tracker records.
    """

    @staticmethod
    def bulk_insert_tracker_sources(sources: list[str]):
        """
        Inserts tracker source values in bulk.

        Null values are ignored. Existing rows are preserved via PostgreSQL
        ON CONFLICT DO NOTHING on the unique tracker_source key.
        """
        rows = [{"tracker_source": s} for s in sources if s is not None]
        if not rows:
            return
        query = insert(TrackerSource).values(rows).on_conflict_do_nothing(
            index_elements=["tracker_source"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_tracker_sources():
        """
        Returns all tracker sources as a lookup map.

        Output format:
            {tracker_source_text: tracker_source_id}
        """
        return {s.tracker_source: s.tracker_source_id for s in TrackerSource.query.all()}

    @staticmethod
    def delete_all():
        """
        Deletes all tracker source rows and commits the transaction.
        """
        TrackerSource.query.delete(synchronize_session=False)
        db.session.commit()
