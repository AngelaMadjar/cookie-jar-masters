from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_category import TrackerCategory


class TrackerCategoryDAO:
    """
        Data Access Object for tracker category lookup values.

        This DAO encapsulates persistence operations for the TrackerCategory model,
        which stores normalized consent-category values used by Tracker rows.
    """

    @staticmethod
    def bulk_insert_tracker_categories(categories: list[str]):
        """
        Inserts tracker category values in bulk.

        Null values are ignored. Existing rows are preserved via PostgreSQL
        ON CONFLICT DO NOTHING on the unique tracker_category key.
        """
        rows = [{"tracker_category": c} for c in categories if c is not None]
        if not rows:
            return
        query = insert(TrackerCategory).values(rows).on_conflict_do_nothing(
            index_elements=["tracker_category"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_tracker_categories():
        """
        Returns all tracker categories as a lookup map.

        Output format:
            {tracker_category_text: tracker_category_id}
        """
        return {c.tracker_category: c.tracker_category_id for c in TrackerCategory.query.all()}

    @staticmethod
    def delete_all():
        """
        Deletes all tracker category rows and commits the transaction.
        """
        TrackerCategory.query.delete(synchronize_session=False)
        db.session.commit()
