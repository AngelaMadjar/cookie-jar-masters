from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_category import TrackerCategory


class TrackerCategoryDAO:
    @staticmethod
    def bulk_insert_tracker_categories(categories: list[str]):
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
        return {c.tracker_category: c.tracker_category_id for c in TrackerCategory.query.all()}

    @staticmethod
    def delete_all():
        TrackerCategory.query.delete(synchronize_session=False)
        db.session.commit()
