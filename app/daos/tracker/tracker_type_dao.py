from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_type import TrackerType


class TrackerTypeDAO:
    @staticmethod
    def bulk_insert_tracker_types(tracker_types: list[str]):
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
        return {t.tracker_type: t.tracker_type_id for t in TrackerType.query.all()}

    @staticmethod
    def delete_all():
        TrackerType.query.delete(synchronize_session=False)
        db.session.commit()
