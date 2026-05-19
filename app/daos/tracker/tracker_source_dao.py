from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_source import TrackerSource


class TrackerSourceDAO:
    @staticmethod
    def bulk_insert_tracker_sources(sources: list[str]):
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
        return {s.tracker_source: s.tracker_source_id for s in TrackerSource.query.all()}

    @staticmethod
    def delete_all():
        TrackerSource.query.delete(synchronize_session=False)
        db.session.commit()
