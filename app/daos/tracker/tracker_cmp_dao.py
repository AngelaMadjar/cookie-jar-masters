from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app import db
from app.models.tracker.tracker_cmp import TrackerCmp


class TrackerCmpDAO:
    @staticmethod
    def bulk_insert_tracker_cmp(links: list[dict]):
        if not links:
            return

        query = insert(TrackerCmp).values(links).on_conflict_do_nothing(
            index_elements=["tracker_id", "cmp_id"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get(tracker_id: int, cmp_id: int):
        return db.session.execute(
            select(TrackerCmp).where(
                TrackerCmp.tracker_id == tracker_id,
                TrackerCmp.cmp_id == cmp_id,
            )
        ).scalar_one_or_none()

    @staticmethod
    def create(tracker_id: int, cmp_id: int, is_active: bool = True):
        query = insert(TrackerCmp).values(
            [{"tracker_id": tracker_id, "cmp_id": cmp_id, "is_active": is_active}]
        ).on_conflict_do_nothing(index_elements=["tracker_id", "cmp_id"])
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def delete_all():
        TrackerCmp.query.delete(synchronize_session=False)
        db.session.commit()
