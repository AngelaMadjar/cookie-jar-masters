from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_purpose import TrackerPurpose


class TrackerPurposeDAO:
    @staticmethod
    def bulk_insert_tracker_purposes(purposes: list[str]):
        rows = [{"tracker_purpose": p} for p in purposes if p is not None]
        if not rows:
            return
        query = insert(TrackerPurpose).values(rows).on_conflict_do_nothing(
            index_elements=["tracker_purpose"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_tracker_purposes():
        return {p.tracker_purpose: p.tracker_purpose_id for p in TrackerPurpose.query.all()}
    
    @staticmethod
    def get_or_create(purpose_text: str) -> int:
        purpose_text = purpose_text.strip()

        query = (
            insert(TrackerPurpose)
            .values([{"tracker_purpose": purpose_text}])
            .on_conflict_do_nothing(index_elements=["tracker_purpose"])
        )
        db.session.execute(query)

        purpose_id = db.session.execute(
            select(TrackerPurpose.tracker_purpose_id).where(
                TrackerPurpose.tracker_purpose == purpose_text
            )
        ).scalar_one()

        return purpose_id

    @staticmethod
    def delete_all():
        TrackerPurpose.query.delete(synchronize_session=False)
        db.session.commit()
