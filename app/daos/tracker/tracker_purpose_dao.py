from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.tracker.tracker_purpose import TrackerPurpose


class TrackerPurposeDAO:
    """
        Data Access Object for tracker purpose lookup values.

        This DAO manages persistence operations for the TrackerPurpose model,
        which stores normalized purpose text used by tracker-purpose associations.
    """

    @staticmethod
    def bulk_insert_tracker_purposes(purposes: list[str]):
        """
        Inserts tracker purpose values in bulk.

        Null values are ignored. Existing rows are preserved via PostgreSQL
        ON CONFLICT DO NOTHING on the unique tracker_purpose key.
        """
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
        """
        Returns all tracker purposes as a lookup map.

        Output format:
            {tracker_purpose_text: tracker_purpose_id}
        """
        return {p.tracker_purpose: p.tracker_purpose_id for p in TrackerPurpose.query.all()}
    
    @staticmethod
    def get_or_create(purpose_text: str) -> int:
        """
        Returns the purpose ID for a text value, creating it if missing.

        The method strips whitespace, attempts an insert with conflict-ignore,
        then selects and returns the canonical tracker_purpose_id.
        """
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
        """
        Deletes all tracker purpose rows and commits the transaction.
        """
        TrackerPurpose.query.delete(synchronize_session=False)
        db.session.commit()
