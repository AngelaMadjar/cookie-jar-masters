from app import db
from app.models.cmp.cmp import Cmp


class CmpDAO:
    """
        Data Access Object for CMP lookup records.

        This DAO manages persistence operations for the Cmp model, which stores
        Consent Management Platform names used across ingestion and mappings.
    """

    @staticmethod
    def bulk_insert_cmps(cmps: list[str]):
        """
        Inserts CMP values in bulk, skipping existing records.

        Falsy values are ignored. Existing values are detected first, then only
        missing CMP rows are added and committed.
        """
        values = [c for c in cmps if c]
        if not values:
            return

        existing = {c.cmp for c in Cmp.query.filter(Cmp.cmp.in_(values)).all()}
        missing = [Cmp(cmp=c) for c in values if c not in existing]
        if missing:
            db.session.add_all(missing)
            db.session.commit()

    @staticmethod
    def get_cmps() -> dict[str, int]:
        """
        Returns all CMP rows as a lookup map.

        Output format:
            {cmp_text: cmp_id}
        """
        return {c.cmp: c.cmp_id for c in Cmp.query.all()}

    @staticmethod
    def delete_all():
        """
        Deletes all CMP rows and commits the transaction.
        """
        Cmp.query.delete(synchronize_session=False)
        db.session.commit()
