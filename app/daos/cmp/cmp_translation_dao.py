from sqlalchemy import select

from app import db
from app.models.cmp.cmp_translation import CmpTranslation


class CmpTranslationDAO:
    """
        Data Access Object for CMP translation lookup records.

        This DAO handles read/create operations for CmpTranslation rows used by
        CMP-to-locale/language mapping data.
    """

    @staticmethod
    def get(cmp_translation: str):
        """
        Returns a CMP translation entity by text value, or None if not found.
        """
        return db.session.execute(
            select(CmpTranslation).where(CmpTranslation.cmp_translation == cmp_translation)
        ).scalar_one_or_none()

    @staticmethod
    def get_or_create(cmp_translation: str) -> CmpTranslation:
        """
        Returns an existing CMP translation row or creates a new one.

        New rows are added and flushed so the returned entity has its primary
        key available within the current transaction.
        """
        instance = CmpTranslationDAO.get(cmp_translation)
        if instance:
            return instance

        instance = CmpTranslation(cmp_translation=cmp_translation)
        db.session.add(instance)
        db.session.flush()
        return instance

    @staticmethod
    def delete_all():
        """
        Deletes all CMP translation rows and commits the transaction.
        """
        CmpTranslation.query.delete(synchronize_session=False)
        db.session.commit()
