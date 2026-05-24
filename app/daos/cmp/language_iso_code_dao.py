from typing import Optional
from sqlalchemy import select

from app import db
from app.models.cmp.language_iso_code import LanguageIsoCode


class LanguageIsoCodeDAO:
    """
        Data Access Object for language ISO code lookup records.

        This DAO manages read/create operations for LanguageIsoCode rows used by
        CMP translation mappings and localization targeting.
    """

    @staticmethod
    def get(language_iso_code: str):
        """
        Returns a language row by ISO code, or None if not found.
        """
        return db.session.execute(
            select(LanguageIsoCode).where(LanguageIsoCode.language_iso_code == language_iso_code)
        ).scalar_one_or_none()

    @staticmethod
    def get_or_create(language_iso_code: str, language_name: Optional[str] = None) -> LanguageIsoCode:
        """
        Returns an existing language row or creates a new one.

        If language_name is not provided, the ISO code is used as a fallback
        display name. New rows are added and flushed so the returned entity has
        its primary key available within the current transaction.
        """
        instance = LanguageIsoCodeDAO.get(language_iso_code)
        if instance:
            return instance

        instance = LanguageIsoCode(
            language_iso_code=language_iso_code,
            language_name=language_name or language_iso_code,
        )
        db.session.add(instance)
        db.session.flush()
        return instance

    @staticmethod
    def delete_all():
        """
        Deletes all language ISO code rows and commits the transaction.
        """
        LanguageIsoCode.query.delete(synchronize_session=False)
        db.session.commit()
