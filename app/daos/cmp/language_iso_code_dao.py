from typing import Optional
from sqlalchemy import select

from app import db
from app.models.cmp.language_iso_code import LanguageIsoCode


class LanguageIsoCodeDAO:
    @staticmethod
    def get(language_iso_code: str):
        return db.session.execute(
            select(LanguageIsoCode).where(LanguageIsoCode.language_iso_code == language_iso_code)
        ).scalar_one_or_none()

    @staticmethod
    def get_or_create(language_iso_code: str, language_name: Optional[str] = None) -> LanguageIsoCode:
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
        LanguageIsoCode.query.delete(synchronize_session=False)
        db.session.commit()
