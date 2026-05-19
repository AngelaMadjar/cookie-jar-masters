from sqlalchemy import select

from app import db
from app.models.cmp.cmp_translation import CmpTranslation


class CmpTranslationDAO:
    @staticmethod
    def get(cmp_translation: str):
        return db.session.execute(
            select(CmpTranslation).where(CmpTranslation.cmp_translation == cmp_translation)
        ).scalar_one_or_none()

    @staticmethod
    def get_or_create(cmp_translation: str) -> CmpTranslation:
        instance = CmpTranslationDAO.get(cmp_translation)
        if instance:
            return instance

        instance = CmpTranslation(cmp_translation=cmp_translation)
        db.session.add(instance)
        db.session.flush()
        return instance

    @staticmethod
    def delete_all():
        CmpTranslation.query.delete(synchronize_session=False)
        db.session.commit()
