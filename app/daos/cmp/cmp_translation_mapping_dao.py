from typing import Optional
from sqlalchemy import select

from app import db
from app.models.cmp.cmp_translation_mapping import CmpTranslationMapping


class CmpTranslationMappingDAO:
    @staticmethod
    def exists(
        site: str,
        locale: Optional[str],
        cmp_id: int,
        cmp_translation_id: Optional[int],
        language_iso_code_id: Optional[int],
    ) -> bool:
        stmt = select(CmpTranslationMapping.id).where(
            CmpTranslationMapping.site == site,
            CmpTranslationMapping.locale == locale,
            CmpTranslationMapping.cmp_id == cmp_id,
            CmpTranslationMapping.cmp_translation_id == cmp_translation_id,
            CmpTranslationMapping.language_iso_code_id == language_iso_code_id,
        )
        return db.session.execute(stmt).scalar_one_or_none() is not None

    @staticmethod
    def create(
        site: str,
        locale: Optional[str],
        cmp_id: int,
        cmp_translation_id: Optional[int],
        language_iso_code_id: Optional[int],
    ):
        if CmpTranslationMappingDAO.exists(site, locale, cmp_id, cmp_translation_id, language_iso_code_id):
            return None

        instance = CmpTranslationMapping(
            site=site,
            locale=locale,
            cmp_id=cmp_id,
            cmp_translation_id=cmp_translation_id,
            language_iso_code_id=language_iso_code_id,
        )
        db.session.add(instance)
        return instance

    @staticmethod
    def delete_all():
        CmpTranslationMapping.query.delete(synchronize_session=False)
        db.session.commit()
