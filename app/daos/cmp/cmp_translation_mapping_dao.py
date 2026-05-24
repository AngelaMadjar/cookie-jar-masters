from typing import Optional
from sqlalchemy import select

from app import db
from app.models.cmp.cmp_translation_mapping import CmpTranslationMapping


class CmpTranslationMappingDAO:
    """
        Data Access Object for CMP translation mapping records.

        This DAO manages rows that map a site/locale to a CMP and optional
        translation/language targets used by downstream localization workflows.
    """

    @staticmethod
    def exists(
        site: str,
        locale: Optional[str],
        cmp_id: int,
        cmp_translation_id: Optional[int],
        language_iso_code_id: Optional[int],
    ) -> bool:
        """
        Returns True if an identical mapping row already exists.

        Identity check fields:
            site, locale, cmp_id, cmp_translation_id, language_iso_code_id
        """
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
        """
        Creates a new mapping row if no identical row exists.

        Returns:
            - the new CmpTranslationMapping entity when inserted
            - None when a duplicate mapping already exists

        Note: this method adds to the current session but does not commit.
        """
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
        """
        Deletes all CMP translation mapping rows and commits the transaction.
        """
        CmpTranslationMapping.query.delete(synchronize_session=False)
        db.session.commit()
