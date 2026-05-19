from app import db


class LanguageIsoCode(db.Model):
    """
        The LanguageIsoCode model specifies the target language and its Iso code 
        to which tracker purposes and vendor descriptions should be translated.

        Iso-codes are used by the translation model (Google Translation API).
    """

    __tablename__ = 'language_iso_code'

    # Primary key
    language_iso_code_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    language_iso_code = db.Column(db.String(20), nullable=False)
    language_name = db.Column(db.String(50), nullable=False)

    # Relationships
    cmp_translation_mapping = db.relationship('CmpTranslationMapping', back_populates='language_iso_code')