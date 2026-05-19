from app import db


class CmpTranslationMapping(db.Model):
    """
        The CmpTranslationMapping model represents the mapping between a Consent 
        Management Platform (CMP), its target translation languages, and the 
        locale/site it applies to.

        Populated with the data/cmp_domain_locale_mapping(Locales).csv file.
    """

    __tablename__ = 'cmp_translation_mapping'

    # Primary key
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    cmp_id = db.Column(db.Integer, db.ForeignKey('cmp.cmp_id'), nullable=False)
    cmp_translation_id = db.Column(db.Integer, db.ForeignKey('cmp_translation.cmp_translation_id'))
    language_iso_code_id = db.Column(db.Integer, db.ForeignKey('language_iso_code.language_iso_code_id'))

    site = db.Column(db.String(255), nullable=False)
    locale = db.Column(db.String(255))

    # Relationships
    cmp = db.relationship('Cmp', back_populates='cmp_translation_mapping')
    cmp_translation = db.relationship('CmpTranslation', back_populates='cmp_translation_mapping')
    language_iso_code = db.relationship('LanguageIsoCode', back_populates='cmp_translation_mapping')