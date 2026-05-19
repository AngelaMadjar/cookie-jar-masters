from app import db


class CmpTranslation(db.Model):
    """
        CmpTranslation Model

        This model represents a translation of a Consent Management Platform (CMP). 
        Each CMP can have multiple translations to support different languages or locales. 

        TODO: Scince the LanguageIsoCode is used to specify target translation languages,
        this is a reduntant table that can be excluded from the schema.
    """

    __tablename__ = 'cmp_translation'

    # Primary key
    cmp_translation_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    cmp_translation = db.Column(db.String(255), nullable=False)

    # Relationships
    cmp_translation_mapping = db.relationship('CmpTranslationMapping', back_populates='cmp_translation')
