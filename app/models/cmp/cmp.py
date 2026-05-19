from app import db


class Cmp(db.Model):
    """
        The Cmp model represents a Consent Management Platform (CMP) used to manage 
        user consent and privacy preferences. TrustArc's scans are received on a
        cmp level (each file containing trackers corresponds to a Cmp where those
        trackers are setup and active).
        
        Translations for tracker purposes and vendor descriptions need to be provided
        in different languages for each Cmp.

        This model tracks the relationship to CMP translation languages and trackers
        that are associated with the CMP.
    """

    __tablename__ = 'cmp'

    # Primary key
    cmp_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    cmp = db.Column(db.String(255), nullable=False)

    # Relationships
    # 1-*
    cmp_translation_mapping = db.relationship('CmpTranslationMapping', back_populates='cmp')
    # *-*
    trackers = db.relationship('TrackerCmp', back_populates='cmp')
