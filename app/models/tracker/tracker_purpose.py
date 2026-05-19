from app import db


class TrackerPurpose(db.Model):
    """
        The TrackerPurpose model represents the description of the purpose for which a 
        tracker is used, such as consent collection, analytics, marketing, etc. 
        
        It is part of a many-to-many relationship with the Tracker model, allowing for 
        historical versioning of Tracker Purposes for a particular tracker. A Tracker 
        is associated with a single active Tracker Purpose at a given time.

        A TrackerPurpose can be associated with multiple translations of its value.

    """

    __tablename__ = 'tracker_purpose'

    # Primary key
    tracker_purpose_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    tracker_purpose = db.Column(db.String(5000), nullable=False)

    # Relationships
    # *-*
    tracker_purposes = db.relationship('TrackerPurposes', back_populates='tracker_purpose')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('tracker_purpose', name='uq_tracker_purpose'),
    )
