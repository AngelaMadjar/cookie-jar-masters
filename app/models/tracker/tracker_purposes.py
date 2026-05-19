from sqlalchemy import Index
from app import db


class TrackerPurposes(db.Model):
    """
        The TrackerPurposes model serves as an association table connecting Tracker 
        and TrackerPurpose in a many-to-many relationship. This model implements a 
        Slowly Changing Dimension Type 2 to keep track of the historical changes 
        in tracker purposes over time.

        Each tracker can have multiple purposes (only one active at a given time), 
        and the model stores changes in these purposes by marking which tracker-purpose 
        associations are currently active through the is_current field. 
    """

    __tablename__ = 'tracker_purposes'

    # Foreign keys
    tracker_id = db.Column(db.Integer, db.ForeignKey('tracker.tracker_id'), primary_key=True)
    tracker_purpose_id = db.Column(db.Integer, db.ForeignKey('tracker_purpose.tracker_purpose_id'), primary_key=True)

    created_at = db.Column(db.DateTime, nullable=True)  # TODO: Set to False

    is_current = db.Column(db.Boolean, nullable=False)

    # Relationships
    tracker = db.relationship('Tracker', back_populates='tracker_purposes')
    tracker_purpose = db.relationship('TrackerPurpose', back_populates='tracker_purposes')

    # Constraints and Indexes
    __table_args__ = (
        db.UniqueConstraint('tracker_id', 'tracker_purpose_id', 'is_current',
                            name='uq_tracker_id_tracker_purpose_id_is_current'),
        Index('idx_tracker_purposes_composite', 'tracker_id', 'tracker_purpose_id', 'is_current')
    )
