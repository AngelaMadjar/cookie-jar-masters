from app import db


# TODO: The data in this model needs to be standardized (e.g. "Javascript", "JavaScript")

class TrackerType(db.Model):
    """
        The TrackerType model represents a type of tracker, such as "HTTP Cookie", 
        "Local Storage", "Beacon", "JavaScript", "ETag" or "Session Storage". 
        This is a field received in TrustArc's scan files.
    """

    __tablename__ = 'tracker_type'

    # Primary key
    tracker_type_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    tracker_type = db.Column(db.String(255), nullable=False)

    # Relationships
    trackers = db.relationship('Tracker', back_populates='tracker_type')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('tracker_type', name='uq_tracker_type'),
    )
