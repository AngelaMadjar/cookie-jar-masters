from app import db


# TODO: The data in this model needs to be standardized (e.g. "FR Original File", "FR Original")

class TrackerSource(db.Model):
    """
        The TrackerSource model represents the origin of a tracker, which can be sources 
        such as "FR Original File", "TrustArc", "Github", "Google Research", or the actual
        filename of TrustArc's scans. The value "FR Original File" specifically refers to 
        a French file that Philips was working on in 2023. 

        This model helps track the initial source of the tracker, providing context 
        on where it originated from for better traceability.
    """

    __tablename__ = 'tracker_source'

    # Primary key
    tracker_source_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    tracker_source = db.Column(db.String(255), nullable=True)

    # Relationships
    trackers = db.relationship('Tracker', back_populates='tracker_source')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('tracker_source', name='uq_tracker_source'),
    )
