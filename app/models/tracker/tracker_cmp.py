from sqlalchemy import Index
from app import db


class TrackerCmp(db.Model):
    """
        The TrackerCmp model serves as an association table connecting Tracker 
        and Cmp models in a many-to-many relationship. 
    
        Each tracker can be active in multiple Cmps. The is_active field specifies
        Whether a tracker is currently active in a particular Cmp.
    """

    __tablename__ = 'tracker_cmp'

    # Foreign keys
    tracker_id = db.Column(db.Integer, db.ForeignKey('tracker.tracker_id'), nullable=False, primary_key=True)
    cmp_id = db.Column(db.Integer, db.ForeignKey('cmp.cmp_id'), nullable=False, primary_key=True)

    is_active = db.Column(db.Boolean, nullable=False, default=True)

    # Relationships
    tracker = db.relationship('Tracker', back_populates='cmps')
    cmp = db.relationship('Cmp', back_populates='trackers')

    # Constraints and Indexes
    # TODO: is_active should be removed from the constraint - if a tracker isn't active anymore, the is_active field should be modified. A record with the same tracker_id and cmp_id should not exist.
    __table_args__ = (
        db.UniqueConstraint('tracker_id', 'cmp_id', name='uq_tracker_id_cmp_id_is_active'),
        Index('idx_tracker_cmp_composite', 'tracker_id', 'cmp_id', 'is_active')
    )
