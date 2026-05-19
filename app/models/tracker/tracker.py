from sqlalchemy import Index
from app import db


class Tracker(db.Model):
    """
        The Tracker table serves as a primary entity in the Cookie Jar system, 
        storing detailed data for individual trackers. Each tracker is uniquely 
        identified by a combination of tracker_name, tracker_type_id, and 
        tracking_domain_id. 

        The table includes foreign key relationships to TrackerType, TrackerCategory, 
        TrackerSource, Vendor, and TrackingDomain, providing a normalized structure.
        Many-to-many relationships with the TrackerPurpose and Cmp models is maintained
        with the TrackerPurposes and TrackerCmp tables.
    """

    __tablename__ = 'tracker'

    # Primary key
    tracker_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    tracker_type_id = db.Column(db.Integer, db.ForeignKey('tracker_type.tracker_type_id'), nullable=False)
    tracker_category_id = db.Column(db.Integer, db.ForeignKey('tracker_category.tracker_category_id'), nullable=False)
    tracker_source_id = db.Column(db.Integer, db.ForeignKey('tracker_source.tracker_source_id'), nullable=False)
    vendor_id = db.Column(db.Integer, db.ForeignKey('vendor.vendor_id'), nullable=True)
    tracking_domain_id = db.Column(db.Integer, db.ForeignKey('tracking_domain.tracking_domain_id'), nullable=False)

    tracker_name = db.Column(db.String(255), nullable=True)
    tracker_duration = db.Column(db.String(255), nullable=True)
    last_modified = db.Column(db.DateTime, nullable=False)
    
    # Relationships
    # 1-*
    tracker_type = db.relationship('TrackerType', back_populates='trackers')
    tracker_category = db.relationship('TrackerCategory', back_populates='trackers')
    tracker_source = db.relationship('TrackerSource', back_populates='trackers')
    vendor = db.relationship('Vendor', back_populates='trackers')
    tracking_domain = db.relationship('TrackingDomain', back_populates='trackers')
    # *-*
    tracker_purposes = db.relationship('TrackerPurposes', back_populates='tracker')
    cmps = db.relationship('TrackerCmp', back_populates='tracker')

    # Constraints and Indexes
    __table_args__ = (
        db.UniqueConstraint('tracker_name', 'tracker_type_id', 'tracking_domain_id',
                            name='uq_tracker_name_type_domain'),
        Index('idx_tracker_composite', 'tracker_type_id', 'tracker_name', 'tracking_domain_id')
    )
