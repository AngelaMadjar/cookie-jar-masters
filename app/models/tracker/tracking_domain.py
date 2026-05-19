from app import db


class TrackingDomain(db.Model):
    """
        The TrackingDomain model represents a tracking domain, which is uniquely 
        associated with a specific vendor. Each tracking domain belongs to a 
        single vendor (`vendor_id`)
    """

    __tablename__ = 'tracking_domain'

    # Primary key
    tracking_domain_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    vendor_id = db.Column(db.Integer, db.ForeignKey('vendor.vendor_id'), nullable=True)

    tracking_domain = db.Column(db.String(255), nullable=False)

    # Relationships
    vendor = db.relationship('Vendor', back_populates='tracking_domains')
    trackers = db.relationship('Tracker', back_populates='tracking_domain')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('vendor_id', 'tracking_domain', name='uq_vendor_id_tracking_domain'),
    )
