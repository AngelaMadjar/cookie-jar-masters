from app import db


class Vendor(db.Model):
    """
        The Vendor model represents a Vendor in the system. Each vendor has a unique ID, 
        name, and is associated with multiple tracking domains. Many-to-many relationship
        with the VendorDescription model is maintained with the VendorDescriptions table.
    """

    __tablename__ = 'vendor'

    # Primary key
    vendor_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    vendor_name = db.Column(db.String(255), nullable=False)
    # Nullable set to true for the moment because db contains data atm
    last_modified = db.Column(db.DateTime,nullable=True)

    # Relationships
    # 1-*
    tracking_domains = db.relationship('TrackingDomain', back_populates='vendor', lazy=True)
    trackers = db.relationship('Tracker', back_populates='vendor')
    # *-*
    vendor_descriptions = db.relationship('VendorDescriptions', back_populates='vendor')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('vendor_name', name='uq_vendor_name'),
    )
