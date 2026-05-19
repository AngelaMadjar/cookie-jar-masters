from sqlalchemy import Index
from app import db


class VendorDescriptions(db.Model):
    """
        The VendorDescriptions model serves as an association table connecting Vendor 
        and VendorDescription in a many-to-many relationship. This model implements a 
        Slowly Changing Dimension Type 2 to keep track of the historical changes 
        in vendor descriptions over time.

        Each vendor can have multiple descriptions (only one active at a given time), 
        and the model stores changes in these descriptions by marking which 
        vendor-description associations are currently active through the is_current field. 
    """

    __tablename__ = 'vendor_descriptions'
    
    # Foeign keys
    vendor_id = db.Column(db.Integer, db.ForeignKey('vendor.vendor_id'), primary_key=True)
    vendor_description_id = db.Column(db.Integer, db.ForeignKey('vendor_description.vendor_description_id'), primary_key=True)

    created_at = db.Column(db.DateTime, nullable=True)  # TODO: Set to False

    is_current = db.Column(db.Boolean, nullable=False)

    # Relationships
    vendor = db.relationship('Vendor', back_populates='vendor_descriptions')
    vendor_description = db.relationship('VendorDescription', back_populates='vendor_descriptions')

    # Constraints and Indexes
    __table_args__ = (
        db.UniqueConstraint('vendor_id', 'vendor_description_id', 'is_current', name='uq_vendor_id_vendor_description_id_is_current'),
        Index('idx_vendor_descriptions_composite', 'vendor_id', 'vendor_description_id', 'is_current') 
    )
