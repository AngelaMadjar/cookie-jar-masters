from app import db


class VendorDescription(db.Model):
    """
        VendorDescription Model

        The VendorDescription model represents a detailed description of a vendor. Each 
        vendor description can have multiple translations.

        It is part of a many-to-many relationship with the Vendor model, allowing for 
        historical versioning of Vendor Descriptions for a particular vendor. A Vendor 
        is associated with a single active Vendor Description at a given time.

        A VendorDescription can be associated with multiple translations of its value.
    """

    __tablename__ = 'vendor_description'

    # Primary key
    vendor_description_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    vendor_description = db.Column(db.String(5000), nullable=False)

    # Relationships
    # *-*
    vendor_descriptions = db.relationship('VendorDescriptions', back_populates='vendor_description')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('vendor_description', name='uq_vendor_description'),
    )
