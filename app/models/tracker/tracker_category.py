from app import db


# TODO: The data in this model needs to be standardized (e.g. "Essential Cookies", "Essential cookies")

class TrackerCategory(db.Model):
    """
        The TrackerCategory model represents a category that a tracker can belong to, 
        such as "Required Cookies", "Essential Cookies", etc.
        
        This category is received from TrustArc's scan files. However, if a tracker 
        with the same combination of name, type, and domain already exists in the 
        database and is associated with a different category, the existing category 
        in the database will take precedence and overwrite the category from the scan.
    """

    __tablename__ = 'tracker_category'

    # Primary key
    tracker_category_id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    tracker_category = db.Column(db.String(255), nullable=False)

    # Relationships
    trackers = db.relationship('Tracker', back_populates='tracker_category')

    # Constraints
    __table_args__ = (
        db.UniqueConstraint('tracker_category', name='uq_tracker_category'),
    )
