from typing import Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import update, func
from app import db
from app.models.vendor.vendor import Vendor


class VendorDAO:
    @staticmethod
    def bulk_insert_vendors(vendor_names: list[str]):
        rows = [{"vendor_name": v} for v in vendor_names if v is not None]
        if not rows:
            return
        query = insert(Vendor).values(rows).on_conflict_do_nothing(index_elements=["vendor_name"])
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_vendors():
        return {v.vendor_name: v.vendor_id for v in Vendor.query.all()}
    
    @staticmethod
    def get_by_id(vendor_id: int) -> Optional[Vendor]:
        return db.session.get(Vendor, vendor_id)

    @staticmethod
    def touch_last_modified(vendor_id: int, ts):
        db.session.execute(
            update(Vendor)
            .where(Vendor.vendor_id == vendor_id)
            .values(last_modified=ts)
        )

    @staticmethod
    def delete_all():
        Vendor.query.delete(synchronize_session=False)
        db.session.commit()

    @staticmethod
    def list_vendors(limit: int = 500) -> list[dict]:
        """
        Returns vendors with their CURRENT description (if any).
        """
        from app.models.vendor.vendor_descriptions import VendorDescriptions
        from app.models.vendor.vendor_description import VendorDescription

        q = (
            db.session.query(
                Vendor.vendor_id.label("vendor_id"),
                Vendor.vendor_name.label("vendor_name"),
                VendorDescription.vendor_description.label("vendor_description"),
            )
            .select_from(Vendor)
            .outerjoin(
                VendorDescriptions,
                (VendorDescriptions.vendor_id == Vendor.vendor_id)
                & (VendorDescriptions.is_current.is_(True))
            )
            .outerjoin(
                VendorDescription,
                VendorDescription.vendor_description_id == VendorDescriptions.vendor_description_id
            )
            .order_by(Vendor.vendor_id.asc())
            .limit(limit)
        )

        return [
            {
                "vendor_id": r.vendor_id,
                "vendor_name": r.vendor_name,
                "vendor_description": r.vendor_description,
            }
            for r in q.all()
        ]


    @staticmethod
    def count_vendors() -> int:
        return int(db.session.query(func.count(Vendor.vendor_id)).scalar() or 0)

    @staticmethod
    def get_sample_vendor_id():
        return (
            db.session.query(Vendor.vendor_id)
            .order_by(Vendor.vendor_id)
            .limit(1)
            .scalar()
        )
