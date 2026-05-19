from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.vendor.vendor_descriptions import VendorDescriptions


def now_utc():
    return datetime.now(timezone.utc)


class VendorDescriptionsDAO:
    @staticmethod
    def set_all_not_current(vendor_ids: list[int]):
        """
        Marks all current vendor descriptions as not current for the given vendor_ids.
        """
        if not vendor_ids:
            return
        VendorDescriptions.query.filter(
            VendorDescriptions.vendor_id.in_(vendor_ids),
            VendorDescriptions.is_current.is_(True)
        ).update({"is_current": False}, synchronize_session=False)
        db.session.commit()

    @staticmethod
    def bulk_insert_vendor_description_links(links: list[dict]):
        if not links:
            return

        for l in links:
            if "created_at" not in l:
                l["created_at"] = now_utc()

        query = insert(VendorDescriptions).values(links).on_conflict_do_nothing(
            index_elements=["vendor_id", "vendor_description_id", "is_current"]  # unique constraint
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_current_vendor_description_id(vendor_id: int) -> Optional[int]:
        """
        Returns the current vendor_description_id for a vendor, or None.
        """
        return db.session.execute(
            select(VendorDescriptions.vendor_description_id).where(
                VendorDescriptions.vendor_id == vendor_id,
                VendorDescriptions.is_current.is_(True),
            )
        ).scalar_one_or_none()
    
    @staticmethod
    def set_not_current(vendor_id: int) -> int:
        """
        Marks all current vendor descriptions as NOT current for this vendor.
        Returns number of rows updated.
        """
        result = db.session.execute(
            update(VendorDescriptions)
            .where(
                VendorDescriptions.vendor_id == vendor_id,
                VendorDescriptions.is_current.is_(True),
            )
            .values(is_current=False)
        )
        return result.rowcount or 0
    
    @staticmethod
    def insert_current(vendor_id: int, vendor_description_id: int, created_at):
        """
        Inserts a new current link row for vendor->description.
        UNIQUE constraint: (vendor_id, vendor_description_id, is_current)
        """
        query = (
            insert(VendorDescriptions)
            .values([{
                "vendor_id": vendor_id,
                "vendor_description_id": vendor_description_id,
                "created_at": created_at,
                "is_current": True
            }])
            .on_conflict_do_nothing(
                index_elements=["vendor_id", "vendor_description_id", "is_current"]
            )
        )
        db.session.execute(query)

    @staticmethod
    def delete_all():
        VendorDescriptions.query.delete(synchronize_session=False)
        db.session.commit()

    