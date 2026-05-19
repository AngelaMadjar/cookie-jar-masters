from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from app import db
from app.models.vendor.vendor_description import VendorDescription


class VendorDescriptionDAO:
    @staticmethod
    def bulk_insert_vendor_descriptions(descriptions: list[str]):
        rows = [{"vendor_description": d} for d in descriptions if d is not None]
        if not rows:
            return
        query = insert(VendorDescription).values(rows).on_conflict_do_nothing(
            index_elements=["vendor_description"]
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def get_vendor_descriptions():
        return {d.vendor_description: d.vendor_description_id for d in VendorDescription.query.all()}

    @staticmethod
    def get_or_create(vendor_description_text: str) -> int:
        vendor_description_text = vendor_description_text.strip()

        query = (
            insert(VendorDescription)
            .values([{"vendor_description": vendor_description_text}])
            .on_conflict_do_nothing(index_elements=["vendor_description"])
        )
        db.session.execute(query)

        vendor_description_id = db.session.execute(
            select(VendorDescription.vendor_description_id).where(
                VendorDescription.vendor_description == vendor_description_text
            )
        ).scalar_one()

        return vendor_description_id
    @staticmethod
    def delete_all():
        VendorDescription.query.delete(synchronize_session=False)
        db.session.commit()
