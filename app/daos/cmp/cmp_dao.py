from app import db
from app.models.cmp.cmp import Cmp


class CmpDAO:
    @staticmethod
    def bulk_insert_cmps(cmps: list[str]):
        values = [c for c in cmps if c]
        if not values:
            return

        existing = {c.cmp for c in Cmp.query.filter(Cmp.cmp.in_(values)).all()}
        missing = [Cmp(cmp=c) for c in values if c not in existing]
        if missing:
            db.session.add_all(missing)
            db.session.commit()

    @staticmethod
    def get_cmps() -> dict[str, int]:
        return {c.cmp: c.cmp_id for c in Cmp.query.all()}

    @staticmethod
    def delete_all():
        Cmp.query.delete(synchronize_session=False)
        db.session.commit()
