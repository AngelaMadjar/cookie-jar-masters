import time

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError

from app import db
from app.models.tracker.tracking_domain import TrackingDomain


class TrackingDomainDAO:
    DEADLOCK_SQLSTATE = "40P01"
    MAX_DEADLOCK_RETRIES = 3
    DEADLOCK_BACKOFF_SEC = 0.05

    @staticmethod
    def _is_deadlock_error(exc: Exception) -> bool:
        orig = getattr(exc, "orig", None)
        pgcode = getattr(orig, "pgcode", None)
        if pgcode == TrackingDomainDAO.DEADLOCK_SQLSTATE:
            return True
        text = str(exc).lower()
        return "deadlock detected" in text

    @staticmethod
    def bulk_insert_tracking_domains(domains: list[dict]):
        """
        domains example:
        [
          {"tracking_domain": "example.com", "vendor_id": 1}
        ]
        """
        # keep only rows that have a domain value
        rows = [d for d in domains if d.get("tracking_domain") is not None]
        if not rows:
            return

        query = insert(TrackingDomain).values(rows).on_conflict_do_nothing(
            index_elements=["vendor_id", "tracking_domain"]  # unique constraint
        )
        for attempt in range(TrackingDomainDAO.MAX_DEADLOCK_RETRIES + 1):
            try:
                db.session.execute(query)
                db.session.commit()
                return
            except DBAPIError as exc:
                db.session.rollback()
                if not TrackingDomainDAO._is_deadlock_error(exc):
                    raise
                if attempt >= TrackingDomainDAO.MAX_DEADLOCK_RETRIES:
                    raise
                sleep_sec = TrackingDomainDAO.DEADLOCK_BACKOFF_SEC * (2**attempt)
                time.sleep(sleep_sec)

    @staticmethod
    def get_tracking_domains():
        """
        Returns mapping keyed by (tracking_domain, vendor_id) -> tracking_domain_id
        """
        return {
            (d.tracking_domain, d.vendor_id): d.tracking_domain_id
            for d in TrackingDomain.query.all()
        }

    @staticmethod
    def delete_all():
        TrackingDomain.query.delete(synchronize_session=False)
        db.session.commit()
