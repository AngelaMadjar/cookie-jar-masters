from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.daos.cmp.cmp_dao import CmpDAO
from app.daos.tracker.tracker_category_dao import TrackerCategoryDAO
from app.daos.tracker.tracker_purpose_dao import TrackerPurposeDAO
from app.daos.tracker.tracker_source_dao import TrackerSourceDAO
from app.daos.tracker.tracker_type_dao import TrackerTypeDAO
from app.daos.tracker.tracking_domain_dao import TrackingDomainDAO
from app.daos.vendor.vendor_dao import VendorDAO
from app.daos.vendor.vendor_description_dao import VendorDescriptionDAO

# Tracker ingestion service flow:
# IngestionService (ValidationService) -> ReferenceDataService -> TrackerUpsertService -> PurposeService -> ReportingService -> ScanLifecycleService

@dataclass
class ReferenceMaps:
    """
    Collection of lookup maps used during ingestion upsert/linking.

    Each map resolves normalized text (or composite domain key) to database IDs.
    """
    tracker_types: dict[str, int]
    tracker_sources: dict[str, int]
    tracker_categories: dict[str, int]
    vendors: dict[str, int]
    cmps: dict[str, int]
    tracking_domains: dict[tuple[str, int | None], int]
    tracker_purposes: dict[str, int]
    vendor_descriptions: dict[str, int]


class ReferenceDataService:
    """
    Prepares foreign-key lookup dictionaries used by tracker upsert.

    The CSV contains text values (for example tracker type/category/vendor and
    tracking domain), but tracker inserts require numeric FK IDs.

    This service performs two steps before upsert:
    1. Ensure lookup rows exist in DB (insert missing values once).
    2. Read lookup tables from DB and build in-memory dictionaries, such as:
       - "JavaScript" -> tracker_type_id
       - "Vendor A" -> vendor_id
       - ("example.com", vendor_id) -> tracking_domain_id

    Downstream services then resolve each CSV row through these dictionaries
    instead of querying the database row-by-row.
    """

    @staticmethod
    def _distinct_values(df: pd.DataFrame, column: str) -> list[str]:
        """
        Returns non-empty distinct string values for a dataframe column.
        """
        if column not in df.columns:
            return []
        return [v for v in df[column].dropna().astype(str).unique().tolist() if v]

    @staticmethod
    def ensure_lookups(df: pd.DataFrame, source_name: str, cmp_name: str):
        """
        Inserts/ensures lookup-table values required by ingestion rows.

        Populates tracker type/source/category, vendor, cmp, tracker purpose,
        and vendor description dictionaries using conflict-safe DAO inserts.
        """
        TrackerTypeDAO.bulk_insert_tracker_types(ReferenceDataService._distinct_values(df, "tracker_type"))
        TrackerSourceDAO.bulk_insert_tracker_sources([source_name])
        TrackerCategoryDAO.bulk_insert_tracker_categories(ReferenceDataService._distinct_values(df, "consent_category"))
        VendorDAO.bulk_insert_vendors(ReferenceDataService._distinct_values(df, "vendor_name"))
        CmpDAO.bulk_insert_cmps([cmp_name])
        TrackerPurposeDAO.bulk_insert_tracker_purposes(
            ReferenceDataService._distinct_values(df, "tracker_purpose")
        )
        VendorDescriptionDAO.bulk_insert_vendor_descriptions(
            ReferenceDataService._distinct_values(df, "vendor_description")
        )

    @staticmethod
    def ensure_tracking_domains(df: pd.DataFrame, vendor_map: dict[str, int]):
        """
        Inserts/ensures tracking-domain rows from distinct (domain, vendor) pairs.
        """
        rows = []
        distinct = df[["tracking_domain", "vendor_name"]].drop_duplicates()

        for _, rec in distinct.iterrows():
            domain = rec.get("tracking_domain")
            vendor_name = rec.get("vendor_name")
            if not domain:
                continue
            vendor_id = vendor_map.get(vendor_name) if vendor_name else None
            rows.append({"tracking_domain": domain, "vendor_id": vendor_id})

        TrackingDomainDAO.bulk_insert_tracking_domains(rows)

    @staticmethod
    def build_reference_maps(df: pd.DataFrame, source_name: str, cmp_name: str) -> ReferenceMaps:
        """
        Runs reference bootstrap and returns all lookup maps needed downstream.
        """
        ReferenceDataService.ensure_lookups(df, source_name, cmp_name)

        vendor_map = VendorDAO.get_vendors()
        ReferenceDataService.ensure_tracking_domains(df, vendor_map)

        return ReferenceMaps(
            tracker_types=TrackerTypeDAO.get_tracker_types(),
            tracker_sources=TrackerSourceDAO.get_tracker_sources(),
            tracker_categories=TrackerCategoryDAO.get_tracker_categories(),
            vendors=vendor_map,
            cmps=CmpDAO.get_cmps(),
            tracking_domains=TrackingDomainDAO.get_tracking_domains(),
            tracker_purposes=TrackerPurposeDAO.get_tracker_purposes(),
            vendor_descriptions=VendorDescriptionDAO.get_vendor_descriptions(),
        )
