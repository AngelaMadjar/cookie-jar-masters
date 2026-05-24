from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from app import db

from app.daos.vendor.vendor_dao import VendorDAO
from app.daos.vendor.vendor_description_dao import VendorDescriptionDAO
from app.daos.vendor.vendor_descriptions_dao import VendorDescriptionsDAO

from app.daos.tracker.tracker_type_dao import TrackerTypeDAO
from app.daos.tracker.tracker_category_dao import TrackerCategoryDAO
from app.daos.tracker.tracker_source_dao import TrackerSourceDAO
from app.daos.tracker.tracking_domain_dao import TrackingDomainDAO
from app.daos.tracker.tracker_purpose_dao import TrackerPurposeDAO
from app.daos.tracker.tracker_dao import TrackerDAO
from app.daos.tracker.tracker_purposes_dao import TrackerPurposesDAO
from app.daos.cmp.cmp_dao import CmpDAO
from app.daos.cmp.cmp_translation_dao import CmpTranslationDAO
from app.daos.cmp.language_iso_code_dao import LanguageIsoCodeDAO
from app.daos.cmp.cmp_translation_mapping_dao import CmpTranslationMappingDAO


def now_utc():
    """
    Returns the current UTC timestamp.
    """
    return datetime.now(timezone.utc)


class SeedService:
    """
        Database seeding service for tracker and CMP reference data.

        This service loads seed CSV files (data/seed), normalizes input columns, 
        and populates lookup tables, core tracker records, and association tables 
        in dependency order so foreign-key references can be resolved correctly.
    """

    @staticmethod
    def _read_csv_flexible(csv_path: str) -> pd.DataFrame:
        """
        Reads a CSV file using a fallback encoding strategy.

        Tries utf-8-sig first, then latin-1. Raises the last read exception
        if all attempts fail.
        """
        last_error = None
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(
                    csv_path,
                    sep=",",
                    dtype=str,
                    encoding=encoding,
                    keep_default_na=False,
                )
            except Exception as exc:
                last_error = exc
        raise last_error

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes input column names to the internal schema field names.

        Any required ingestion columns missing from the source are created with
        None values so downstream logic can run consistently.
        """
        rename_map = {
            "Tracking Domain": "tracking_domain",
            "Consent Category": "consent_category",
            "Old Consent Category": "old_consent_category",
            "New Consent Category": "consent_category",
            "Vendor Name": "vendor_name",
            "Tracker Type": "tracker_type",
            "Tracker Name": "tracker_name",
            "Tracker Purpose": "tracker_purpose",
            "Tracker Purpose (Source)": "tracker_purpose_source",
            "Vendor Description": "vendor_description",
            "Tracker Duration": "tracker_duration",
        }
        df = df.rename(columns=rename_map)
        for col in (
            "tracking_domain",
            "consent_category",
            "vendor_name",
            "tracker_type",
            "tracker_name",
            "tracker_purpose",
            "vendor_description",
            "tracker_duration",
        ):
            if col not in df.columns:
                df[col] = None
        return df

    @staticmethod
    def _prepare_data(csv_path: str) -> pd.DataFrame:
        """
        Loads and cleans a seed CSV file for seeding operations.

        Steps:
        - read with flexible encoding
        - normalize column names
        - trim whitespace from string columns
        - convert empty strings and "-" placeholders to None
        """
        data = SeedService._read_csv_flexible(csv_path)
        data = SeedService._normalize_columns(data)
        string_cols = data.select_dtypes(include=["object"]).columns
        if len(string_cols) > 0:
            data[string_cols] = data[string_cols].apply(lambda col: col.str.strip())
        data = data.replace({"": None, "-": None})
        return data

    @staticmethod
    def populate_from_csv(csv_path: str, source_name: str = "seed_anonymized.csv") -> dict:
        """
        Seeds tracker-related tables from a single CSV file.

        Returns a summary dictionary with insertion attempt counters.
        """
        data = SeedService._prepare_data(csv_path)

        return SeedService._populate_dataframe(data, source_name)

    @staticmethod
    def populate_from_csv_files(
        csv_paths: list[str],
        cmp_mappings_paths: list[str] = None,
    ) -> dict:
        """
        Seeds tracker-related tables from multiple CSV files merged together.

        Each file is prepared with the same normalization/cleanup logic, then
        concatenated and passed to the shared dataframe seeding flow.

        Note: cmp_mappings_paths is currently unused in this method.
        """
        if not csv_paths:
            raise ValueError("csv_paths cannot be empty")

        prepared_frames = []
        source_labels = []
        for csv_path in csv_paths:
            prepared_frames.append(SeedService._prepare_data(csv_path))
            source_labels.append(Path(csv_path).name)

        merged = pd.concat(prepared_frames, ignore_index=True)
        result = SeedService._populate_dataframe(merged, ", ".join(source_labels))
        result["cmp_translation_mappings_loaded"] = 0
        return result

    @staticmethod
    def populate_cmp_mappings_from_files(cmp_mappings_paths: list[str]) -> dict:
        """
        Seeds CMP translation mapping data from one or more mapping CSV files.

        Returns a summary with total inserted mapping rows.
        """
        if not cmp_mappings_paths:
            raise ValueError("cmp_mappings_paths cannot be empty")

        cmp_rows = 0
        for cmp_path in cmp_mappings_paths:
            cmp_rows += SeedService.load_cmp_translation_mappings(cmp_path)

        return {
            "status": "ok",
            "cmp_mappings_files": cmp_mappings_paths,
            "cmp_translation_mappings_loaded": int(cmp_rows),
        }

    @staticmethod
    def load_cmp_translation_mappings(csv_path: str) -> int:
        """
        Loads CMP translation mapping rows from a single CSV file.

        For each valid row, the method ensures dependent lookup values exist
        (CMP, CMP translation, language ISO code), then inserts a mapping row
        when not already present. Returns the number of newly inserted mappings.
        """
        path = Path(csv_path)
        if not path.exists():
            return 0

        data = SeedService._read_csv_flexible(str(path))
        required = ["Site", "Locale", "CMP", "Cmp_Translation", "Language_Iso_Code", "Language_Name"]
        for col in required:
            if col not in data.columns:
                raise ValueError(f"Missing required CMP mapping column: {col}")

        inserted_rows = 0
        for _, row in data.iterrows():
            site = str(row.get("Site", "")).strip() or None
            cmp_value = str(row.get("CMP", "")).strip() or None
            cmp_translation_value = str(row.get("Cmp_Translation", "")).strip() or None
            lang_iso = str(row.get("Language_Iso_Code", "")).strip() or None
            lang_name = str(row.get("Language_Name", "")).strip() or None
            locale = str(row.get("Locale", "")).strip() or None

            if not site or not cmp_value:
                continue

            CmpDAO.bulk_insert_cmps([cmp_value])
            cmp_map = CmpDAO.get_cmps()
            cmp_id = cmp_map.get(cmp_value)
            if not cmp_id:
                continue

            cmp_translation = (
                CmpTranslationDAO.get_or_create(cmp_translation_value)
                if cmp_translation_value
                else None
            )
            language_iso_code = (
                LanguageIsoCodeDAO.get_or_create(lang_iso, lang_name)
                if lang_iso
                else None
            )

            created = CmpTranslationMappingDAO.create(
                site=site,
                locale=locale,
                cmp_id=cmp_id,
                cmp_translation_id=(
                    cmp_translation.cmp_translation_id if cmp_translation else None
                ),
                language_iso_code_id=(
                    language_iso_code.language_iso_code_id if language_iso_code else None
                ),
            )
            if created is not None:
                inserted_rows += 1

        db.session.commit()
        return inserted_rows

    @staticmethod
    def _populate_dataframe(data: pd.DataFrame, source_name: str) -> dict:
        """
        Core tracker-data seeding routine from a prepared dataframe.

        Processing order:
        - insert lookup/reference entities
        - build value->id maps
        - insert tracking domains
        - insert trackers
        - insert tracker-purpose associations
        - insert vendor-description associations

        Returns a summary dictionary with attempted row/link counts.
        """

        # Distinct lists 
        # DB tableshave UNIQUE constraints, so I first select distinct values for these entities
        distinct_vendors = data["vendor_name"].dropna().unique().tolist()
        distinct_tracker_types = data["tracker_type"].dropna().unique().tolist()
        distinct_tracker_categories = data["consent_category"].dropna().unique().tolist()
        distinct_tracker_purposes = data["tracker_purpose"].dropna().unique().tolist()
        distinct_vendor_descriptions = data["vendor_description"].dropna().unique().tolist()

        # Insert lookups first
        # Tracker entity depends on vendor/type/category/source/domain, so those must exist in the 
        # DB before creating Tracker rows
        VendorDAO.bulk_insert_vendors(distinct_vendors)
        TrackerTypeDAO.bulk_insert_tracker_types(distinct_tracker_types)
        TrackerCategoryDAO.bulk_insert_tracker_categories(distinct_tracker_categories)
        TrackerPurposeDAO.bulk_insert_tracker_purposes(distinct_tracker_purposes)
        VendorDescriptionDAO.bulk_insert_vendor_descriptions(distinct_vendor_descriptions)
        TrackerSourceDAO.bulk_insert_tracker_sources([source_name])

        # Build maps (value -> id)
        # These maps convert the string values from the CSV into foreign-key IDs.
        vendor_map = VendorDAO.get_vendors()
        tracker_type_map = TrackerTypeDAO.get_tracker_types()
        tracker_category_map = TrackerCategoryDAO.get_tracker_categories()
        tracker_purpose_map = TrackerPurposeDAO.get_tracker_purposes()
        vendor_description_map = VendorDescriptionDAO.get_vendor_descriptions()
        tracker_source_map = TrackerSourceDAO.get_tracker_sources()
        tracker_source_id = tracker_source_map[source_name]

        # Insert TrackingDomains 
        # tracking_domain has a unique constraint with vendor_id: (vendor_id, tracking_domain)
        distinct_domains = data[["tracking_domain", "vendor_name"]].drop_duplicates()


        domain_rows = []
        for _, r in distinct_domains.iterrows():
            domain = r["tracking_domain"]
            vendor_name = r["vendor_name"]

            if domain is None:
                continue

            vendor_id = vendor_map.get(vendor_name) if vendor_name is not None else None
            domain_rows.append({
                "tracking_domain": domain,
                "vendor_id": vendor_id
            })

        TrackingDomainDAO.bulk_insert_tracking_domains(domain_rows)
        tracking_domain_map = TrackingDomainDAO.get_tracking_domains()  # (domain, vendor_id) -> id


        # Insert Trackers
        # In order to create tracker_purposes associations, trackers must be created first to retrieve their IDs
        trackers_to_create = []
        for _, r in data.iterrows():
            vendor_name = r["vendor_name"]
            vendor_id = vendor_map.get(vendor_name) if vendor_name is not None else None

            domain = r["tracking_domain"]
            tracking_domain_id = tracking_domain_map.get((domain, vendor_id)) if domain is not None else None

            tracker_type_id = tracker_type_map.get(r["tracker_type"])
            tracker_category_id = tracker_category_map.get(r["consent_category"])

            # Required FKs
            if tracking_domain_id is None or tracker_type_id is None or tracker_category_id is None:
                continue

            trackers_to_create.append({
                "tracker_name": r["tracker_name"],
                "tracker_duration": r.get("tracker_duration"),
                "last_modified": now_utc(),
                "tracker_type_id": tracker_type_id,
                "tracker_category_id": tracker_category_id,
                "tracker_source_id": tracker_source_id,
                "vendor_id": vendor_id,
                "tracking_domain_id": tracking_domain_id,
            })

        TrackerDAO.bulk_insert_trackers(trackers_to_create)


        # Build tracker_id map 
        tracker_id_map = TrackerDAO.get_trackers()  # (name,type_id,domain_id) -> tracker_id

        # Insert TrackerPurposes links
        # Creating *-* relationships requires tracker ids
        tracker_purpose_links = []
        for _, r in data.iterrows():
            vendor_name = r["vendor_name"]
            vendor_id = vendor_map.get(vendor_name) if vendor_name is not None else None

            domain = r["tracking_domain"]
            tracking_domain_id = tracking_domain_map.get((domain, vendor_id)) if domain is not None else None
            tracker_type_id = tracker_type_map.get(r["tracker_type"])

            if tracking_domain_id is None or tracker_type_id is None:
                continue

            tracker_key = (r["tracker_name"], tracker_type_id, tracking_domain_id)
            tracker_id = tracker_id_map.get(tracker_key)

            purpose = r.get("tracker_purpose")
            tracker_purpose_id = tracker_purpose_map.get(purpose) if purpose is not None else None

            if tracker_id is None or tracker_purpose_id is None:
                continue

            tracker_purpose_links.append({
                "tracker_id": tracker_id,
                "tracker_purpose_id": tracker_purpose_id,
                "created_at": now_utc(),
                "is_current": True
            })

        TrackerPurposesDAO.bulk_insert_tracker_purpose_links(tracker_purpose_links)


        # Insert VendorDescriptions links
        vendor_desc_links = []
        for _, r in data.iterrows():
            vendor_name = r["vendor_name"]
            desc = r.get("vendor_description")

            if vendor_name is None or desc is None:
                continue

            vendor_id = vendor_map.get(vendor_name)
            vendor_description_id = vendor_description_map.get(desc)

            if vendor_id is None or vendor_description_id is None:
                continue

            vendor_desc_links.append({
                "vendor_id": vendor_id,
                "vendor_description_id": vendor_description_id,
                "created_at": now_utc(),
                "is_current": True
            })

        VendorDescriptionsDAO.bulk_insert_vendor_description_links(vendor_desc_links)

        return {
            "status": "ok",
            "rows_in_csv": int(len(data)),
            "source_name": source_name,
            "trackers_attempted": int(len(trackers_to_create)),
            "vendor_desc_links_attempted": int(len(vendor_desc_links)),
            "tracker_purpose_links_attempted": int(len(tracker_purpose_links)),
        }
