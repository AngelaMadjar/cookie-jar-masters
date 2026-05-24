from app.daos.tracker.tracker_purposes_dao import TrackerPurposesDAO
from app.daos.tracker.tracker_cmp_dao import TrackerCmpDAO
from app.daos.vendor.vendor_descriptions_dao import VendorDescriptionsDAO

from app.daos.tracker.tracker_dao import TrackerDAO
from app.daos.tracker.tracking_domain_dao import TrackingDomainDAO

from app.daos.tracker.tracker_purpose_dao import TrackerPurposeDAO
from app.daos.tracker.tracker_type_dao import TrackerTypeDAO
from app.daos.tracker.tracker_category_dao import TrackerCategoryDAO
from app.daos.tracker.tracker_source_dao import TrackerSourceDAO

from app.daos.vendor.vendor_description_dao import VendorDescriptionDAO
from app.daos.vendor.vendor_dao import VendorDAO
from app.daos.cmp.cmp_translation_mapping_dao import CmpTranslationMappingDAO
from app.daos.cmp.cmp_translation_dao import CmpTranslationDAO
from app.daos.cmp.language_iso_code_dao import LanguageIsoCodeDAO
from app.daos.cmp.cmp_dao import CmpDAO


class CleanupService:
    """
        Deletes rows from all tables in a safe order (children first, then parents)
        to avoid Foreign Key constraint errors.
    """

    @staticmethod
    def empty_all():
        # Association tables first (*-* tables)
        TrackerPurposesDAO.delete_all()
        TrackerCmpDAO.delete_all()
        VendorDescriptionsDAO.delete_all()
        CmpTranslationMappingDAO.delete_all()

        # Main tables that depend on lookups
        TrackerDAO.delete_all()
        TrackingDomainDAO.delete_all()

        # Lookup tables for tracker side
        TrackerPurposeDAO.delete_all()
        TrackerTypeDAO.delete_all()
        TrackerCategoryDAO.delete_all()
        TrackerSourceDAO.delete_all()

        # Lookup tables for vendor side
        VendorDescriptionDAO.delete_all()
        VendorDAO.delete_all()

        # CMP lookup tables
        CmpDAO.delete_all()
        CmpTranslationDAO.delete_all()
        LanguageIsoCodeDAO.delete_all()

    @staticmethod
    def empty_trackers_only():
        TrackerPurposesDAO.delete_all()
        TrackerCmpDAO.delete_all()
        TrackerDAO.delete_all()

    @staticmethod
    def empty_vendors_only():
        VendorDescriptionsDAO.delete_all()
        VendorDescriptionDAO.delete_all()
        VendorDAO.delete_all()
