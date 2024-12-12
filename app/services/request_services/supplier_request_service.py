from datetime import datetime
from typing import Dict

from app.models.fhir.r4.types import Entry, Bundle
# from fhir.resources.R4B.bundle import Bundle, BundleEntry

from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.fhir_request_service import FhirRequestService


class SupplierRequestsService:
    def __init__(self, supplier_service: SupplierService) -> None:
        self.__supplier_service = supplier_service
        self.__fhir_request_service = FhirRequestService(timeout=10, backoff=0.1)

    def get_resource_history(
        self,
        supplier_id: str,
        resource_type: str,
        resource_id: str | None = None,
        _since: datetime | None = None,
    ) -> Bundle:
        supplier = self.__supplier_service.get_one(supplier_id)

        history = self.__fhir_request_service.get_resource_history(
            resource_type=resource_type,
            url=supplier.endpoint,
            resource_id=resource_id,
            _since=_since,
        )
        return Bundle(**history)

    def get_latest_entry_from_reference(
        self, supplier_id: str, reference: Dict[str, str]
    ) -> Entry:
        supplier = self.__supplier_service.get_one(supplier_id)

        split_ref = reference["reference"].split("/")
        res_type = split_ref[0]
        res_id = split_ref[1]

        res_history = self.__fhir_request_service.get_resource_history(
            resource_type=res_type,
            url=supplier.endpoint,
            resource_id=res_id,
        )
        history_bundle = Bundle(**res_history)

        # Get latest (which is the first) from history Bundle
        entries = history_bundle.entry
        if entries is None:
            raise ValueError("Entry not found")

        latest: Entry = entries[0]
        if latest.request and latest.request.method == "DELETE":
            raise RuntimeError("REFERENCED RESOURCE WAS DELETED")

        return latest
