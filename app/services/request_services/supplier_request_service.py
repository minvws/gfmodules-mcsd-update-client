from datetime import datetime

from fhir.resources.R4B.bundle import Bundle

from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.fhir_request_service import FhirRequestService


class SupplierRequestsService:
    def __init__(self, supplier_service: SupplierService) -> None:
        self.__suppler_service = supplier_service
        self.__fhir_request_service = FhirRequestService(timeout=10, backoff=0.1)

    def get_resource_history(self, resource_type: str, supplier_id: str|None=None, resource_id: str | None = None, _since: datetime|None=None) -> list[Bundle]:
        suppliers = list(self.__suppler_service.get_all()) if supplier_id is None else [self.__suppler_service.get_one(supplier_id)]
        bundle_list: list[Bundle] = []
        for supplier in suppliers:
            history = self.__fhir_request_service.get_resource_history(
                resource_type=resource_type,
                url=supplier.endpoint,
                resource_id=resource_id,
                _since=_since,
            )
            bundle_list.append(Bundle(**history))
        return bundle_list
