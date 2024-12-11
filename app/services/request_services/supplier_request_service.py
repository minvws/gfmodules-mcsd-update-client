from fhir.resources.R4B.bundle import Bundle
from fhir.resources.R4B.endpoint import Endpoint

from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.fhir_request_service import FhirRequestService


class SupplierRequestsService:
    def __init__(self, supplier_service: SupplierService) -> None:
        self.__suppler_service = supplier_service
        self.__fhir_request_service = FhirRequestService(timeout=10, backoff=0.1)

    def get_org_history(self, supplier_id: str, org_id: str | None = None) -> Bundle:
        supplier = self.__suppler_service.get_one(supplier_id)

        history = self.__fhir_request_service.get_resource_history(
            resource_type="Organization",
            url=supplier.endpoint,
            resource_id=org_id,
        )
        return Bundle(**history)

    def get_endpoint_history(
        self, supplier_id: str, endpoint_id: str | None = None
    ) -> Bundle:
        supplier = self.__suppler_service.get_one(supplier_id)

        history = self.__fhir_request_service.get_resource_history(
            resource_type="Endpoint",
            url=supplier.endpoint,
            resource_id=endpoint_id,
        )
        return Bundle(**history)

    def get_endpoint(self, supplier_id: str, endpoint_id: str) -> Endpoint:
        supplier = self.__suppler_service.get_one(supplier_id)
        response = self.__fhir_request_service.get_resource(
            resource_type="Endpoint",
            url=supplier.endpoint,
            resource_id=endpoint_id,
        )

        return Endpoint(**response)
