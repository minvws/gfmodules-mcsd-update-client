from datetime import datetime
from enum import Enum
from typing import Dict, List

from app.models.fhir.r4.types import Entry, Bundle

from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.fhir_request_service import FhirRequestService


class McsdResources(Enum):
    ORGANIZATION_AFFILIATION = "OrganizationAffiliation"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTH_CARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    ENDPOINT = "Endpoint"
    ORGANIZATION = "Organization"


class SupplierRequestsService:
    def __init__(self, supplier_service: SupplierService) -> None:
        self.__supplier_service = supplier_service
        self.__fhir_request_service = FhirRequestService(timeout=10, backoff=0.1)

    def get_resource_history(
        self,
        supplier_id: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        _since: datetime | None = None,
    ) -> Bundle:
        supplier = self.__supplier_service.get_one(supplier_id)

        histories: List[Bundle] = []
        if resource_type is None:
            for res_type in McsdResources:
                histories.append(
                    self.__fhir_request_service.get_resource_history(
                        resource_type=res_type.value,
                        url=supplier.endpoint,
                        resource_id=resource_id,
                        params={"_since": _since.isoformat()} if _since else None,
                    )
                )
        else:
            histories.append(
                self.__fhir_request_service.get_resource_history(
                    resource_type=resource_type,
                    url=supplier.endpoint,
                    resource_id=resource_id,
                    params={"_since": _since.isoformat()} if _since else None,
                )
            )
        extended_history = histories.pop(0)
        if extended_history.entry is None or len(extended_history.entry) == 0:
            raise Exception("History is empty")
        for history in histories:
            if history.entry is None or len(history.entry) == 0:
                raise Exception("History is empty")
            extended_history.entry.extend(history.entry)
        return extended_history

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

        if res_history.entry is None or len(res_history.entry) == 0:
            raise Exception("HISTORY IS EMPTY")

        # Get latest (which is the first) from history Bundle
        latest = res_history.entry[0]

        if latest.request.method == "DELETE":
            raise Exception("Referenced resource was deleted")

        return latest
