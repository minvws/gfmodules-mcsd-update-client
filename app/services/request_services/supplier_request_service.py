from datetime import datetime
from enum import Enum
from typing import Dict, List, Tuple

from app.models.fhir.r4.types import Entry, Bundle
from app.services.bundle_tools import get_resource_from_reference

from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.Authenticators import Authenticator
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
    def __init__(
        self, supplier_service: SupplierService, auth: Authenticator, request_count: int
    ) -> None:
        self.__supplier_service = supplier_service
        self.__fhir_request_service = FhirRequestService(
            timeout=10, backoff=0.1, auth=auth
        )
        self.__request_count = request_count

    def get_resource_history(
        self,
        supplier_id: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        _since: datetime | None = None,
    ) -> Bundle:
        supplier = self.__supplier_service.get_one(supplier_id)
        if supplier is None:
            new_bundle = Bundle(type="history")
            new_bundle.entry = []
            return new_bundle

        params = {"_count": str(self.__request_count), "_offset": "0"}

        if _since is not None:
            params.update({"_since": _since.isoformat()})

        histories: List[Bundle] = []
        if resource_type is None:
            for res_type in McsdResources:
                histories.append(
                    self.__fhir_request_service.get_resource_history(
                        base_url=supplier.endpoint,
                        resource_type=res_type.value,
                        resource_id=resource_id,
                        params=params,
                    )
                )
        else:
            histories.append(
                self.__fhir_request_service.get_resource_history(
                    base_url=supplier.endpoint,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    params=params,
                )
            )

        new_bundle = Bundle(type="history")
        new_bundle.entry = []
        for history in histories:
            if history.entry is not None:
                new_bundle.entry.extend(history.entry)

        return new_bundle

    def get_latest_entry_and_length_from_reference(
        self, supplier_id: str, reference: Dict[str, str]
    ) -> Tuple[int, Entry | None]:
        """
        Return the number of entries for this reference from the history, and get the latest entry
        """
        supplier = self.__supplier_service.get_one(supplier_id)
        if supplier is None:
            return 0, None

        (res_type, res_id) = get_resource_from_reference(
            reference["reference"] if "reference" in reference else ""
        )
        if res_type is None:
            return 0, None

        res_history = self.__fhir_request_service.get_resource_history(
            base_url=supplier.endpoint,
            resource_type=res_type,
            resource_id=res_id,
        )

        if res_history.entry is None or len(res_history.entry) == 0:
            return 0, None

        return len(res_history.entry), res_history.entry[0]
