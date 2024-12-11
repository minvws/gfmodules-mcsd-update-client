import re
from datetime import datetime
from enum import Enum
from typing import Dict, Any

from fastapi.encoders import jsonable_encoder
from fhir.resources.R4B.bundle import (
    BundleEntry,
    BundleEntryResponse, Bundle,
)
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.fhirtypes import BundleEntryType

from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)

class McsdResources(Enum):
    ORGANIZATION = "Organization"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTH_CARE_SERVICE = "HealthcareService"
    ENDPOINT = "Endpoint"

class UpdateConsumerService:
    def __init__(
        self,
        supplier_request_service: SupplierRequestsService,
        consumer_request_service: ConsumerRequestService,
        resource_map_service: ResourceMapService,
    ):
        self.__supplier_request_service = supplier_request_service
        self.__consumer_request_service = consumer_request_service
        self.__resource_map_service = resource_map_service

    def update_resources(self, supplier_id: str|None=None, resource_type: str|None=None, _since: datetime|None=None) -> list[Bundle]:
        bundles: list[Bundle] = []
        if resource_type is None:
            for resource in McsdResources:
                bundles.extend(self.__supplier_request_service.get_resource_history(
                    resource_type=resource.value, supplier_id=supplier_id, _since=_since))
        else:
            bundles.extend(self.__supplier_request_service.get_resource_history(
                resource_type=resource_type, supplier_id=supplier_id, _since=_since))

        return [jsonable_encoder(bundle.dict()) for bundle in bundles]

    @staticmethod
    def _get_latest_etag_version(first_entry: BundleEntry | BundleEntryType) -> int:
        if not isinstance(first_entry, BundleEntry):
            raise TypeError("first entry is not of type BundleEntry")
        response = first_entry.response
        if not isinstance(response, BundleEntryResponse):
            raise TypeError("response is not of type BundleEntryResponse")

        etag = re.search(r"(?<=\")\d*(?=\")", response.etag)
        if etag is None:
            raise ValueError("Did not find etag")
        return int(etag.group())
