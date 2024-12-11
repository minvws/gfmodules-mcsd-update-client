import re
from typing import Dict, Any, Final, List

from fastapi.encoders import jsonable_encoder
from fhir.resources.R4B.bundle import (
    BundleEntry,
    BundleEntryRequest,
    BundleEntryResponse,
)
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.endpoint import Endpoint
from fhir.resources.R4B.fhirtypes import ReferenceType, BundleEntryType, Id
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.resource import Resource

from app.models.resource_map.dto import ResourceMapDto, ResourceMapUpdateDto
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)

IDENTIFIER_SYSTEM_NAME: Final[str] = "my_own_system_name"


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

    def update_organizations(self, supplier_id: str) -> Dict[str, Any]:
        # TODO: optimize the supplier map check
        # handle first update from supplier
        supplier_history = self.__supplier_request_service.get_org_history(supplier_id)
        entries = supplier_history.entry

        org_ids: set[Id] = set()
        for entry in entries:
            if not isinstance(entry, BundleEntry):
                raise TypeError("entry is not of type BundleEntry")
            if isinstance(entry.resource, DomainResource):
                org_ids.add(entry.resource.id)

        for org_id in org_ids:
            org_history = self.__supplier_request_service.get_org_history(
                supplier_id, org_id
            )
            latest_entry = org_history.entry[0]

            if not isinstance(latest_entry, BundleEntry):
                raise TypeError("entry is not of type BundleEntry")

            self.update_org(supplier_id=supplier_id, resource_id=org_id)

        return {
            "message": "organizations and endpoints are updated",
            "data": self.__resource_map_service.find(supplier_id=supplier_id),
        }

    def update_org(self, supplier_id: str, resource_id: str) -> None:
        resource_map = self.__resource_map_service.get(supplier_resource_id=resource_id)
        supplier_org_history = self.__supplier_request_service.get_org_history(
            supplier_id, resource_id
        )
        entry = supplier_org_history.entry[0]
        if not isinstance(entry, BundleEntry):
            raise TypeError("entry is not of type BundleEntry")
        entry_request = entry.request
        if not isinstance(entry_request, BundleEntryRequest):
            raise TypeError("entry is not of type BundleEntryRequest")

        if (
            resource_map
            and supplier_org_history.total == resource_map.supplier_resource_version
        ):
            return

        if resource_map is None:
            # no map then post
            if entry_request.method == "DELETE":
                return

            if not isinstance(entry.resource, Resource):
                raise TypeError("Entry is not of type resource")

            supplier_org = Organization(**entry.resource.dict())
            supplier_org.id = None
            parent_org = supplier_org.partOf
            if parent_org is not None:
                if not isinstance(parent_org, Reference):
                    raise TypeError("parent is not of type Reference")

                parent_id = str(parent_org.reference.replace("Organization/", ""))
                self.update_org(
                    supplier_id=supplier_id,
                    resource_id=parent_id,
                )
                parent_resource_map = self.__resource_map_service.get(
                    supplier_resource_id=parent_id
                )
                supplier_org.partOf = Reference.construct(
                    reference=f"Organization/{parent_resource_map.consumer_resource_id}"
                )

            supplier_endpoints = supplier_org.endpoint
            if supplier_endpoints is not None:
                supplier_org.endpoint = self._handle_endpoints(
                    supplier_endpoints, supplier_id
                )

            consumer_org = self.__consumer_request_service.post_organization(
                jsonable_encoder(supplier_org.dict())
            )
            resource_map_dto = ResourceMapDto(
                supplier_resource_id=resource_id,
                supplier_resource_version=supplier_org.meta.versionId,
                consumer_resource_id=consumer_org.id,
                consumer_resource_version=consumer_org.meta.versionId,
                resource_type="Organization",
                supplier_id=supplier_id,
            )
            self.__resource_map_service.add_one(resource_map_dto)
            return

        if entry_request.method == "DELETE":
            endpoints = self.__consumer_request_service.get_organization(
                organization_id=resource_map.consumer_resource_id
            ).endpoint
            endpoint_ids = []
            if endpoints is not None:
                for endpoint in endpoints:
                    endpoint_id = endpoint.reference.replace("Endpoint/", "")
                    referenced_org = self.__consumer_request_service.find_organization(
                        {"endpoint": endpoint_id}
                    )
                    if referenced_org.total == 1:
                        endpoint_ids.append(endpoint_id)

            self.__consumer_request_service.delete_organization(
                organization_id=resource_map.consumer_resource_id
            )

            for endpoint_id in endpoint_ids:
                endpoint_resource_map = self.__resource_map_service.get(
                    consumer_resource_id=endpoint_id
                )
                supplier_endpoint = self.__supplier_request_service.get_endpoint(
                    supplier_id, endpoint_resource_map.supplier_resource_id
                )
                self.__consumer_request_service.delete_endpoint(endpoint_id=endpoint_id)
                consumer_endpoint_history = (
                    self.__consumer_request_service.get_endpoint_history(endpoint_id)
                )
                latest_consumer_version = self._get_latest_etag_version(
                    consumer_endpoint_history.entry[0]
                )
                update_dto = ResourceMapUpdateDto(
                    supplier_resource_id=endpoint_resource_map.supplier_resource_id,
                    supplier_resource_version=supplier_endpoint.meta.versionId,
                    consumer_resource_version=latest_consumer_version,
                )
                self.__resource_map_service.update_one(update_dto)

            self.__resource_map_service.update_one(
                ResourceMapUpdateDto(
                    supplier_resource_id=resource_map.supplier_resource_id,
                    supplier_resource_version=self._get_latest_etag_version(
                        supplier_org_history.entry[0]
                    ),
                    consumer_resource_version=resource_map.consumer_resource_version
                    + 1,
                )
            )
            return

        supplier_org = Organization(**entry.resource.dict())
        supplier_org.id = resource_map.consumer_resource_id
        parent_org = supplier_org.partOf
        if parent_org is not None:
            if not isinstance(parent_org, Reference):
                raise TypeError("parent is not of type Reference")

            parent_id = str(parent_org.reference.replace("Organization/", ""))
            self.update_org(
                supplier_id=supplier_id,
                resource_id=parent_id,
            )
            parent_resource_map = self.__resource_map_service.get(
                supplier_resource_id=parent_id
            )
            supplier_org.partOf = Reference.construct(
                reference=f"Organization/{parent_resource_map.consumer_resource_id}"
            )

        supplier_endpoints = supplier_org.endpoint
        if supplier_endpoints is not None:
            supplier_org.endpoint = self._handle_endpoints(
                supplier_endpoints, supplier_id
            )

        self.__consumer_request_service.put_organization(
            organization=jsonable_encoder(supplier_org.dict()),
            resource_id=resource_map.consumer_resource_id,
        )
        self.__resource_map_service.update_one(
            ResourceMapUpdateDto(
                supplier_resource_id=resource_map.supplier_resource_id,
                supplier_resource_version=supplier_org.meta.versionId,
                consumer_resource_version=resource_map.consumer_resource_version + 1,
            )
        )

    def update_endpoint(
        self, entry: BundleEntry, supplier_id: str, resource_id: str
    ) -> Endpoint | None:
        resource_map = self.__resource_map_service.get(
            supplier_resource_id=resource_id,
            supplier_id=supplier_id,
        )
        entry_request = entry.request
        if not isinstance(entry_request, BundleEntryRequest):
            raise TypeError("entry is not of type BundleEntryRequest")

        if resource_map is None:
            if entry_request.method == "DELETE":
                return

            supplier_endpoint = Endpoint(**entry.resource.dict())
            supplier_endpoint.managingOrganization = None
            supplier_endpoint.id = None
            consumer_endpoint = self.__consumer_request_service.post_endpoint(
                endpoint=jsonable_encoder(supplier_endpoint.dict())
            )
            self.__resource_map_service.add_one(
                ResourceMapDto(
                    supplier_resource_id=resource_id,
                    supplier_resource_version=supplier_endpoint.meta.versionId,
                    consumer_resource_id=consumer_endpoint.id,
                    consumer_resource_version=consumer_endpoint.meta.versionId,
                    resource_type="Endpoint",
                    supplier_id=supplier_id,
                )
            )
            return consumer_endpoint

        supplier_endpoint_history = (
            self.__supplier_request_service.get_endpoint_history(
                supplier_id=supplier_id, endpoint_id=resource_id
            )
        )
        latest_supplier_version = self._get_latest_etag_version(
            supplier_endpoint_history.entry[0]
        )

        if entry_request.method == "DELETE":
            self.__consumer_request_service.delete_endpoint(
                endpoint_id=resource_map.consumer_resource_id
            )
            self.__resource_map_service.update_one(
                ResourceMapUpdateDto(
                    supplier_resource_id=resource_map.supplier_resource_id,
                    supplier_resource_version=latest_supplier_version,
                    consumer_resource_version=resource_map.consumer_resource_version
                    + 1,
                )
            )
            return
        else:
            supplier_endpoint = Endpoint(**entry.resource.dict())
            supplier_endpoint.id = resource_map.consumer_resource_id
            supplier_endpoint.managingOrganization = None
            updated_endpoint = self.__consumer_request_service.put_endpoint(
                endpoint=jsonable_encoder(supplier_endpoint.dict()),
                resource_id=resource_map.consumer_resource_id,
            )
            # if resource_map.supplier_resource_version != latest_supplier_version:

            self.__resource_map_service.update_one(
                ResourceMapUpdateDto(
                    supplier_resource_id=resource_map.supplier_resource_id,
                    supplier_resource_version=supplier_endpoint.meta.versionId,
                    consumer_resource_version=updated_endpoint.meta.versionId,
                )
            )
            return updated_endpoint

    # Endpoint is reference by 2 organization, remove the reference of the deleted org
    # Endpoint has a managing org <Maybe>
    # Delete Endpoints for orgs that do not exist anymore

    def _handle_endpoints(
        self, endpoints: List[ReferenceType], supplier_id: str
    ) -> list[Reference]:
        endpoints_refs: list[Reference] = []
        for endpoint_ref in endpoints:
            if not isinstance(endpoint_ref, Reference):
                raise TypeError("endpoint is not of type Reference")
            supplier_endpoint_id = endpoint_ref.reference.replace("Endpoint/", "")
            supplier_endpoint_history = (
                self.__supplier_request_service.get_endpoint_history(
                    supplier_id, supplier_endpoint_id
                )
            )
            latest_entry = supplier_endpoint_history.entry[0]
            updated_endpoint = self.update_endpoint(
                latest_entry, supplier_id, supplier_endpoint_id
            )
            endpoints_refs.append(
                Reference.construct(reference=f"Endpoint/{updated_endpoint.id}")
            )
        return endpoints_refs

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
