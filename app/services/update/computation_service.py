import copy
from typing import Literal
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource

from app.db.entities.resource_map import ResourceMap
from app.services.fhir.fhir_service import FhirService
from app.services.entity.resource_map_service import ResourceMapService


class ComputationService:
    def __init__(
        self,
        supplier_id: str,
        fhir_service: FhirService,
        resource_map_service: ResourceMapService,
    ) -> None:
        self.__supplier_id = supplier_id
        self.__fhir_service = fhir_service
        self.__resource_map_service = resource_map_service

    def get_update_status(
        self,
        resource_id: str,
        resource_type: str,
        method: str,
        supplier_hash: int | None = None,
        consumer_hash: int | None = None,
        resource_map: ResourceMap | None = None,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:
        # resource_map = self.__resource_map_service.get(
        #     self.__supplier_id, resource_type, resource_id
        # )

        return self._determine_update_status(
            supplier_hash, consumer_hash, resource_map, method
        )

    def _determine_update_status(
        self,
        supplier_hash: int | None,
        consumer_hash: int | None,
        resource_map: ResourceMap | None,
        method: str,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:
        if method == "DELETE" and consumer_hash is None:
            return "ignore"

        if (
            method != "DELETE"
            and supplier_hash
            and consumer_hash
            and supplier_hash == consumer_hash
        ):
            return "equal"

        if method == "DELETE" and consumer_hash is not None:
            return "delete"

        if (
            method != "DELETE"
            and supplier_hash is not None
            and consumer_hash is None
            and resource_map is None
        ):
            return "new"

        return "update"

    def hash_supplier_entry(self, entry: BundleEntry) -> int | None:
        if entry.resource is None:
            return None

        resource = copy.deepcopy(entry.resource)
        self.__fhir_service.namespace_resource_references(resource, self.__supplier_id)

        return self.hash_resource(resource)

    def hash_consumer_entry(self, entry: BundleEntry) -> int | None:
        if entry.resource is None:
            return None

        resource = copy.deepcopy(entry.resource)
        return self.hash_resource(resource)

    def hash_resource(self, resource: DomainResource) -> int:
        resource.meta = None
        resource.id = None
        return hash(resource.model_dump().__repr__())
