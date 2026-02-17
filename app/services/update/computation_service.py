import copy
from typing import Literal
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource

from app.db.entities.resource_map import ResourceMap
from app.services.fhir.fhir_service import FhirService


class ComputationService:
    def __init__(
        self,
        directory_id: str,
    ) -> None:
        self.__directory_id = directory_id

    def get_update_status(
        self,
        method: str,
        directory_hash: int | None = None,
        update_client_hash: int | None = None,
        resource_map: ResourceMap | None = None,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:
        return self._determine_update_status(
            directory_hash, update_client_hash, resource_map, method
        )

    def _determine_update_status(
        self,
        directory_hash: int | None,
        update_client_hash: int | None,
        resource_map: ResourceMap | None,
        method: str,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:
        if method == "DELETE" and update_client_hash is None:
            return "ignore"

        if (
            method != "DELETE"
            and directory_hash
            and update_client_hash
            and directory_hash == update_client_hash
        ):
            return "equal"

        if method == "DELETE" and update_client_hash is not None:
            return "delete"

        if (
            method != "DELETE"
            and directory_hash is not None
            and update_client_hash is None
            and resource_map is None
        ):
            return "new"

        return "update"

    def hash_directory_entry(self, entry: BundleEntry) -> int | None:
        if entry.resource is None or not isinstance(entry.resource, DomainResource):
            return None

        resource = copy.deepcopy(entry.resource)
        FhirService.namespace_resource_references(resource, self.__directory_id)

        return self.hash_resource(resource)

    def hash_update_client_entry(self, entry: BundleEntry) -> int | None:
        if entry.resource is None or not isinstance(entry.resource, DomainResource):
            return None

        resource = copy.deepcopy(entry.resource)
        return self.hash_resource(resource)

    def hash_resource(self, resource: DomainResource) -> int:
        resource.meta = None
        resource.id = None
        return hash(resource.model_dump().__repr__())
