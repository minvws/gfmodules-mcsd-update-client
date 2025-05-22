import copy
from typing import Literal

from fhir.resources.R4B.bundle import BundleEntry

from app.models.adjacency.node import ToBeUpdatedNode
from app.services.fhir.fhir_service import FhirService


status: Literal["ignore", "equal", "delete", "update", "new"]


class NodeService:
    def __init__(self,
                 supplier_id: str,
                 fhir_service: FhirService
    ):
        self.__fhir_service = fhir_service
        self.__supplier_id = supplier_id

    def determine_status(
        self,
    ) -> Literal["ignore", "equal", "delete", "update", "new"]:
        if (
            self.supplier_data.method == "DELETE"
            and self.consumer_data.resource is None
        ):
            return "ignore"

        if (
            self.supplier_data.method != "DELETE"
            and self.supplier_data.hash_value
            and self.consumer_data.hash_value
            and self.supplier_data.hash_value == self.consumer_data.hash_value
        ):
            return "equal"

        if (
            self.supplier_data.method == "DELETE"
            and self.consumer_data.resource is not None
        ):
            return "delete"

        if (
            self.supplier_data.method != "DELETE"
            and self.supplier_data.hash_value is not None
            and self.consumer_data.hash_value is None
            and self.resource_map is None
        ):
            return "new"

        return "update"


    def get_supplier_hash(self, entry: BundleEntry) -> str | None:
        if entry.resource is None:
            return None

        resource = copy.deepcopy(entry.resource)
        self.__fhir_service.namespace_resource_reference(resource, self.__supplier_id)
        resource.meta = None
        resource.id = None

        return hash(resource.model_dump().__repr__())

    def get_consumer_hash(self, entry: BundleEntry) -> str | None:
        if entry.resource is None:
            return None

        res = copy.deepcopy(entry.resource)
        res.meta = None
        res.id = None
        return hash(res.model_dump().__repr__())


    #
    # def create_bundle_entry_from_node(self, node: ToBeUpdatedNode):
    #     pass
    #
    # def create_node(self,
    #                 entry: BundleEntry
    #                 ) -> BaseNode:
    #     pass
