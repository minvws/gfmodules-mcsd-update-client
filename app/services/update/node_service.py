from typing import Literal

from app.models.adjacency.node import ToBeUpdatedNode
from app.services.fhir.fhir_service import FhirService


status: Literal["ignore", "equal", "delete", "update", "new"]


class NodeService:
    def __init__(self,
                 fhir_service: FhirService
    ):
        self._fhir_service = fhir_service

    def determine_status(
        self,
        supplier_
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
    #
    # def create_bundle_entry_from_node(self, node: ToBeUpdatedNode):
    #     pass
    #
    # def create_node(self,
    #                 entry: BundleEntry
    #                 ) -> BaseNode:
    #     pass
