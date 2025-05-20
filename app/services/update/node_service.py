from app.models.adjacency.node import ToBeUpdatedNode
from app.services.fhir.fhir_service import FhirService


class NodeService:
    def __init__(self,
                 fhir_service: FhirService
    ):
        self._fhir_service = fhir_service


    def create_bundle_entry_from_node(self, node: ToBeUpdatedNode):
        pass

    def create_node(self,
                    entry: BundleEntry
                    ) -> BaseNode:
        pass
