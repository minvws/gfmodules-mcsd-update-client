from typing import List
from fhir.resources.R4B.bundle import BundleEntry

from app.models.adjacency.node import (
    ConsumerNodeData,
    Node,
    NodeReference,
    SupplierNodeData,
)
from app.models.resource_map.dto import ResourceMapDto
from app.services.fhir.fhir_service import FhirService


def create_mock_node(
    bundle_entry: BundleEntry,
    node_refs: List[NodeReference],
    supplier_id: str,
    fhir_service: FhirService,
) -> Node:
    """
    Helper function to create mock nodes
    """
    res_type, id = fhir_service.get_resource_type_and_id_from_entry(bundle_entry)
    method = fhir_service.get_request_method_from_entry(bundle_entry)
    supplier_data = SupplierNodeData(
        supplier_id=supplier_id, references=node_refs, method=method, entry=bundle_entry
    )
    resource_map = ResourceMapDto(
        supplier_id=supplier_id,
        resource_type=res_type,
        supplier_resource_id=id,
        consumer_resource_id=f"{supplier_id}-{id}",
        history_size=1,
    )

    return Node(
        resource_id=id,
        resource_type=res_type,
        supplier_data=supplier_data,
        consumer_data=ConsumerNodeData(resource=None),
        resource_map=resource_map,
    )
