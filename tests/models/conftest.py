from typing import Any, Dict, List
from fhir.resources.R4B.bundle import BundleEntry
import pytest

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import (
    ConsumerNodeData,
    Node,
    NodeReference,
    SupplierNodeData,
)
from app.models.resource_map.dto import ResourceMapDto
from app.services.fhir.fhir_service import FhirService
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry


def __create_mock_node(
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


@pytest.fixture()
def mock_supplier_id() -> str:
    return "example-supplier-id"


@pytest.fixture()
def org_node_references(mock_ep: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_ep["id"], resource_type=mock_ep["resourceType"])]


@pytest.fixture()
def ep_node_references(mock_org: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_org["id"], resource_type=mock_org["resourceType"])]


@pytest.fixture()
def location_node_references(mock_org: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_org["id"], resource_type=mock_org["resourceType"])]


@pytest.fixture()
def mock_org_bundle_entry(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "PUT", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def mock_ep_bundle_entry(mock_ep: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service/fhir/Endpoint/{mock_ep['id']}",
        "resource": mock_ep,
        "request": {"method": "PUT", "url": f"Endpoint/{mock_ep['id']}"},
    }


@pytest.fixture()
def mock_loc_bundle_entry(mock_location: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service/fhir/Location/{mock_location['id']}",
        "resource": mock_location,
        "request": {"method": "PUT", "url": f"Location/{mock_location['id']}"},
    }


@pytest.fixture()
def mock_loc_node(
    mock_loc_bundle_entry: Dict[str, Any],
    location_node_references: List[NodeReference],
    mock_supplier_id: str,
    fhir_service: FhirService,
) -> Node:
    entry = create_bundle_entry(mock_loc_bundle_entry)
    return __create_mock_node(
        bundle_entry=entry,
        node_refs=location_node_references,
        supplier_id=mock_supplier_id,
        fhir_service=fhir_service,
    )


@pytest.fixture()
def mock_node_org(
    mock_org_bundle_entry: Dict[str, Any],
    org_node_references: List[NodeReference],
    mock_supplier_id: str,
    fhir_service: FhirService,
) -> Node:
    entry = create_bundle_entry(mock_org_bundle_entry)
    return __create_mock_node(
        bundle_entry=entry,
        node_refs=org_node_references,
        supplier_id=mock_supplier_id,
        fhir_service=fhir_service,
    )


@pytest.fixture()
def mock_node_ep(
    mock_ep_bundle_entry: Dict[str, Any],
    ep_node_references: List[NodeReference],
    mock_supplier_id: str,
    fhir_service: FhirService,
) -> Node:
    entry = create_bundle_entry(mock_ep_bundle_entry)
    return __create_mock_node(
        bundle_entry=entry,
        node_refs=ep_node_references,
        supplier_id=mock_supplier_id,
        fhir_service=fhir_service,
    )


@pytest.fixture()
def adjacency_map(mock_node_org: Node, mock_node_ep: Node) -> AdjacencyMap:
    return AdjacencyMap(nodes=[mock_node_org, mock_node_ep])


@pytest.fixture()
def adjacency_map_with_updated_ids(
    mock_node_org: Node, mock_node_ep: Node
) -> AdjacencyMap:
    return AdjacencyMap(
        nodes=[mock_node_org, mock_node_ep],
        updated_ids=[mock_node_org.resource_id, mock_node_ep.resource_id],
    )
