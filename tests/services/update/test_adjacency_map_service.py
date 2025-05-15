from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectionError

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference, SupplierNodeData
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.update.adjacency_map_service import AdjacencyMapService

PATCHED_MODULE = "app.services.update.adjacency_map_service.FhirApi.post_bundle"


@pytest.fixture()
def consumer_org_entry(
    mock_supplier_id: str, mock_org: Dict[str, Any]
) -> Dict[str, Any]:
    resource_id = f"{mock_supplier_id}-{mock_org['id']}"
    return {
        "fullUrl": f"http://example-consumer-url/Organization/{resource_id}",
        "resource": {"resourceType": "Organization", "id": resource_id},
        "request": {"method": "POST", "url": f"Organization/{resource_id}"},
    }


@pytest.fixture()
def consumer_ep_entry(mock_supplier_id: str, mock_ep: Dict[str, Any]) -> Dict[str, Any]:
    resource_id = f"{mock_supplier_id}-{mock_ep['id']}"
    return {
        "fullUrl": f"http://example-consumer-url/Endpoint/{resource_id}",
        "resource": {"resourceType": "Endpoint", "id": resource_id},
        "request": {"method": "POST", "url": f"Endpoint/{resource_id}"},
    }


@pytest.fixture()
def mock_supplier_node_data(
    mock_supplier_id: str,
    org_history_entry_1: Dict[str, Any],
    org_node_references: List[NodeReference],
    fhir_service: FhirService,
) -> SupplierNodeData:
    bundle_entry = create_bundle_entry(org_history_entry_1)
    method = fhir_service.get_request_method_from_entry(bundle_entry)
    return SupplierNodeData(
        supplier_id=mock_supplier_id,
        references=org_node_references,
        method=method,
        entry=bundle_entry,
    )


def test_build_adjacency_map_should_succeed(
    adjacency_map_service: AdjacencyMapService,
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
) -> None:
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    entries = [org_entry, ep_entry]
    nodes = [
        adjacency_map_service.create_node(org_entry),
        adjacency_map_service.create_node(ep_entry),
    ]
    adjacency_map_service.get_supplier_data = MagicMock(return_value=[])  # type: ignore
    adjacency_map_service.get_consumer_data = MagicMock(return_value=[])  # type: ignore

    expected = AdjacencyMap(nodes)

    actual = adjacency_map_service.build_adjacency_map(entries)

    assert expected.data == actual.data


def test_build_adjacency_map_should_succeed_when_refs_are_missing(
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
) -> None:
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    org_node = adjacency_map_service.create_node(org_entry)
    ep_node = adjacency_map_service.create_node(ep_entry)
    nodes = [org_node, ep_node]
    entries = [org_entry]
    adjacency_map_service.get_supplier_data = MagicMock(return_value=[ep_entry])  # type: ignore
    adjacency_map_service.get_consumer_data = MagicMock(return_value=[])  # type: ignore

    expected = AdjacencyMap(nodes)
    actual = adjacency_map_service.build_adjacency_map(entries)

    adjacency_map_service.get_supplier_data.assert_called()
    assert expected.data == actual.data


def test_build_adjacency_map_should_succeed_when_data_from_consumer_exists(
    adjacency_map_service: AdjacencyMapService,
    fhir_service: FhirService,
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    consumer_org_entry: Dict[str, Any],
    consumer_ep_entry: Dict[str, Any],
) -> None:
    sup_org_entry = create_bundle_entry(org_history_entry_1)
    sup_ep_entry = create_bundle_entry(ep_history_entry)
    cons_org_entry = create_bundle_entry(consumer_org_entry)
    cons_ep_entry = create_bundle_entry(consumer_ep_entry)
    org_node = adjacency_map_service.create_node(sup_org_entry)
    org_node.consumer_data.resource = fhir_service.create_resource(
        consumer_org_entry["resource"]
    )
    ep_node = adjacency_map_service.create_node(sup_ep_entry)
    ep_node.consumer_data.resource = fhir_service.create_resource(
        consumer_ep_entry["resource"]
    )
    adjacency_map_service.get_supplier_data = MagicMock(return_value=[sup_ep_entry])  # type: ignore
    adjacency_map_service.get_consumer_data = MagicMock(  # type: ignore
        return_value=[cons_org_entry, cons_ep_entry]
    )
    expected = AdjacencyMap([org_node, ep_node])

    actual = adjacency_map_service.build_adjacency_map([sup_org_entry])

    assert expected.data == actual.data


@patch(PATCHED_MODULE)
def test_build_adjacency_map_should_raise_exception_when_supplier_is_down(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    org_history_entry_1: Dict[str, Any],
) -> None:
    mock_response.side_effect = ConnectionError("Connection Error")
    org_entry = create_bundle_entry(org_history_entry_1)
    with pytest.raises(Exception):
        adjacency_map_service.build_adjacency_map([org_entry])


@patch(PATCHED_MODULE)
def test_get_supplier_data_should_succeed(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    fhir_service: FhirService,
    mock_org_bundle_entry: Dict[str, Any],
    mock_ep_bundle_entry: Dict[str, Any],
    mock_bundle_of_bundles: Dict[str, Any],
) -> None:
    bundle = fhir_service.create_bundle(mock_bundle_of_bundles)
    assert bundle.entry is not None
    mock_response.return_value = bundle
    org_entry = create_bundle_entry(mock_org_bundle_entry)
    ep_entry = create_bundle_entry(mock_ep_bundle_entry)
    node_refs = [
        NodeReference(
            id=mock_org_bundle_entry["resource"]["id"],
            resource_type=mock_org_bundle_entry["resource"]["resourceType"],
        ),
        NodeReference(
            id=mock_ep_bundle_entry["resource"]["id"],
            resource_type=mock_ep_bundle_entry["resource"]["resourceType"],
        ),
    ]
    expected = [org_entry, ep_entry]

    actual = adjacency_map_service.get_supplier_data(node_refs)

    assert expected == actual


@patch(PATCHED_MODULE)
def test_get_supplier_data__should_raise_exception_when_supplier_is_down(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    mock_org: Dict[str, Any],
) -> None:
    mock_response.side_effect = ConnectionError("Whops!!")
    node_refs = [
        NodeReference(id=mock_org["id"], resource_type=mock_org["resourceType"])
    ]
    with pytest.raises(Exception):
        adjacency_map_service.get_supplier_data(node_refs)


def test_create_node_should_succeed(
    adjacency_map_service: AdjacencyMapService,
    mock_org_bundle_entry: Dict[str, Any],
    mock_node_org: Node,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)
    mock_node_org.resource_map = None

    actual = adjacency_map_service.create_node(entry)

    assert mock_node_org == actual


def test_create_supplier_data_should_succeed(
    adjacency_map_service: AdjacencyMapService,
    mock_supplier_node_data: SupplierNodeData,
    org_history_entry_1: Dict[str, Any],
    mock_supplier_id: str,
) -> None:
    bundle_entry = create_bundle_entry(org_history_entry_1)

    expected = adjacency_map_service.create_supplier_data(
        mock_supplier_id, bundle_entry
    )

    assert expected == mock_supplier_node_data
