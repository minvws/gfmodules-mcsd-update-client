from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest
from requests.exceptions import ConnectionError

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference
from app.models.fhir.types import BundleRequestParams
from app.models.resource_map.dto import ResourceMapDto
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.update.adjacency_map_service import AdjacencyMapService
from app.services.update.computation_service import ComputationService

PATCHED_MODULE = "app.services.update.adjacency_map_service.FhirApi.post_bundle"


@pytest.fixture()
def update_client_org_bundle_entry(
    mock_directory_id: str, mock_org: Dict[str, Any]
) -> Dict[str, Any]:
    resource_id = f"{mock_directory_id}-{mock_org['id']}"
    return {
        "fullUrl": f"http://example-update_client-url/Organization/{resource_id}",
        "resource": {"resourceType": "Organization", "id": resource_id},
        "request": {"method": "POST", "url": f"Organization/{resource_id}"},
    }


@pytest.fixture()
def update_client_ep_entry(mock_directory_id: str, mock_ep: Dict[str, Any]) -> Dict[str, Any]:
    resource_id = f"{mock_directory_id}-{mock_ep['id']}"
    return {
        "fullUrl": f"http://example-update_client-url/Endpoint/{resource_id}",
        "resource": {"resourceType": "Endpoint", "id": resource_id},
        "request": {"method": "POST", "url": f"Endpoint/{resource_id}"},
    }


def test_build_adjacency_map_should_succeed(
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
    computation_service: ComputationService,
) -> None:
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    entries = [org_entry, ep_entry]
    org_node = adjacency_map_service.create_node(org_entry)
    org_node.status = computation_service.get_update_status(
        method=org_node.method,
        directory_hash=org_node.directory_hash,
        update_client_hash=org_node.update_client_hash,
    )
    org_node.update_data = adjacency_map_service.create_update_data(org_node, None)
    ep_node = adjacency_map_service.create_node(ep_entry)
    ep_node.status = computation_service.get_update_status(
        method=ep_node.method,
        directory_hash=ep_node.directory_hash,
        update_client_hash=ep_node.update_client_hash,
    )
    ep_node.update_data = adjacency_map_service.create_update_data(ep_node, None)
    nodes = [org_node, ep_node]
    adjacency_map_service.get_directory_data = MagicMock(return_value=[])  # type: ignore
    adjacency_map_service.get_update_client_data = MagicMock(return_value=[])  # type: ignore

    expected = AdjacencyMap(nodes)

    actual = adjacency_map_service.build_adjacency_map(entries)

    assert expected.data == actual.data


def test_build_adjacency_map_should_succeed_when_refs_are_missing(
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
    computation_service: ComputationService,
) -> None:
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    org_node = adjacency_map_service.create_node(org_entry)
    org_node.status = computation_service.get_update_status(
        method=org_node.method,
        directory_hash=org_node.directory_hash,
        update_client_hash=org_node.update_client_hash,
    )
    org_node.update_data = adjacency_map_service.create_update_data(org_node, None)
    ep_node = adjacency_map_service.create_node(ep_entry)
    ep_node.status = computation_service.get_update_status(
        method=ep_node.method,
        directory_hash=ep_node.directory_hash,
        update_client_hash=ep_node.update_client_hash,
    )
    ep_node.update_data = adjacency_map_service.create_update_data(ep_node, None)
    nodes = [org_node, ep_node]
    entries = [org_entry]
    adjacency_map_service.get_directory_data = MagicMock(return_value=[ep_entry])  # type: ignore
    adjacency_map_service.get_update_client_data = MagicMock(return_value=[])  # type: ignore

    expected = AdjacencyMap(nodes)
    actual = adjacency_map_service.build_adjacency_map(entries)

    adjacency_map_service.get_directory_data.assert_called()
    assert expected.data == actual.data


def test_build_adjacency_map_should_succeed_when_data_from_update_client_exists(
    adjacency_map_service: AdjacencyMapService,
    computation_service: ComputationService,
    resource_map_service: ResourceMapService,
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    update_client_org_bundle_entry: Dict[str, Any],
    update_client_ep_entry: Dict[str, Any],
    mock_directory_id: str,
) -> None:
    sup_org_entry = create_bundle_entry(org_history_entry_1)
    sup_ep_entry = create_bundle_entry(ep_history_entry)
    cons_org_entry = create_bundle_entry(update_client_org_bundle_entry)
    cons_ep_entry = create_bundle_entry(update_client_ep_entry)
    org_node = adjacency_map_service.create_node(sup_org_entry)
    org_res_map = resource_map_service.add_one(
        ResourceMapDto(
            directory_id=mock_directory_id,
            directory_resource_id=org_node.resource_id,
            resource_type=org_node.resource_type,
            update_client_resource_id=f"{mock_directory_id}-{org_node.resource_id}",
        )
    )
    org_node.update_client_hash = computation_service.hash_update_client_entry(cons_org_entry)
    org_node.status = computation_service.get_update_status(
        method=org_node.method,
        directory_hash=org_node.directory_hash,
        update_client_hash=org_node.update_client_hash,
    )
    org_node.update_data = adjacency_map_service.create_update_data(
        org_node, org_res_map
    )
    ep_node = adjacency_map_service.create_node(sup_ep_entry)
    ep_res_map = resource_map_service.add_one(
        ResourceMapDto(
            directory_id=mock_directory_id,
            directory_resource_id=ep_node.resource_id,
            resource_type=ep_node.resource_type,
            update_client_resource_id=f"{mock_directory_id}-{ep_node.resource_id}",
        )
    )
    ep_node.update_client_hash = computation_service.hash_update_client_entry(cons_ep_entry)
    ep_node.status = computation_service.get_update_status(
        method=ep_node.method,
        directory_hash=ep_node.directory_hash,
        update_client_hash=ep_node.update_client_hash,
    )
    ep_node.update_data = adjacency_map_service.create_update_data(ep_node, ep_res_map)
    adjacency_map_service.get_directory_data = MagicMock(return_value=[sup_ep_entry])  # type: ignore
    adjacency_map_service.get_update_client_data = MagicMock(  # type: ignore
        return_value=[cons_org_entry, cons_ep_entry]
    )
    expected = AdjacencyMap([org_node, ep_node])

    actual = adjacency_map_service.build_adjacency_map([sup_org_entry])

    assert expected.data == actual.data


@patch(PATCHED_MODULE)
def test_build_adjacency_map_should_raise_exception_when_directory_is_down(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    org_history_entry_1: Dict[str, Any],
) -> None:
    mock_response.side_effect = ConnectionError("Connection Error")
    org_entry = create_bundle_entry(org_history_entry_1)
    with pytest.raises(Exception):
        adjacency_map_service.build_adjacency_map([org_entry])


@patch(PATCHED_MODULE)
def test_get_directory_data_should_succeed(
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
        BundleRequestParams(
            id=mock_org_bundle_entry["resource"]["id"],
            resource_type=mock_org_bundle_entry["resource"]["resourceType"],
        ),
        BundleRequestParams(
            id=mock_ep_bundle_entry["resource"]["id"],
            resource_type=mock_ep_bundle_entry["resource"]["resourceType"],
        ),
    ]
    expected = [org_entry, ep_entry]

    actual = adjacency_map_service.get_directory_data(node_refs)

    assert expected == actual


@patch(PATCHED_MODULE)
def test_get_directory_data_should_raise_exception_when_directory_is_down(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    mock_org: Dict[str, Any],
) -> None:
    mock_response.side_effect = ConnectionError("Whops!!")
    bundle_params = [
        BundleRequestParams(id=mock_org["id"], resource_type=mock_org["resourceType"])
    ]
    with pytest.raises(Exception):
        adjacency_map_service.get_directory_data(bundle_params)


def test_create_node_should_succeed(
    adjacency_map_service: AdjacencyMapService,
    mock_org_bundle_entry: Dict[str, Any],
    expected_node_org: Node,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)

    actual = adjacency_map_service.create_node(entry)

    assert expected_node_org == actual


def test_create_node_references_should_succeed(
    org_node_references: List[NodeReference],
    mock_org_bundle_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)
    adjacency_map_service.create_node_references = MagicMock(  # type: ignore
        return_value=org_node_references
    )
    node = adjacency_map_service.create_node(entry)

    assert node.references == org_node_references
    adjacency_map_service.create_node_references.assert_called_once_with(entry)


def test_create_update_data_should_succeed_and_mark_item_as_new(
    mock_org_bundle_entry: Dict[str, Any],
    mock_directory_id: str,
    adjacency_map_service: AdjacencyMapService,
    computation_service: ComputationService,
    fhir_service: FhirService,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)
    node = adjacency_map_service.create_node(entry)
    node.status = computation_service.get_update_status(
        method=node.method,
        directory_hash=node.directory_hash,
        update_client_hash=node.update_client_hash,
    )
    node.update_data = adjacency_map_service.create_update_data(node, None)
    expected_data = create_bundle_entry(mock_org_bundle_entry)
    assert expected_data.resource is not None
    assert expected_data.request is not None
    fhir_service.namespace_resource_references(expected_data.resource, mock_directory_id)
    expected_data.resource.id = f"{mock_directory_id}-{expected_data.resource.id}"
    expected_data.request.url = f"Organization/{mock_directory_id}-{node.resource_id}"
    expected_data.request.method = "PUT"
    expected_data.fullUrl = None

    assert node.status == "new"
    assert node.update_data is not None
    assert node.update_data.bundle_entry is not None
    assert node.update_data.bundle_entry == expected_data


def test_create_node_data_should_succeed_with_status_update(
    mock_org_bundle_entry: Dict[str, Any],
    update_client_org_bundle_entry: Dict[str, Any],
    mock_directory_id: str,
    adjacency_map_service: AdjacencyMapService,
    computation_service: ComputationService,
    resource_map_service: ResourceMapService,
) -> None:
    directory_entry = create_bundle_entry(mock_org_bundle_entry)
    update_client_entry = create_bundle_entry(update_client_org_bundle_entry)

    node = adjacency_map_service.create_node(directory_entry)
    update_client_res_id = f"{mock_directory_id}-{node.resource_id}"
    res_map = resource_map_service.add_one(
        ResourceMapDto(
            directory_id=mock_directory_id,
            directory_resource_id=node.resource_id,
            resource_type=node.resource_type,
            update_client_resource_id=update_client_res_id,
        )
    )
    node.directory_hash = computation_service.hash_directory_entry(directory_entry)
    node.update_client_hash = computation_service.hash_update_client_entry(update_client_entry)
    node.status = computation_service.get_update_status(
        method=node.method,
        directory_hash=node.directory_hash,
        update_client_hash=node.update_client_hash,
    )
    node.update_data = adjacency_map_service.create_update_data(node, res_map)

    assert node.status == "update"
    assert node.update_data is not None
    assert node.update_data.resource_map_dto is not None
    assert node.update_data.bundle_entry is not None


def test_create_node_update_data_should_raise_exception_when_status_is_unknown(
    mock_org_bundle_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)
    node = adjacency_map_service.create_node(entry)
    with pytest.raises(Exception):
        adjacency_map_service.create_update_data(node, None)


def test_create_node_update_data_should_raise_exception_when_data_is_inconsistent(
    mock_org_bundle_entry: Dict[str, Any],
    adjacency_map_service: AdjacencyMapService,
) -> None:
    entry = create_bundle_entry(mock_org_bundle_entry)
    node = adjacency_map_service.create_node(entry)
    node.method = "DELETE"
    with pytest.raises(Exception):
        adjacency_map_service.create_update_data(node, None)
