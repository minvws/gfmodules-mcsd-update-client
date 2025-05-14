from typing import Any, Dict
from unittest.mock import MagicMock


from app.models.adjacency.adjacency_map import AdjacencyMap
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.update.adjacency_map_service import AdjacencyMapService


PATCHED_MODULE = "app.services.update.adjacency_map_service.FhirApi.post_bundle"


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
    adjacency_map_service.get_supplier_data = MagicMock(return_value=[])
    adjacency_map_service.get_consumer_data = MagicMock(return_value=[])

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
    nodes = [
        adjacency_map_service.create_node(org_entry),
        adjacency_map_service.create_node(ep_entry),
    ]
    entries = [org_entry]
    adjacency_map_service.get_supplier_data = MagicMock(return_value=[ep_entry])
    adjacency_map_service.get_consumer_data = MagicMock(return_value=[])

    expected = AdjacencyMap(nodes)
    actual = adjacency_map_service.build_adjacency_map(entries)

    assert expected.data == actual.data
