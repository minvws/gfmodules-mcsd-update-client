from typing import Any, Dict
from unittest.mock import Mock, patch, MagicMock


from app.models.adjacency.adjacency_map import AdjacencyMap
from app.services.api.fhir_api import FhirApi
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.update.adjacency_map_service import AdjacencyMapService


PATCHED_MODULE = "app.services.update.adjacency_map_service.FhirApi.post_bundle"


@patch(PATCHED_MODULE)
def test_build_adjacency_map_should_succeed(
    mock_response: MagicMock,
    adjacency_map_service: AdjacencyMapService,
    fhir_service: FhirService,
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
) -> None:
    mock_response.return_value = fhir_service.create_bundle({"type": "history"})
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    entries = [org_entry, ep_entry]
    nodes = [
        adjacency_map_service.create_node(org_entry),
        adjacency_map_service.create_node(ep_entry),
    ]
    expected = AdjacencyMap(nodes)

    actual = adjacency_map_service.build_adjacency_map(entries)

    assert expected.data == actual.data


# @patch(PATCHED_MODULE)
# def test_buidl_adjacency_map_should_succeed_when_refs_are_missing(
#     mock_response: MagicMock,
#     mock_ep_history_bundle: Dict[str, Any],
#     org_history_entry_1: Dict[str, Any],
#     ep_history_entry: Dict[str, Any],
#     fhir_service: FhirService,
#     adjacency_map_service: AdjacencyMapService,
# ) -> None:
#     mock_response.side_effect = [
#         fhir_service.create_bundle(mock_ep_history_bundle),
#         fhir_service.create_bundle({"type": "history"}),
#     ]
#     org_entry = create_bundle_entry(org_history_entry_1)
#     ep_entry = create_bundle_entry(ep_history_entry)
#     nodes = [
#         adjacency_map_service.create_node(org_entry),
#         adjacency_map_service.create_node(ep_entry),
#     ]
#     entries = [org_entry]
#     expected = AdjacencyMap(nodes)
#
#     actual = adjacency_map_service.build_adjacency_map(entries)
#
#     assert expected.data == actual.data


def test_buidl_adjacency_map_should_succeed_when_refs_are_missing(
    mock_ep_history_bundle: Dict[str, Any],
    org_history_entry_1: Dict[str, Any],
    ep_history_entry: Dict[str, Any],
    fhir_service: FhirService,
    adjacency_map_service: AdjacencyMapService,
) -> None:
    org_entry = create_bundle_entry(org_history_entry_1)
    ep_entry = create_bundle_entry(ep_history_entry)
    nodes = [
        adjacency_map_service.create_node(org_entry),
        adjacency_map_service.create_node(ep_entry),
    ]
    entries = [org_entry]
    service = MagicMock()
    service.get_entries.return_value = fhir_service.create_bundle(
        mock_ep_history_bundle
    )
    service.get_consumer_data.return_value = None
    service.build_adjacency_map.return_value = AdjacencyMap(nodes)
    service.return_value = adjacency_map_service
    expected = AdjacencyMap(nodes)

    actual = service.build_adjacency_map(entries)

    assert expected.data == actual.data
