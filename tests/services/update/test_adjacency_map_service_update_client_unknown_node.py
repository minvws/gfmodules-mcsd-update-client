from unittest.mock import MagicMock

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference
from fhir.resources.bundle import BundleEntry
from fhir.resources.organization import Organization
from app.services.update.adjacency_map_service import AdjacencyMapService


def test_update_client_hashes_should_warn_and_skip_when_entry_not_in_adjacency_map(
    adjacency_map_service: AdjacencyMapService, mock_directory_id: str, caplog
) -> None:
    adj_map = AdjacencyMap(nodes=[])
    # Only include Organization/A in the map
    adj_map.add_node(Node(resource_type="Organization", resource_id="A", method="PUT"))

    # Force the service to think Organization/B is missing from update-client
    adjacency_map_service._AdjacencyMapService__get_update_client_missing_targets = MagicMock(  # type: ignore[attr-defined]
        return_value=[NodeReference(resource_type="Organization", id="B")]
    )

    # Update-client returns Organization/<dir>-B which is NOT in adj_map -> should warn and continue
    update_client_entry = BundleEntry.model_construct(
        fullUrl="urn:uuid:1",
        resource=Organization.model_construct(id=f"{mock_directory_id}-B"),
    )
    adjacency_map_service.get_update_client_data = MagicMock(return_value=[update_client_entry])

    with caplog.at_level("WARNING"):
        adjacency_map_service._update_client_hashes(adj_map)

    assert "not in the adjacency map" in caplog.text
