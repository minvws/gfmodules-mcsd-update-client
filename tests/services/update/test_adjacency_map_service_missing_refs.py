from unittest.mock import MagicMock

from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import NodeReference
from app.services.update.adjacency_map_service import AdjacencyMapService


def test_fetch_and_add_missing_should_only_fetch_unattempted_references(adjacency_map_service: AdjacencyMapService) -> None:
    adj_map = AdjacencyMap(nodes=[])
    still_missing = [
        NodeReference(resource_type="Organization", id="A"),
        NodeReference(resource_type="Organization", id="B"),
        NodeReference(resource_type="Organization", id="C"),
    ]
    # Mark A and C as already attempted
    attempted_keys = {("Organization", "A"), ("Organization", "C")}
    missing_refs = still_missing.copy()

    adjacency_map_service.get_directory_data = MagicMock(return_value=[])
    adjacency_map_service.create_node = MagicMock()

    adjacency_map_service._fetch_and_add_missing(  # type: ignore[attr-defined]
        adj_map=adj_map,
        still_missing=still_missing,
        attempted_keys=attempted_keys,
        missing_refs=missing_refs,
    )

    assert adjacency_map_service.get_directory_data.call_count == 1
    args, _ = adjacency_map_service.get_directory_data.call_args
    # Only B should be fetched
    assert len(args[0]) == 1
    assert args[0][0].resource_type == "Organization"
    assert args[0][0].id == "B"
