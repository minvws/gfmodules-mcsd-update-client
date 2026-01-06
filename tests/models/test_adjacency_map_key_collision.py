from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node


def test_adjacency_map_should_not_collide_on_same_id_different_resource_type(
    mock_node_org: Node, mock_node_ep: Node
) -> None:
    # Force same id to simulate potential collisions
    mock_node_org.resource_id = "123"
    mock_node_ep.resource_id = "123"

    adj = AdjacencyMap(nodes=[mock_node_org, mock_node_ep])

    assert adj.data[mock_node_org.cache_key()] is mock_node_org
    assert adj.data[mock_node_ep.cache_key()] is mock_node_ep
    assert len(adj.data) == 2
