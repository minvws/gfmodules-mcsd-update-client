from typing import List
import pytest
from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference


def test_add_nodes_should_succeed(
    adjacency_map: AdjacencyMap, mock_loc_node: Node
) -> None:
    adjacency_map.add_nodes([mock_loc_node])
    actual = adjacency_map.data[mock_loc_node.resource_id]

    assert mock_loc_node == actual


def test_add_node_should_succeed(
    adjacency_map: AdjacencyMap, mock_loc_node: Node
) -> None:
    adjacency_map.add_node(mock_loc_node)
    actual = adjacency_map.data[mock_loc_node.resource_id]

    assert mock_loc_node == actual


def test_get_group_shoud_succeed_with_parent_child_relationship(
    adjacency_map: AdjacencyMap, mock_node_org: Node, mock_node_ep: Node
) -> None:
    expected = [mock_node_org, mock_node_ep]

    actual = adjacency_map.get_group(mock_node_org)
    assert expected == actual
    for node in actual:
        assert node.visited is True


def test_get_group_shoud_succeed_with_child_parent_relationship(
    adjacency_map: AdjacencyMap, mock_node_org: Node, mock_node_ep: Node
) -> None:
    expected = [mock_node_ep, mock_node_org]

    actual = adjacency_map.get_group(mock_node_ep)
    assert expected == actual
    for node in actual:
        assert node.visited is True


def test_get_group_should_succeed_when_a_new_node_with_refs_to_existing_ones_is_added(
    adjacency_map: AdjacencyMap,
    mock_node_org: Node,
    mock_node_ep: Node,
    mock_loc_node: Node,
) -> None:
    expected = [mock_loc_node, mock_node_org, mock_node_ep]

    adjacency_map.add_node(mock_loc_node)
    actual = adjacency_map.get_group(mock_loc_node)

    assert expected == actual


def test_get_group_should_raise_exception_when_node_does_not_exist(
    adjacency_map: AdjacencyMap, mock_loc_node: Node, mock_node_ep: Node
) -> None:
    adjacency_map.data.pop(mock_node_ep.resource_id)
    with pytest.raises(KeyError):
        adjacency_map.get_group(mock_loc_node)


def test_get_missing_refs_should_succeed(
    adjacency_map: AdjacencyMap,
    mock_node_ep: Node,
    org_node_references: List[NodeReference],
) -> None:
    adjacency_map.data.pop(mock_node_ep.resource_id)
    expected = org_node_references

    actual = adjacency_map.get_missing_refs()
    assert expected == actual


def test_get_missing_refs_should_succeed_and_return_empty_list_when_all_refs_exist(
    adjacency_map: AdjacencyMap,
) -> None:
    actual = adjacency_map.get_missing_refs()
    assert actual == []


def test_adjacency_map_should_mark_nodes_as_updated_when_updated_ids_is_passed(
    adjacency_map_with_updated_ids: AdjacencyMap,
) -> None:
    for node in adjacency_map_with_updated_ids.data.values():
        assert node.updated is True
