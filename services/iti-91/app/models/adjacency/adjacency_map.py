from collections import deque
from itertools import chain
from typing import Deque, Dict, List

from app.models.adjacency.node import Node, NodeReference

# Map key is always "<resourceType>/<id>" to avoid collisions across resource types.
AdjacencyData = Dict[str, Node]


class AdjacencyMap:
    def __init__(self, nodes: List[Node]) -> None:
        self.data: AdjacencyData = self._create_adj_map(nodes)

    def add_nodes(self, nodes: List[Node]) -> None:
        self.data.update([(node.cache_key(), node) for node in nodes])

    def add_node(self, node: Node) -> None:
        self.data[node.cache_key()] = node

    def node_count(self) -> int:
        """Returns the number of nodes in the adjacency map."""
        return len(self.data)

    def get_group(self, node: Node) -> List[Node]:
        """
        Use Breadth First Search approach to retrieve Node and siblings.

        "siblings" are nodes reachable via reference edges.
        """
        queue: Deque[Node] = deque()
        group: list[Node] = []

        node.visited = True
        queue.append(node)
        group.append(node)

        while queue:
            current = queue.popleft()
            for ref in current.references:
                sibling = self.data[ref.cache_key()]
                if not sibling.visited:
                    sibling.visited = True
                    queue.append(sibling)
                    group.append(sibling)

        return group

    def get_missing_refs(self) -> List[NodeReference]:
        """Returns a list of references that are not present in the adjacency map."""
        refs = list(chain.from_iterable([node.references for node in self.data.values()]))
        return [r for r in refs if not self._ref_in_adj_map(r)]

    def _ref_in_adj_map(self, adj_ref: NodeReference) -> bool:
        """Returns True when the node reference is present in the adjacency map."""
        return adj_ref.cache_key() in self.data

    def _create_adj_map(self, nodes: List[Node]) -> AdjacencyData:
        ids = [node.cache_key() for node in nodes]
        return dict(zip(ids, nodes))
