from collections import deque
from itertools import chain
from typing import Dict, List, Deque

from app.models.adjacency.node import Node, NodeReference

AdjacencyData = Dict[str, Node]


class AdjacencyMap:
    def __init__(self, nodes: List[Node]) -> None:
        self.data: AdjacencyData = self._create_adj_map(nodes)

    def add_nodes(self, nodes: List[Node]) -> None:
        self.data.update([(node.resource_id, node) for node in nodes])

    def add_node(self, node: Node) -> None:
        self.data[node.resource_id] = node

    def node_count(self) -> int:
        """
        Returns the number of nodes in the adjacency map.
        """
        return len(self.data)

    def get_group(self, node: Node) -> List[Node]:
        """
        Use Breadth First Search approach to retrieve Node and siblings.
        """
        queue: Deque["Node"] = deque()
        group = []

        node.visited = True
        queue.append(node)
        group.append(node)

        while queue:
            current = queue.popleft()
            for ref in current.references:
                sibling = self.data[ref.id]
                if sibling.visited is False:
                    sibling.visited = True
                    queue.append(sibling)
                    group.append(sibling)

        return group

    def get_missing_refs(self) -> List[NodeReference]:
        """
        Returns a list of references that are not present in the adjacency map.
        """
        refs = list(
            chain.from_iterable([node.references for node in self.data.values()])
        )
        return [r for r in refs if not self._ref_in_adj_map(r)]

    def _ref_in_adj_map(self, adj_ref: NodeReference) -> bool:
        """
        Returns true when the node reference is present in the adjacency map.
        """
        return adj_ref.id in self.data.keys()

    def _create_adj_map(self, nodes: List[Node]) -> AdjacencyData:
        ids = [node.resource_id for node in nodes]
        data: AdjacencyData = dict(zip(ids, nodes))

        return data
