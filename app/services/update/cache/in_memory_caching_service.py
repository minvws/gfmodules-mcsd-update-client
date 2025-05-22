from typing import Dict
from app.models.adjacency.node import Node
from app.services.update.cache.caching_service import CachingService


class InMemoryCachingService(CachingService):
    def __init__(self) -> None:
        self.data: Dict[str, Node] = {}

    def get_node(self, id: str) -> Node | None:
        if id not in self.data:
            return None
        print(f"\nfound {id} in Cache, fetting...\n")
        return self.data[id]

    def add_node(self, node: Node) -> None:
        if node.resource_id not in self.data.keys():
            self.data[node.resource_id] = node

    def delete_node(self, id: str) -> None:
        if id not in self.data:
            raise ValueError(f"{id} is not known")

        self.data.pop(id)

    def clear(self) -> None:
        self.data = {}

    def is_empty(self) -> bool:
        return len(self.data.keys()) > 0

    def key_exists(self, id: str) -> bool:
        return id in self.data.keys()
