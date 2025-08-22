from typing import Dict, List
from uuid import UUID
from app.models.adjacency.node import Node
from app.services.update.cache.caching_service import CachingService


class InMemoryCachingService(CachingService):
    def __init__(self, run_id: UUID) -> None:
        self.__data: Dict[str, Node] = {}
        self.run_id = run_id

    def get_node(self, id: str) -> Node | None:
        target_id = self.make_target_id(id)
        if not self.key_exists(target_id):
            return None

        return self.__data[target_id]

    def add_node(self, node: Node) -> None:
        target_id = self.make_target_id(node.resource_id)
        self.__data[target_id] = node

    def key_exists(self, id: str) -> bool:
        return id in self.__data.keys()

    def is_healthy(self) -> bool:
        return True

    def clear(self) -> None:
        self.__data = {}

    def keys(self) -> List[str]:
        data = [key for key in self.__data.keys()]
        results = list(map(lambda x: x.replace(f"{self.run_id}-", ""), data))

        return results
