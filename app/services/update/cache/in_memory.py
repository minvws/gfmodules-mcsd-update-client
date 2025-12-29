from typing import Dict, List
from uuid import UUID

from app.models.adjacency.node import Node
from app.services.update.cache.caching_service import CachingService


class InMemoryCachingService(CachingService):
    """
    Simple in-memory cache implementation.

    This implementation is mainly useful for local development/tests. In production,
    an external cache (Redis) is recommended so multiple app instances can share state.
    """

    def __init__(
        self,
        run_id: UUID,
        namespace: str = "mcsd",
        object_ttl_seconds: int | None = None,
    ) -> None:
        super().__init__(run_id=run_id, namespace=namespace, object_ttl_seconds=object_ttl_seconds)
        self.__data: Dict[str, Node] = {}

    def get_node(self, id: str) -> Node | None:
        if not self.key_exists(id):
            return None

        target_id = self.make_target_id(id)
        return self.__data[target_id]

    def add_node(self, node: Node) -> None:
        target_id = self.make_target_id(node.cache_key())
        self.__data[target_id] = node

    def key_exists(self, id: str) -> bool:
        target_id = self.make_target_id(id)
        return target_id in self.__data

    def is_healthy(self) -> bool:
        return True

    def clear(self) -> None:
        self.__data.clear()

    def keys(self) -> List[str]:
        prefix = f"{self.namespace}:{self.run_id}:"
        return [key.replace(prefix, "") for key in self.__data.keys()]
