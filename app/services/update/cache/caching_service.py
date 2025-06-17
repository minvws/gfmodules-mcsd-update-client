from abc import ABC, abstractmethod
from typing import Dict
from uuid import UUID
from app.models.adjacency.node import Node


class CachingService(ABC):
    def __init__(self, run_id: UUID) -> None:
        self.run_id = run_id
        self.data: Dict[str, Node] = {}

    @abstractmethod
    def get_node(self, id: str) -> Node | None: ...

    @abstractmethod
    def add_node(self, node: Node) -> None: ...

    @abstractmethod
    def key_exists(self, id: str) -> bool: ...

    @abstractmethod
    def is_healthy(self) -> bool: ...


class InMemoryCachingService(CachingService):
    def __init__(self, run_id: UUID) -> None:
        self.data: Dict[str, Node] = {}
        self.run_id = run_id

    def get_node(self, id: str) -> Node | None:
        if not self.key_exists(id):
            return None

        target_id = f"{self.run_id}-{id}"
        return self.data[target_id]

    def add_node(self, node: Node) -> None:
        target_id = f"{self.run_id}-{node.resource_id}"
        self.data[target_id] = node

    def key_exists(self, id: str) -> bool:
        target_id = f"{self.run_id}-{id}"
        return target_id in self.data.keys()

    def is_healthy(self) -> bool:
        return True
