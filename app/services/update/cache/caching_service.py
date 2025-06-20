from abc import ABC, abstractmethod
from typing import Dict, List
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

    @abstractmethod
    def clear(self) -> None: ...

    @abstractmethod
    def keys(self) -> List[str]: ...

    def make_target_id(self, id: str) -> str:
        return f"{str(self.run_id)}-{id}"
