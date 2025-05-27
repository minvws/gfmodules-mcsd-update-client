from abc import ABC, abstractmethod
from typing import Dict
from app.models.adjacency.node import Node


class CachingService(ABC):

    @abstractmethod
    def get_node(self, id: str) -> Node | None: ...

    # @abstractmethod
    # def get_nodes(self, id: str) -> List[Node]: ...

    @abstractmethod
    def add_node(self, node: Node) -> None: ...

    # @abstractmethod
    # def add_nodes(self, nodes: List[Node]) -> None: ...

    @abstractmethod
    def delete_node(self, id: str) -> None: ...

    @abstractmethod
    def clear(self) -> None: ...

    @abstractmethod
    def is_empty(self) -> bool: ...

    @abstractmethod
    def key_exists(self, id: str) -> bool: ...
