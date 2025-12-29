from abc import ABC, abstractmethod
from typing import List
from uuid import UUID

from app.models.adjacency.node import Node


class CachingService(ABC):
    """
    Abstract base class for caching.

    NOTE: All cache operations are keyed by a *stable* cache id in the form:
        <resourceType>/<id>

    The implementation will additionally prefix keys with:
        <namespace>:<run_id>:

    This prevents collisions between different update runs and between resource types.
    """

    def __init__(
        self,
        run_id: UUID,
        namespace: str = "mcsd",
        object_ttl_seconds: int | None = None,
    ) -> None:
        self.run_id = run_id
        self.namespace = namespace
        self.object_ttl_seconds = object_ttl_seconds

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

    def bulk_exists(self, ids: List[str]) -> set[str]:
        """Return the subset of ids that are present in the cache."""
        return {i for i in ids if self.key_exists(i)}

    def make_target_id(self, id: str) -> str:
        """Build the physical storage key for a logical cache key."""
        return f"{self.namespace}:{self.run_id}:{id}"
