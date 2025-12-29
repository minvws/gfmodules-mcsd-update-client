from __future__ import annotations

from typing import List
from uuid import UUID

from redis import ConnectionError, Redis

from app.models.adjacency.node import Node
from app.services.update.cache.caching_service import CachingService


class ExternalCachingService(CachingService):
    """
    Redis-backed caching service.

    Key format:
        <namespace>:<run_id>:<resourceType>/<id>

    This prevents collisions across runs and across resource types. It also ensures
    we never need to FLUSHDB (which would be dangerous in shared Redis deployments).
    """

    def __init__(
        self,
        run_id: UUID,
        host: str,
        port: int,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_ca_certs: str | None = None,
        ssl_certfile: str | None = None,
        ssl_check_hostname: bool = True,
        namespace: str = "mcsd",
        object_ttl_seconds: int | None = None,
    ) -> None:
        super().__init__(run_id=run_id, namespace=namespace, object_ttl_seconds=object_ttl_seconds)
        self.__redis = Redis(
            host=host,
            port=port,
            db=0,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_ca_certs=ssl_ca_certs,
            ssl_check_hostname=ssl_check_hostname,
        )

    def _pattern(self) -> str:
        return f"{self.namespace}:{self.run_id}:*"

    def get_node(self, id: str) -> Node | None:
        target_id = self.make_target_id(id)
        serialized_data = self.__redis.get(target_id)
        if serialized_data is None:
            return None
        return Node.model_validate_json(serialized_data)  # type: ignore[arg-type]

    def add_node(self, node: Node) -> None:
        target_id = self.make_target_id(node.cache_key())
        serialized_data = node.model_dump_json()

        ttl = self.object_ttl_seconds
        if ttl is not None and ttl > 0:
            # Use Redis expiration so old runs don't leak memory.
            self.__redis.set(target_id, serialized_data, ex=ttl)
        else:
            self.__redis.set(target_id, serialized_data)

    def key_exists(self, id: str) -> bool:
        target_id = self.make_target_id(id)
        return bool(self.__redis.exists(target_id))

    def bulk_exists(self, ids: List[str]) -> set[str]:
        if not ids:
            return set()
        # Pipeline EXISTS calls to avoid one network round-trip per key.
        pipe = self.__redis.pipeline(transaction=False)
        for i in ids:
            pipe.exists(self.make_target_id(i))
        results = pipe.execute()
        return {ids[idx] for idx, exists in enumerate(results) if exists}

    def clear(self) -> None:
        # Delete only keys for the current run id/namespace.
        keys = list(self.__redis.scan_iter(match=self._pattern()))
        if not keys:
            return
        pipe = self.__redis.pipeline(transaction=False)
        for k in keys:
            pipe.delete(k)
        pipe.execute()

    def is_healthy(self) -> bool:
        try:
            return bool(self.__redis.ping())
        except ConnectionError:
            return False

    def keys(self) -> List[str]:
        prefix = f"{self.namespace}:{self.run_id}:"
        return [
            key.decode("utf-8").replace(prefix, "")
            for key in self.__redis.scan_iter(match=self._pattern())
        ]
